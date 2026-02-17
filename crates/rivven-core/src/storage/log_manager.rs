use super::segment::{Segment, SegmentSyncPolicy};
use crate::{Message, Result};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

/// Maximum number of messages to return per read operation (F-030)
const MAX_MESSAGES_PER_READ: usize = 10_000;

#[derive(Debug)]
pub struct LogManager {
    dir: PathBuf,
    segments: Vec<Segment>,
    active_segment_index: usize,
    max_segment_size: u64,
    /// Fsync policy for all segments (F-001 fix)
    sync_policy: SegmentSyncPolicy,
}

impl LogManager {
    pub async fn new(
        base_dir: PathBuf,
        topic: &str,
        partition: u32,
        max_segment_size: u64,
    ) -> Result<Self> {
        Self::with_sync_policy(
            base_dir,
            topic,
            partition,
            max_segment_size,
            SegmentSyncPolicy::EveryNWrites(1),
        )
        .await
    }

    /// Create a new LogManager with an explicit segment sync policy (F-001 fix).
    pub async fn with_sync_policy(
        base_dir: PathBuf,
        topic: &str,
        partition: u32,
        max_segment_size: u64,
        sync_policy: SegmentSyncPolicy,
    ) -> Result<Self> {
        let dir = base_dir
            .join(topic)
            .join(format!("partition-{}", partition));
        fs::create_dir_all(&dir)?;

        let mut segments = Vec::new();
        // Load existing segments
        // Simply look for .log files and sort them
        let mut paths: Vec<_> = fs::read_dir(&dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .collect();

        paths.sort();

        if paths.is_empty() {
            // Create initial segment with configured sync policy
            segments.push(Segment::with_sync_policy(&dir, 0, sync_policy)?);
        } else {
            for path in paths {
                // Handle malformed filenames gracefully (non-UTF8, missing stem, etc.)
                let Some(stem) = path.file_stem() else {
                    tracing::warn!(path = ?path, "Skipping segment file with no stem");
                    continue;
                };
                let Some(filename) = stem.to_str() else {
                    tracing::warn!(path = ?path, "Skipping segment file with non-UTF8 name");
                    continue;
                };
                // Assumes filename is just the offset (e.g. "00000000000000000000")
                if let Ok(base_offset) = filename.parse::<u64>() {
                    segments.push(Segment::with_sync_policy(&dir, base_offset, sync_policy)?);
                } else {
                    tracing::warn!(filename = %filename, "Skipping segment file with unparseable offset");
                }
            }
        }

        if segments.is_empty() {
            segments.push(Segment::with_sync_policy(&dir, 0, sync_policy)?);
        }

        let active_segment_index = segments.len() - 1;

        Ok(Self {
            dir,
            segments,
            active_segment_index,
            max_segment_size,
            sync_policy,
        })
    }

    pub async fn append(&mut self, offset: u64, message: Message) -> Result<u64> {
        let segment = &mut self.segments[self.active_segment_index];

        // Check if we need to roll
        if segment.size() >= self.max_segment_size {
            // Flush current segment before rolling
            segment.flush().await?;
            let new_segment = Segment::with_sync_policy(&self.dir, offset, self.sync_policy)?;
            self.segments.push(new_segment);
            self.active_segment_index += 1;
        }

        let segment = &mut self.segments[self.active_segment_index];
        segment.append(offset, message).await
    }

    /// F-136 fix: Take the deferred sync handle from the active segment.
    ///
    /// Call this while still holding the write lock, then release the lock
    /// and run `sync_data()` on the returned file descriptor. This allows
    /// concurrent readers to proceed during the slow fdatasync syscall.
    pub fn take_pending_sync(&self) -> Option<Arc<std::fs::File>> {
        self.segments[self.active_segment_index].take_pending_sync()
    }

    /// Batch append for high-throughput scenarios
    /// Splits batches across segment boundaries when needed
    pub async fn append_batch(&mut self, messages: Vec<(u64, Message)>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_positions = Vec::with_capacity(messages.len());
        let mut remaining = messages;

        while !remaining.is_empty() {
            let segment = &mut self.segments[self.active_segment_index];

            // Roll if current segment is already at capacity
            if segment.size() >= self.max_segment_size {
                segment.flush().await?;
                let new_segment =
                    Segment::with_sync_policy(&self.dir, remaining[0].0, self.sync_policy)?;
                self.segments.push(new_segment);
                self.active_segment_index += 1;
            }

            let segment = &self.segments[self.active_segment_index];
            let available = self.max_segment_size.saturating_sub(segment.size());

            // Find how many messages fit in the remaining capacity
            let mut accumulated = 0u64;
            let mut split_at = 0;
            for (_, m) in remaining.iter() {
                // Accurate size estimation: value + key + headers + fixed overhead
                // Fixed overhead (64 bytes): CRC(4) + length prefix(4) + offset(8) + timestamp(12) + postcard framing(~36)
                let key_len = m.key.as_ref().map(|k| k.len()).unwrap_or(0);
                let headers_len = m
                    .headers
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 8)
                    .sum::<usize>();
                let msg_size = (m.value.len() + key_len + headers_len + 64) as u64;
                if accumulated + msg_size > available && split_at > 0 {
                    break;
                }
                accumulated += msg_size;
                split_at += 1;
                // Always include at least one message to guarantee progress
                if accumulated > available {
                    break;
                }
            }

            // Split without cloning: drain the batch portion, keep the rest
            let rest = remaining.split_off(split_at);
            let batch = remaining;
            let segment = &mut self.segments[self.active_segment_index];
            let positions = segment.append_batch(batch).await?;
            all_positions.extend(positions);
            remaining = rest;
        }

        Ok(all_positions)
    }

    pub async fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        let mut bytes_collected = 0;

        // Optimized: Find first segment where base_offset <= offset
        let start_segment_idx = self
            .segments
            .partition_point(|seg| seg.base_offset() <= offset)
            .saturating_sub(1);

        for segment in self.segments.iter().skip(start_segment_idx) {
            // If we have enough data, stop
            if bytes_collected >= max_bytes {
                break;
            }

            // Read from segment
            let batch = segment.read(offset, max_bytes - bytes_collected).await?;

            for msg in batch {
                if msg.offset < offset {
                    continue;
                }

                if messages.len() < MAX_MESSAGES_PER_READ && bytes_collected < max_bytes {
                    // Estimate size (header + key + val)
                    let size = 8 + msg.key.as_ref().map(|k| k.len()).unwrap_or(0) + msg.value.len();
                    bytes_collected += size;

                    messages.push(msg);
                }
            }
        }

        Ok(messages)
    }

    pub fn earliest_offset(&self) -> u64 {
        self.segments.first().map(|s| s.base_offset()).unwrap_or(0)
    }

    pub async fn recover_next_offset(&self) -> Result<u64> {
        if let Some(last_segment) = self.segments.last() {
            if let Some(last_offset) = last_segment.recover_last_offset().await? {
                return Ok(last_offset + 1);
            }
            // If last segment is empty, check `base_offset` of it?
            // Usually base_offset is the next offset of previous segment.
            // But if it's completely empty (newly created), next offset is `base_offset`.
            return Ok(last_segment.base_offset());
        }
        Ok(0)
    }

    /// Flush all segments to disk ensuring durability
    pub async fn flush(&self) -> Result<()> {
        for segment in &self.segments {
            segment.flush().await?;
        }
        Ok(())
    }

    /// Physically remove segments whose data is entirely below the given offset.
    ///
    /// A segment is eligible for deletion when the *next* segment's `base_offset`
    /// is ≤ `watermark`, meaning every record in the segment is before the
    /// watermark. The currently-active segment is never deleted.
    ///
    /// Returns the number of segments removed.
    pub fn truncate_before(&mut self, watermark: u64) -> crate::Result<usize> {
        if self.segments.len() <= 1 {
            return Ok(0);
        }

        // Find how many segments are fully below the watermark.
        // A segment at index `i` is fully below if the *next* segment's
        // base_offset <= watermark (all records in segment[i] < base[i+1] <= watermark).
        let mut remove_count = 0;
        for i in 0..self.segments.len().saturating_sub(1) {
            let next_base = self.segments[i + 1].base_offset();
            if next_base <= watermark {
                remove_count = i + 1;
            } else {
                break;
            }
        }

        if remove_count == 0 {
            return Ok(0);
        }

        // Delete files for removed segments
        for seg in self.segments.drain(..remove_count) {
            if let Err(e) = seg.delete_files() {
                tracing::warn!(
                    "Failed to delete segment files at offset {}: {}",
                    seg.base_offset(),
                    e
                );
            }
        }

        // Adjust active segment index
        self.active_segment_index = self.active_segment_index.saturating_sub(remove_count);

        Ok(remove_count)
    }

    /// Find the first offset with timestamp >= target_timestamp (milliseconds since epoch)
    /// Scans through segments to find the earliest message matching the timestamp.
    /// Returns None if no matching offset is found.
    pub async fn find_offset_for_timestamp(&self, target_timestamp: i64) -> Result<Option<u64>> {
        // Scan segments from oldest to newest
        for segment in &self.segments {
            // Check timestamp bounds to skip segments that are entirely before target
            if let Some((_min_ts, max_ts)) = segment.timestamp_bounds().await? {
                // If the entire segment is before our target, skip it
                if max_ts < target_timestamp {
                    continue;
                }
                // If the segment might contain our target, search it
                if let Some(offset) = segment.find_offset_for_timestamp(target_timestamp).await? {
                    return Ok(Some(offset));
                }
            }
        }
        Ok(None)
    }

    /// F-125 fix: Key-based log compaction for sealed (non-active) segments.
    ///
    /// Reads all messages from eligible segments, keeps only the latest value
    /// per key (by highest offset), removes tombstones (empty value = deletion
    /// marker), and rewrites compacted segments in-place.
    ///
    /// Only sealed segments (not the active segment) are compacted. The active
    /// segment is left untouched to avoid interfering with concurrent appends.
    ///
    /// Returns the number of messages removed by compaction.
    pub async fn compact(&mut self) -> Result<usize> {
        // Need at least 2 segments (1 sealed + 1 active) to compact
        if self.segments.len() <= 1 {
            return Ok(0);
        }

        let sealed_count = self.segments.len() - 1; // exclude active segment
        let mut total_removed = 0;

        // Phase 1: Read all messages from sealed segments, build key→latest map
        let mut key_latest: HashMap<Vec<u8>, (u64, usize)> = HashMap::new(); // key → (offset, segment_idx)
        let mut all_messages: Vec<Vec<Message>> = Vec::with_capacity(sealed_count);

        for seg_idx in 0..sealed_count {
            let messages = self.segments[seg_idx]
                .read(self.segments[seg_idx].base_offset(), usize::MAX)
                .await?;
            for msg in &messages {
                if let Some(key) = &msg.key {
                    let key_bytes = key.to_vec();
                    // Later offsets always win (higher offset = more recent)
                    match key_latest.get(&key_bytes) {
                        Some(&(existing_offset, _)) if existing_offset >= msg.offset => {}
                        _ => {
                            key_latest.insert(key_bytes, (msg.offset, seg_idx));
                        }
                    }
                }
            }
            all_messages.push(messages);
        }

        // Phase 2: For each sealed segment, filter to keep only latest-keyed
        // messages and non-tombstones, then rewrite if any messages were removed
        for seg_idx in 0..sealed_count {
            let messages = &all_messages[seg_idx];
            let base_offset = self.segments[seg_idx].base_offset();

            let compacted: Vec<&Message> = messages
                .iter()
                .filter(|msg| {
                    match &msg.key {
                        Some(key) => {
                            let key_bytes = key.to_vec();
                            // Keep if this is the latest value for this key
                            if let Some(&(latest_offset, _)) = key_latest.get(&key_bytes) {
                                if msg.offset != latest_offset {
                                    return false; // superseded by later write
                                }
                                // Remove tombstones (empty value)
                                if msg.value.is_empty() {
                                    return false;
                                }
                                true
                            } else {
                                true // shouldn't happen, but keep
                            }
                        }
                        None => true, // keyless messages are always kept
                    }
                })
                .collect();

            let removed = messages.len() - compacted.len();
            if removed == 0 {
                continue; // no compaction needed for this segment
            }

            total_removed += removed;

            // Phase 3: Rewrite the segment — delete old files, create new segment,
            // re-append the compacted messages
            self.segments[seg_idx].delete_files()?;
            let mut new_segment =
                Segment::with_sync_policy(&self.dir, base_offset, self.sync_policy)?;

            let batch: Vec<(u64, Message)> = compacted
                .into_iter()
                .map(|m| (m.offset, m.clone()))
                .collect();

            if !batch.is_empty() {
                new_segment.append_batch(batch).await?;
                new_segment.flush().await?;
            }

            self.segments[seg_idx] = new_segment;

            tracing::debug!(
                segment_base = base_offset,
                removed,
                "Compacted segment"
            );
        }

        if total_removed > 0 {
            tracing::info!(
                removed = total_removed,
                segments = sealed_count,
                "Log compaction complete"
            );
        }

        Ok(total_removed)
    }
}

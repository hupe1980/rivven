use super::segment::Segment;
use crate::{Message, Result};
use std::fs;
use std::path::PathBuf;

#[derive(Debug)]
pub struct LogManager {
    dir: PathBuf,
    segments: Vec<Segment>,
    active_segment_index: usize,
    max_segment_size: u64,
}

impl LogManager {
    pub async fn new(
        base_dir: PathBuf,
        topic: &str,
        partition: u32,
        max_segment_size: u64,
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
            // Create initial segment
            segments.push(Segment::new(&dir, 0)?);
        } else {
            for path in paths {
                let filename = path.file_stem().unwrap().to_str().unwrap();
                // Assumes filename is just the offset (e.g. "00000000000000000000")
                if let Ok(base_offset) = filename.parse::<u64>() {
                    segments.push(Segment::new(&dir, base_offset)?);
                }
            }
        }

        if segments.is_empty() {
            segments.push(Segment::new(&dir, 0)?);
        }

        let active_segment_index = segments.len() - 1;

        Ok(Self {
            dir,
            segments,
            active_segment_index,
            max_segment_size,
        })
    }

    pub async fn append(&mut self, offset: u64, message: Message) -> Result<u64> {
        let segment = &mut self.segments[self.active_segment_index];

        // Check if we need to roll
        if segment.size() >= self.max_segment_size {
            // Flush current segment before rolling
            segment.flush().await?;
            let new_segment = Segment::new(&self.dir, offset)?;
            self.segments.push(new_segment);
            self.active_segment_index += 1;
        }

        let segment = &mut self.segments[self.active_segment_index];
        segment.append(offset, message).await
    }

    /// Batch append for high-throughput scenarios
    /// Splits batches across segment boundaries when needed
    pub async fn append_batch(&mut self, messages: Vec<(u64, Message)>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_positions = Vec::with_capacity(messages.len());
        let mut remaining = messages.as_slice();

        while !remaining.is_empty() {
            let segment = &mut self.segments[self.active_segment_index];

            // Roll if current segment is already at capacity
            if segment.size() >= self.max_segment_size {
                segment.flush().await?;
                let new_segment = Segment::new(&self.dir, remaining[0].0)?;
                self.segments.push(new_segment);
                self.active_segment_index += 1;
            }

            let segment = &self.segments[self.active_segment_index];
            let available = self.max_segment_size.saturating_sub(segment.size());

            // Find how many messages fit in the remaining capacity
            let mut accumulated = 0u64;
            let mut split_at = 0;
            for (_, m) in remaining.iter() {
                let msg_size = (m.value.len() + 100) as u64;
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

            let (batch, rest) = remaining.split_at(split_at);
            let segment = &mut self.segments[self.active_segment_index];
            let positions = segment.append_batch(batch.to_vec()).await?;
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

                if messages.len() < 1000 && bytes_collected < max_bytes {
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
}

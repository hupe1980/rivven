use std::path::PathBuf;
use crate::{Result, Message};
use super::segment::Segment;
use std::fs;

#[derive(Debug)]
pub struct LogManager {
    dir: PathBuf,
    segments: Vec<Segment>,
    active_segment_index: usize,
    max_segment_size: u64,
}

impl LogManager {
    pub async fn new(base_dir: PathBuf, topic: &str, partition: u32, max_segment_size: u64) -> Result<Self> {
        let dir = base_dir.join(topic).join(format!("partition-{}", partition));
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
            let new_segment = Segment::new(&self.dir, offset)?;
            self.segments.push(new_segment);
            self.active_segment_index += 1;
        }

        let segment = &mut self.segments[self.active_segment_index];
        segment.append(offset, message).await
    }

    pub async fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        let mut bytes_collected = 0;

        // Optimized: Find first segment where base_offset <= offset
        let start_segment_idx = self.segments.partition_point(|seg| seg.base_offset() <= offset).saturating_sub(1);

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

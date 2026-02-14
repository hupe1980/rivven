use std::collections::BTreeMap;
use std::sync::RwLock;

use super::traits::StorageBackend;

/// In-memory storage backend that actually stores data for non-persistent mode.
///
/// Uses a `BTreeMap<u64, Vec<u8>>` per (topic, partition) so offset-ordered
/// retrieval is fast and `trim()` can efficiently remove ranges.
///
/// F-093 fix: replaced the previous no-op implementation with a functional
/// in-memory store so code paths that depend on `StorageBackend::retrieve`
/// returning data work correctly in non-persistent mode.
pub struct MemoryStorage {
    data: RwLock<BTreeMap<(String, u32), BTreeMap<u64, Vec<u8>>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }
}

impl StorageBackend for MemoryStorage {
    fn store(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        data: &[u8],
    ) -> crate::Result<()> {
        let mut guard = self.data.write().unwrap();
        guard
            .entry((topic.to_string(), partition))
            .or_default()
            .insert(offset, data.to_vec());
        Ok(())
    }

    fn retrieve(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
    ) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        let guard = self.data.read().unwrap();
        let key = (topic.to_string(), partition);
        match guard.get(&key) {
            Some(offsets) => Ok(offsets
                .range(start_offset..)
                .take(max_messages)
                .map(|(k, v)| (*k, v.clone()))
                .collect()),
            None => Ok(Vec::new()),
        }
    }

    fn trim(&self, topic: &str, partition: u32, before_offset: u64) -> crate::Result<()> {
        let mut guard = self.data.write().unwrap();
        let key = (topic.to_string(), partition);
        if let Some(offsets) = guard.get_mut(&key) {
            let to_remove: Vec<u64> = offsets
                .range(..before_offset)
                .map(|(k, _)| *k)
                .collect();
            for offset in to_remove {
                offsets.remove(&offset);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_storage_store_and_retrieve() {
        let storage = MemoryStorage::new();
        storage.store("test", 0, 0, b"hello").unwrap();
        storage.store("test", 0, 1, b"world").unwrap();

        let messages = storage.retrieve("test", 0, 0, 10).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], (0, b"hello".to_vec()));
        assert_eq!(messages[1], (1, b"world".to_vec()));
    }

    #[test]
    fn test_memory_storage_retrieve_from_offset() {
        let storage = MemoryStorage::new();
        storage.store("test", 0, 0, b"a").unwrap();
        storage.store("test", 0, 1, b"b").unwrap();
        storage.store("test", 0, 2, b"c").unwrap();

        let messages = storage.retrieve("test", 0, 1, 10).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, 1);
    }

    #[test]
    fn test_memory_storage_trim() {
        let storage = MemoryStorage::new();
        storage.store("test", 0, 0, b"a").unwrap();
        storage.store("test", 0, 1, b"b").unwrap();
        storage.store("test", 0, 2, b"c").unwrap();

        storage.trim("test", 0, 2).unwrap();
        let messages = storage.retrieve("test", 0, 0, 10).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, 2);
    }

    #[test]
    fn test_memory_storage_max_messages() {
        let storage = MemoryStorage::new();
        for i in 0..100 {
            storage.store("test", 0, i, &[i as u8]).unwrap();
        }

        let messages = storage.retrieve("test", 0, 0, 5).unwrap();
        assert_eq!(messages.len(), 5);
    }

    #[test]
    fn test_memory_storage_empty_retrieve() {
        let storage = MemoryStorage::new();
        let messages = storage.retrieve("test", 0, 0, 10).unwrap();
        assert!(messages.is_empty());
    }
}

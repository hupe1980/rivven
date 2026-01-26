use super::traits::StorageBackend;

/// In-memory storage (no persistence)
pub struct MemoryStorage;

impl StorageBackend for MemoryStorage {
    fn store(&self, _topic: &str, _partition: u32, _offset: u64, _data: &[u8]) -> crate::Result<()> {
        // No-op for memory storage
        Ok(())
    }

    fn retrieve(
        &self,
        _topic: &str,
        _partition: u32,
        _start_offset: u64,
        _max_messages: usize,
    ) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        Ok(Vec::new())
    }

    fn trim(&self, _topic: &str, _partition: u32, _before_offset: u64) -> crate::Result<()> {
        Ok(())
    }
}

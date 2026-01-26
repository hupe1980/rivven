/// Storage backend trait for persistence
pub trait StorageBackend: Send + Sync {
    /// Store a message
    fn store(&self, topic: &str, partition: u32, offset: u64, data: &[u8]) -> crate::Result<()>;
    
    /// Retrieve messages
    fn retrieve(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
    ) -> crate::Result<Vec<(u64, Vec<u8>)>>;
    
    /// Delete messages before offset
    fn trim(&self, topic: &str, partition: u32, before_offset: u64) -> crate::Result<()>;
}

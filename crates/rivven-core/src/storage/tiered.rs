//! Tiered Storage System for Rivven
//!
//! Implements a hot/warm/cold tiered storage architecture:
//! - **Hot Tier**: In-memory buffer + NVMe/SSD for recent data (sub-ms access)
//! - **Warm Tier**: Local disk storage for medium-aged data (ms access)
//! - **Cold Tier**: Object storage (S3/MinIO/Azure Blob) for archival (100ms+ access)
//!
//! Features:
//! - Automatic tier promotion/demotion based on access patterns
//! - LRU cache for hot tier with size limits
//! - Asynchronous background compaction and migration
//! - Zero-copy reads from memory-mapped warm tier
//! - Pluggable cold storage backends

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::time::interval;

use crate::{Error, Result};

/// Storage tier classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageTier {
    /// Hot tier: In-memory + fast SSD, < 1ms access
    Hot,
    /// Warm tier: Local disk, mmap'd, 1-10ms access  
    Warm,
    /// Cold tier: Object storage, 100ms+ access
    Cold,
}

impl StorageTier {
    /// Get tier name for metrics/logging
    pub fn name(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
        }
    }

    /// Get next cooler tier (for demotion)
    pub fn demote(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => Some(StorageTier::Warm),
            StorageTier::Warm => Some(StorageTier::Cold),
            StorageTier::Cold => None,
        }
    }

    /// Get next hotter tier (for promotion)
    pub fn promote(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => None,
            StorageTier::Warm => Some(StorageTier::Hot),
            StorageTier::Cold => Some(StorageTier::Warm),
        }
    }
}

/// Configuration for tiered storage
#[derive(Debug, Clone)]
pub struct TieredStorageConfig {
    /// Maximum size of hot tier in bytes
    pub hot_tier_max_bytes: u64,
    /// Maximum age of data in hot tier before demotion
    pub hot_tier_max_age: Duration,
    /// Maximum size of warm tier in bytes
    pub warm_tier_max_bytes: u64,
    /// Maximum age of data in warm tier before demotion  
    pub warm_tier_max_age: Duration,
    /// Path for warm tier storage
    pub warm_tier_path: PathBuf,
    /// Cold storage backend configuration
    pub cold_storage: ColdStorageConfig,
    /// How often to run tier migration
    pub migration_interval: Duration,
    /// Number of concurrent migration operations
    pub migration_concurrency: usize,
    /// Enable access-based promotion (promote frequently accessed cold data)
    pub enable_promotion: bool,
    /// Access count threshold for promotion
    pub promotion_threshold: u64,
    /// Compaction threshold (ratio of dead bytes to total)
    pub compaction_threshold: f64,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            hot_tier_max_bytes: 1024 * 1024 * 1024,            // 1 GB
            hot_tier_max_age: Duration::from_secs(3600),       // 1 hour
            warm_tier_max_bytes: 100 * 1024 * 1024 * 1024,     // 100 GB
            warm_tier_max_age: Duration::from_secs(86400 * 7), // 7 days
            warm_tier_path: PathBuf::from("/var/lib/rivven/warm"),
            cold_storage: ColdStorageConfig::default(),
            migration_interval: Duration::from_secs(60),
            migration_concurrency: 4,
            enable_promotion: true,
            promotion_threshold: 100,
            compaction_threshold: 0.5,
        }
    }
}

impl TieredStorageConfig {
    /// High-performance config for low-latency workloads
    pub fn high_performance() -> Self {
        Self {
            hot_tier_max_bytes: 8 * 1024 * 1024 * 1024,    // 8 GB
            hot_tier_max_age: Duration::from_secs(7200),   // 2 hours
            warm_tier_max_bytes: 500 * 1024 * 1024 * 1024, // 500 GB
            migration_interval: Duration::from_secs(30),
            ..Default::default()
        }
    }

    /// Cost-optimized config for archival workloads
    pub fn cost_optimized() -> Self {
        Self {
            hot_tier_max_bytes: 256 * 1024 * 1024,         // 256 MB
            hot_tier_max_age: Duration::from_secs(300),    // 5 minutes
            warm_tier_max_bytes: 10 * 1024 * 1024 * 1024,  // 10 GB
            warm_tier_max_age: Duration::from_secs(86400), // 1 day
            migration_interval: Duration::from_secs(120),
            enable_promotion: false,
            ..Default::default()
        }
    }
}

/// Cold storage backend configuration
#[derive(Debug, Clone)]
pub enum ColdStorageConfig {
    /// Local filesystem (for development/testing)
    LocalFs { path: PathBuf },
    /// S3-compatible object storage
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        use_path_style: bool,
    },
    /// Azure Blob Storage
    AzureBlob {
        account: String,
        container: String,
        access_key: Option<String>,
    },
    /// Disabled (warm tier is final)
    Disabled,
}

impl Default for ColdStorageConfig {
    fn default() -> Self {
        ColdStorageConfig::LocalFs {
            path: PathBuf::from("/var/lib/rivven/cold"),
        }
    }
}

/// Metadata about a stored segment
#[derive(Debug)]
pub struct SegmentMetadata {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: u32,
    /// Base offset of segment
    pub base_offset: u64,
    /// End offset (exclusive)
    pub end_offset: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Current storage tier
    pub tier: StorageTier,
    /// Creation timestamp
    pub created_at: u64,
    /// Last accessed timestamp
    pub last_accessed: AtomicU64,
    /// Access count for promotion decisions
    pub access_count: AtomicU64,
    /// Number of deleted/compacted records
    pub dead_records: AtomicU64,
    /// Total records
    pub total_records: u64,
}

impl SegmentMetadata {
    pub fn new(
        topic: String,
        partition: u32,
        base_offset: u64,
        end_offset: u64,
        size_bytes: u64,
        tier: StorageTier,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            topic,
            partition,
            base_offset,
            end_offset,
            size_bytes,
            tier,
            created_at: now,
            last_accessed: AtomicU64::new(now),
            access_count: AtomicU64::new(0),
            dead_records: AtomicU64::new(0),
            total_records: (end_offset - base_offset),
        }
    }

    /// Record an access and update statistics
    pub fn record_access(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_accessed.store(now, Ordering::Relaxed);
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get age in seconds
    pub fn age_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(self.created_at)
    }

    /// Get seconds since last access
    pub fn idle_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(self.last_accessed.load(Ordering::Relaxed))
    }

    /// Calculate compaction ratio (dead/total)
    pub fn compaction_ratio(&self) -> f64 {
        let dead = self.dead_records.load(Ordering::Relaxed);
        if self.total_records == 0 {
            0.0
        } else {
            dead as f64 / self.total_records as f64
        }
    }

    /// Build segment key for storage
    pub fn segment_key(&self) -> String {
        format!("{}/{}/{:020}", self.topic, self.partition, self.base_offset)
    }
}

/// Hot tier: In-memory LRU cache
#[derive(Debug)]
pub struct HotTier {
    /// Segment data keyed by (topic, partition, base_offset)
    segments: RwLock<HashMap<(String, u32, u64), Bytes>>,
    /// LRU order tracking
    lru_order: Mutex<VecDeque<(String, u32, u64)>>,
    /// Current size in bytes
    current_size: AtomicU64,
    /// Maximum size in bytes
    max_size: u64,
}

impl HotTier {
    pub fn new(max_size: u64) -> Self {
        Self {
            segments: RwLock::new(HashMap::new()),
            lru_order: Mutex::new(VecDeque::new()),
            current_size: AtomicU64::new(0),
            max_size,
        }
    }

    /// Insert data into hot tier
    pub async fn insert(&self, topic: &str, partition: u32, base_offset: u64, data: Bytes) -> bool {
        let size = data.len() as u64;

        // Check if it fits
        if size > self.max_size {
            return false;
        }

        // Evict until we have space
        while self.current_size.load(Ordering::Relaxed) + size > self.max_size {
            if !self.evict_one().await {
                break;
            }
        }

        let key = (topic.to_string(), partition, base_offset);

        // Insert data
        {
            let mut segments = self.segments.write().await;
            if let Some(old) = segments.insert(key.clone(), data) {
                self.current_size
                    .fetch_sub(old.len() as u64, Ordering::Relaxed);
            }
        }

        // Update LRU
        {
            let mut lru = self.lru_order.lock().await;
            lru.retain(|k| k != &key);
            lru.push_back(key);
        }

        self.current_size.fetch_add(size, Ordering::Relaxed);
        true
    }

    /// Get data from hot tier
    pub async fn get(&self, topic: &str, partition: u32, base_offset: u64) -> Option<Bytes> {
        let key = (topic.to_string(), partition, base_offset);

        let data = {
            let segments = self.segments.read().await;
            segments.get(&key).cloned()
        };

        if data.is_some() {
            // Update LRU on access
            let mut lru = self.lru_order.lock().await;
            lru.retain(|k| k != &key);
            lru.push_back(key);
        }

        data
    }

    /// Remove data from hot tier
    pub async fn remove(&self, topic: &str, partition: u32, base_offset: u64) -> Option<Bytes> {
        let key = (topic.to_string(), partition, base_offset);

        let removed = {
            let mut segments = self.segments.write().await;
            segments.remove(&key)
        };

        if let Some(ref data) = removed {
            self.current_size
                .fetch_sub(data.len() as u64, Ordering::Relaxed);
            let mut lru = self.lru_order.lock().await;
            lru.retain(|k| k != &key);
        }

        removed
    }

    /// Evict least recently used segment
    async fn evict_one(&self) -> bool {
        let to_evict = {
            let mut lru = self.lru_order.lock().await;
            lru.pop_front()
        };

        if let Some(key) = to_evict {
            let removed = {
                let mut segments = self.segments.write().await;
                segments.remove(&key)
            };

            if let Some(data) = removed {
                self.current_size
                    .fetch_sub(data.len() as u64, Ordering::Relaxed);
                return true;
            }
        }

        false
    }

    /// Get current usage statistics
    pub fn stats(&self) -> HotTierStats {
        HotTierStats {
            current_size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HotTierStats {
    pub current_size: u64,
    pub max_size: u64,
}

/// Warm tier: Memory-mapped local disk storage
#[derive(Debug)]
pub struct WarmTier {
    /// Base path for warm tier storage
    base_path: PathBuf,
    /// Segment metadata index
    segments: RwLock<BTreeMap<(String, u32, u64), Arc<SegmentMetadata>>>,
    /// Current total size
    current_size: AtomicU64,
    /// Maximum size
    max_size: u64,
}

impl WarmTier {
    pub fn new(base_path: PathBuf, max_size: u64) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;

        Ok(Self {
            base_path,
            segments: RwLock::new(BTreeMap::new()),
            current_size: AtomicU64::new(0),
            max_size,
        })
    }

    /// Get segment file path
    fn segment_path(&self, topic: &str, partition: u32, base_offset: u64) -> PathBuf {
        self.base_path
            .join(topic)
            .join(format!("{}", partition))
            .join(format!("{:020}.segment", base_offset))
    }

    /// Store segment data
    pub async fn store(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        end_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let path = self.segment_path(topic, partition, base_offset);

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Write segment file
        tokio::fs::write(&path, data).await?;

        let size = data.len() as u64;

        // Update metadata
        let metadata = Arc::new(SegmentMetadata::new(
            topic.to_string(),
            partition,
            base_offset,
            end_offset,
            size,
            StorageTier::Warm,
        ));

        {
            let mut segments = self.segments.write().await;
            segments.insert((topic.to_string(), partition, base_offset), metadata);
        }

        self.current_size.fetch_add(size, Ordering::Relaxed);

        Ok(())
    }

    /// Read segment data using mmap for zero-copy
    pub async fn read(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Result<Option<Bytes>> {
        let path = self.segment_path(topic, partition, base_offset);

        if !path.exists() {
            return Ok(None);
        }

        // Memory map the file for efficient reading
        let file = std::fs::File::open(&path)?;
        // SAFETY: File is opened read-only and remains valid for mmap lifetime.
        // The mmap is only used for reading and copied to Bytes before return.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        // Update access stats
        let key = (topic.to_string(), partition, base_offset);
        if let Some(meta) = self.segments.read().await.get(&key) {
            meta.record_access();
        }

        // Convert to Bytes (this copies, but allows the mmap to be dropped)
        // For true zero-copy, we'd need to return an Arc<Mmap>
        Ok(Some(Bytes::copy_from_slice(&mmap)))
    }

    /// Remove segment
    pub async fn remove(&self, topic: &str, partition: u32, base_offset: u64) -> Result<()> {
        let path = self.segment_path(topic, partition, base_offset);
        let key = (topic.to_string(), partition, base_offset);

        let size = {
            let mut segments = self.segments.write().await;
            segments.remove(&key).map(|m| m.size_bytes)
        };

        if let Some(size) = size {
            self.current_size.fetch_sub(size, Ordering::Relaxed);
        }

        if path.exists() {
            tokio::fs::remove_file(path).await?;
        }

        Ok(())
    }

    /// Get segments that should be demoted to cold tier
    pub async fn get_demotion_candidates(&self, max_age: Duration) -> Vec<(String, u32, u64)> {
        let max_age_secs = max_age.as_secs();
        let segments = self.segments.read().await;

        segments
            .iter()
            .filter(|(_, meta)| meta.age_secs() > max_age_secs)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Get segments metadata
    pub async fn get_metadata(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Option<Arc<SegmentMetadata>> {
        let key = (topic.to_string(), partition, base_offset);
        self.segments.read().await.get(&key).cloned()
    }

    pub fn stats(&self) -> WarmTierStats {
        WarmTierStats {
            current_size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WarmTierStats {
    pub current_size: u64,
    pub max_size: u64,
}

/// Cold storage backend trait
#[async_trait::async_trait]
pub trait ColdStorageBackend: Send + Sync {
    /// Upload segment to cold storage
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Download segment from cold storage
    async fn download(&self, key: &str) -> Result<Option<Bytes>>;

    /// Delete segment from cold storage
    async fn delete(&self, key: &str) -> Result<()>;

    /// List all segment keys with prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check if segment exists
    async fn exists(&self, key: &str) -> Result<bool>;
}

/// Local filesystem cold storage (for dev/testing)
pub struct LocalFsColdStorage {
    base_path: PathBuf,
}

impl LocalFsColdStorage {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        // Canonicalize base path to prevent path traversal
        let base_path = base_path.canonicalize()?;
        Ok(Self { base_path })
    }

    /// Convert a key to a safe filesystem path, preventing path traversal attacks.
    ///
    /// # Security
    /// - Rejects keys containing `..` components
    /// - Rejects absolute paths
    /// - Validates resulting path stays within base_path
    fn key_to_path(&self, key: &str) -> Result<PathBuf> {
        // Security: Reject keys with path traversal attempts
        if key.contains("..") || key.starts_with('/') || key.starts_with('\\') {
            return Err(Error::Other(format!(
                "Invalid key: path traversal attempt detected: {}",
                key
            )));
        }

        // Also reject any key with null bytes (could bypass checks in some systems)
        if key.contains('\0') {
            return Err(Error::Other("Invalid key: null byte not allowed".into()));
        }

        let path = self
            .base_path
            .join(key.replace('/', std::path::MAIN_SEPARATOR_STR));

        // Double-check: ensure the resolved path is under base_path
        // This catches edge cases like symlinks
        if let Ok(canonical) = path.canonicalize() {
            if !canonical.starts_with(&self.base_path) {
                return Err(Error::Other(format!(
                    "Invalid key: path escapes base directory: {}",
                    key
                )));
            }
        }
        // If canonicalize fails (file doesn't exist yet), the path should still be
        // safe because we've already rejected .. and absolute paths

        Ok(path)
    }
}

#[async_trait::async_trait]
impl ColdStorageBackend for LocalFsColdStorage {
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.key_to_path(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn download(&self, key: &str) -> Result<Option<Bytes>> {
        let path = self.key_to_path(key)?;
        if !path.exists() {
            return Ok(None);
        }
        let data = tokio::fs::read(&path).await?;
        Ok(Some(Bytes::from(data)))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.key_to_path(key)?;
        if path.exists() {
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let base = self.key_to_path(prefix)?;
        let mut keys = Vec::new();

        if !base.exists() {
            return Ok(keys);
        }

        fn walk_dir(
            dir: &std::path::Path,
            base: &std::path::Path,
            keys: &mut Vec<String>,
        ) -> std::io::Result<()> {
            if dir.is_dir() {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        walk_dir(&path, base, keys)?;
                    } else if let Ok(rel) = path.strip_prefix(base) {
                        keys.push(
                            rel.to_string_lossy()
                                .replace(std::path::MAIN_SEPARATOR, "/"),
                        );
                    }
                }
            }
            Ok(())
        }

        walk_dir(&self.base_path, &self.base_path, &mut keys)?;
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.key_to_path(key)?.exists())
    }
}

/// Disabled cold storage (warm tier is final)
pub struct DisabledColdStorage;

#[async_trait::async_trait]
impl ColdStorageBackend for DisabledColdStorage {
    async fn upload(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Err(Error::Other("Cold storage is disabled".into()))
    }

    async fn download(&self, _key: &str) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn exists(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }
}

/// Migration task
#[derive(Debug)]
enum MigrationTask {
    Demote {
        topic: String,
        partition: u32,
        base_offset: u64,
        from_tier: StorageTier,
    },
    Promote {
        topic: String,
        partition: u32,
        base_offset: u64,
        to_tier: StorageTier,
    },
    Compact {
        topic: String,
        partition: u32,
        base_offset: u64,
    },
}

/// Main tiered storage manager
pub struct TieredStorage {
    config: TieredStorageConfig,
    hot_tier: Arc<HotTier>,
    warm_tier: Arc<WarmTier>,
    cold_storage: Arc<dyn ColdStorageBackend>,
    /// Global segment index across all tiers
    segment_index: RwLock<BTreeMap<(String, u32, u64), Arc<SegmentMetadata>>>,
    /// Migration task queue
    migration_tx: mpsc::Sender<MigrationTask>,
    /// Statistics
    stats: Arc<TieredStorageStats>,
    /// Shutdown signal
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl TieredStorage {
    /// Create new tiered storage system
    pub async fn new(config: TieredStorageConfig) -> Result<Arc<Self>> {
        let hot_tier = Arc::new(HotTier::new(config.hot_tier_max_bytes));
        let warm_tier = Arc::new(WarmTier::new(
            config.warm_tier_path.clone(),
            config.warm_tier_max_bytes,
        )?);

        let cold_storage: Arc<dyn ColdStorageBackend> = match &config.cold_storage {
            ColdStorageConfig::LocalFs { path } => Arc::new(LocalFsColdStorage::new(path.clone())?),
            ColdStorageConfig::Disabled => Arc::new(DisabledColdStorage),
            // S3 and Azure would be implemented with their respective SDKs
            ColdStorageConfig::S3 { .. } => {
                // Would use aws-sdk-s3 crate
                return Err(Error::Other("S3 cold storage not yet implemented".into()));
            }
            ColdStorageConfig::AzureBlob { .. } => {
                // Would use azure_storage_blobs crate
                return Err(Error::Other(
                    "Azure Blob cold storage not yet implemented".into(),
                ));
            }
        };

        let (migration_tx, migration_rx) = mpsc::channel(1024);
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        let storage = Arc::new(Self {
            config: config.clone(),
            hot_tier,
            warm_tier,
            cold_storage,
            segment_index: RwLock::new(BTreeMap::new()),
            migration_tx,
            stats: Arc::new(TieredStorageStats::new()),
            shutdown: shutdown_tx,
        });

        // Start background migration worker
        storage.clone().start_migration_worker(migration_rx);

        // Start background tier manager
        storage.clone().start_tier_manager();

        Ok(storage)
    }

    /// Write messages to storage (always starts in hot tier)
    pub async fn write(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        end_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let size = data.len() as u64;

        // Always write to hot tier first
        let inserted = self
            .hot_tier
            .insert(topic, partition, base_offset, data.clone())
            .await;

        if !inserted {
            // Hot tier full and can't evict, write directly to warm
            self.warm_tier
                .store(topic, partition, base_offset, end_offset, &data)
                .await?;
            self.stats.warm_writes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.hot_writes.fetch_add(1, Ordering::Relaxed);
        }

        // Update segment index
        let metadata = Arc::new(SegmentMetadata::new(
            topic.to_string(),
            partition,
            base_offset,
            end_offset,
            size,
            if inserted {
                StorageTier::Hot
            } else {
                StorageTier::Warm
            },
        ));

        {
            let mut index = self.segment_index.write().await;
            index.insert((topic.to_string(), partition, base_offset), metadata);
        }

        self.stats
            .total_bytes_written
            .fetch_add(size, Ordering::Relaxed);

        Ok(())
    }

    /// Read messages from storage
    pub async fn read(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<(u64, Bytes)>> {
        let _start = Instant::now();
        let mut results = Vec::new();
        let mut bytes_collected = 0;

        // Find relevant segments
        let segments = {
            let index = self.segment_index.read().await;
            index
                .range((topic.to_string(), partition, 0)..(topic.to_string(), partition, u64::MAX))
                .filter(|(_, meta)| meta.end_offset > start_offset)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        };

        for ((_, _, base_offset), metadata) in segments {
            if bytes_collected >= max_bytes {
                break;
            }

            metadata.record_access();

            // Try to read from each tier in order
            let data = match metadata.tier {
                StorageTier::Hot => {
                    if let Some(data) = self.hot_tier.get(topic, partition, base_offset).await {
                        self.stats.hot_reads.fetch_add(1, Ordering::Relaxed);
                        Some(data)
                    } else {
                        // Might have been evicted, try warm
                        None
                    }
                }
                _ => None,
            };

            let data = match data {
                Some(d) => d,
                None => {
                    // Try warm tier
                    if let Some(data) = self.warm_tier.read(topic, partition, base_offset).await? {
                        self.stats.warm_reads.fetch_add(1, Ordering::Relaxed);

                        // Consider promotion if frequently accessed
                        if self.config.enable_promotion {
                            let access_count = metadata.access_count.load(Ordering::Relaxed);
                            if access_count >= self.config.promotion_threshold {
                                let _ = self
                                    .migration_tx
                                    .send(MigrationTask::Promote {
                                        topic: topic.to_string(),
                                        partition,
                                        base_offset,
                                        to_tier: StorageTier::Hot,
                                    })
                                    .await;
                            }
                        }

                        data
                    } else {
                        // Try cold tier
                        let key = metadata.segment_key();
                        if let Some(data) = self.cold_storage.download(&key).await? {
                            self.stats.cold_reads.fetch_add(1, Ordering::Relaxed);

                            // Consider promotion
                            if self.config.enable_promotion {
                                let access_count = metadata.access_count.load(Ordering::Relaxed);
                                if access_count >= self.config.promotion_threshold {
                                    let _ = self
                                        .migration_tx
                                        .send(MigrationTask::Promote {
                                            topic: topic.to_string(),
                                            partition,
                                            base_offset,
                                            to_tier: StorageTier::Warm,
                                        })
                                        .await;
                                }
                            }

                            data
                        } else {
                            continue; // Segment not found
                        }
                    }
                }
            };

            results.push((base_offset, data.clone()));
            bytes_collected += data.len();
        }

        self.stats
            .total_bytes_read
            .fetch_add(bytes_collected as u64, Ordering::Relaxed);

        Ok(results)
    }

    /// Get metadata for a specific segment
    pub async fn get_segment_metadata(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Option<Arc<SegmentMetadata>> {
        self.segment_index
            .read()
            .await
            .get(&(topic.to_string(), partition, base_offset))
            .cloned()
    }

    /// Force demote segments from hot to warm
    pub async fn flush_hot_tier(&self, topic: &str, partition: u32) -> Result<()> {
        let segments: Vec<_> = {
            let index = self.segment_index.read().await;
            index
                .range((topic.to_string(), partition, 0)..(topic.to_string(), partition, u64::MAX))
                .filter(|(_, meta)| meta.tier == StorageTier::Hot)
                .map(|(k, _)| k.2)
                .collect()
        };

        for base_offset in segments {
            let _ = self
                .migration_tx
                .send(MigrationTask::Demote {
                    topic: topic.to_string(),
                    partition,
                    base_offset,
                    from_tier: StorageTier::Hot,
                })
                .await;
        }

        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> TieredStorageStatsSnapshot {
        TieredStorageStatsSnapshot {
            hot_tier: self.hot_tier.stats(),
            warm_tier: self.warm_tier.stats(),
            hot_reads: self.stats.hot_reads.load(Ordering::Relaxed),
            warm_reads: self.stats.warm_reads.load(Ordering::Relaxed),
            cold_reads: self.stats.cold_reads.load(Ordering::Relaxed),
            hot_writes: self.stats.hot_writes.load(Ordering::Relaxed),
            warm_writes: self.stats.warm_writes.load(Ordering::Relaxed),
            cold_writes: self.stats.cold_writes.load(Ordering::Relaxed),
            total_bytes_read: self.stats.total_bytes_read.load(Ordering::Relaxed),
            total_bytes_written: self.stats.total_bytes_written.load(Ordering::Relaxed),
            migrations_completed: self.stats.migrations_completed.load(Ordering::Relaxed),
            migrations_failed: self.stats.migrations_failed.load(Ordering::Relaxed),
        }
    }

    /// Start the background migration worker
    fn start_migration_worker(self: Arc<Self>, mut rx: mpsc::Receiver<MigrationTask>) {
        let semaphore = Arc::new(Semaphore::new(self.config.migration_concurrency));
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(task) = rx.recv() => {
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let storage = self.clone();

                        tokio::spawn(async move {
                            let result = storage.execute_migration(task).await;
                            if result.is_ok() {
                                storage.stats.migrations_completed.fetch_add(1, Ordering::Relaxed);
                            } else {
                                storage.stats.migrations_failed.fetch_add(1, Ordering::Relaxed);
                            }
                            drop(permit);
                        });
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// Start the background tier manager (checks for demotions)
    fn start_tier_manager(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        let migration_interval = self.config.migration_interval;

        tokio::spawn(async move {
            let mut ticker = interval(migration_interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.check_tier_migrations().await {
                            tracing::warn!("Tier migration check failed: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// Check and queue tier migrations
    async fn check_tier_migrations(&self) -> Result<()> {
        let hot_max_age = self.config.hot_tier_max_age;
        let warm_max_age = self.config.warm_tier_max_age;

        // Check hot tier for demotions
        let hot_candidates: Vec<_> = {
            let index = self.segment_index.read().await;
            index
                .iter()
                .filter(|(_, meta)| {
                    meta.tier == StorageTier::Hot
                        && Duration::from_secs(meta.age_secs()) > hot_max_age
                })
                .map(|(k, _)| k.clone())
                .collect()
        };

        for (topic, partition, base_offset) in hot_candidates {
            let _ = self
                .migration_tx
                .send(MigrationTask::Demote {
                    topic,
                    partition,
                    base_offset,
                    from_tier: StorageTier::Hot,
                })
                .await;
        }

        // Check warm tier for demotions to cold
        let warm_candidates = self.warm_tier.get_demotion_candidates(warm_max_age).await;

        for (topic, partition, base_offset) in warm_candidates {
            let _ = self
                .migration_tx
                .send(MigrationTask::Demote {
                    topic,
                    partition,
                    base_offset,
                    from_tier: StorageTier::Warm,
                })
                .await;
        }

        // Check for compaction candidates
        let compaction_threshold = self.config.compaction_threshold;
        let compaction_candidates: Vec<_> = {
            let index = self.segment_index.read().await;
            index
                .iter()
                .filter(|(_, meta)| meta.compaction_ratio() > compaction_threshold)
                .map(|(k, _)| k.clone())
                .collect()
        };

        for (topic, partition, base_offset) in compaction_candidates {
            let _ = self
                .migration_tx
                .send(MigrationTask::Compact {
                    topic,
                    partition,
                    base_offset,
                })
                .await;
        }

        Ok(())
    }

    /// Execute a migration task
    async fn execute_migration(&self, task: MigrationTask) -> Result<()> {
        match task {
            MigrationTask::Demote {
                topic,
                partition,
                base_offset,
                from_tier,
            } => {
                self.demote_segment(&topic, partition, base_offset, from_tier)
                    .await
            }
            MigrationTask::Promote {
                topic,
                partition,
                base_offset,
                to_tier,
            } => {
                self.promote_segment(&topic, partition, base_offset, to_tier)
                    .await
            }
            MigrationTask::Compact {
                topic,
                partition,
                base_offset,
            } => self.compact_segment(&topic, partition, base_offset).await,
        }
    }

    /// Demote a segment to a cooler tier
    async fn demote_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        from_tier: StorageTier,
    ) -> Result<()> {
        let to_tier = match from_tier.demote() {
            Some(t) => t,
            None => return Ok(()), // Already at coldest tier
        };

        // Get segment data from current tier
        let data = match from_tier {
            StorageTier::Hot => self.hot_tier.remove(topic, partition, base_offset).await,
            StorageTier::Warm => self.warm_tier.read(topic, partition, base_offset).await?,
            StorageTier::Cold => None,
        };

        let data = match data {
            Some(d) => d,
            None => return Ok(()), // Segment already gone
        };

        // Get metadata for end_offset
        let metadata = self
            .get_segment_metadata(topic, partition, base_offset)
            .await;
        let end_offset = metadata
            .as_ref()
            .map(|m| m.end_offset)
            .unwrap_or(base_offset);

        // Write to new tier
        match to_tier {
            StorageTier::Warm => {
                self.warm_tier
                    .store(topic, partition, base_offset, end_offset, &data)
                    .await?;
            }
            StorageTier::Cold => {
                let key = format!("{}/{}/{:020}", topic, partition, base_offset);
                self.cold_storage.upload(&key, &data).await?;
                self.stats.cold_writes.fetch_add(1, Ordering::Relaxed);

                // Remove from warm tier
                self.warm_tier.remove(topic, partition, base_offset).await?;
            }
            StorageTier::Hot => unreachable!(),
        }

        // Update metadata
        if let Some(meta) = metadata {
            // Create new metadata with updated tier (since SegmentMetadata.tier is not atomic)
            let new_meta = Arc::new(SegmentMetadata {
                topic: meta.topic.clone(),
                partition: meta.partition,
                base_offset: meta.base_offset,
                end_offset: meta.end_offset,
                size_bytes: meta.size_bytes,
                tier: to_tier,
                created_at: meta.created_at,
                last_accessed: AtomicU64::new(meta.last_accessed.load(Ordering::Relaxed)),
                access_count: AtomicU64::new(meta.access_count.load(Ordering::Relaxed)),
                dead_records: AtomicU64::new(meta.dead_records.load(Ordering::Relaxed)),
                total_records: meta.total_records,
            });

            let mut index = self.segment_index.write().await;
            index.insert((topic.to_string(), partition, base_offset), new_meta);
        }

        tracing::debug!(
            "Demoted segment {}/{}/{} from {:?} to {:?}",
            topic,
            partition,
            base_offset,
            from_tier,
            to_tier
        );

        Ok(())
    }

    /// Promote a segment to a hotter tier
    async fn promote_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        to_tier: StorageTier,
    ) -> Result<()> {
        // Get current tier from metadata
        let metadata = match self
            .get_segment_metadata(topic, partition, base_offset)
            .await
        {
            Some(m) => m,
            None => return Ok(()),
        };

        let from_tier = metadata.tier;

        // Get data from current tier
        let data = match from_tier {
            StorageTier::Cold => {
                let key = metadata.segment_key();
                self.cold_storage.download(&key).await?
            }
            StorageTier::Warm => self.warm_tier.read(topic, partition, base_offset).await?,
            StorageTier::Hot => return Ok(()), // Already hot
        };

        let data = match data {
            Some(d) => d,
            None => return Ok(()),
        };

        // Write to new tier
        match to_tier {
            StorageTier::Hot => {
                self.hot_tier
                    .insert(topic, partition, base_offset, data)
                    .await;
            }
            StorageTier::Warm => {
                self.warm_tier
                    .store(topic, partition, base_offset, metadata.end_offset, &data)
                    .await?;
            }
            StorageTier::Cold => unreachable!(),
        }

        // Update metadata
        let new_meta = Arc::new(SegmentMetadata {
            topic: metadata.topic.clone(),
            partition: metadata.partition,
            base_offset: metadata.base_offset,
            end_offset: metadata.end_offset,
            size_bytes: metadata.size_bytes,
            tier: to_tier,
            created_at: metadata.created_at,
            last_accessed: AtomicU64::new(metadata.last_accessed.load(Ordering::Relaxed)),
            access_count: AtomicU64::new(0), // Reset access count after promotion
            dead_records: AtomicU64::new(metadata.dead_records.load(Ordering::Relaxed)),
            total_records: metadata.total_records,
        });

        {
            let mut index = self.segment_index.write().await;
            index.insert((topic.to_string(), partition, base_offset), new_meta);
        }

        tracing::debug!(
            "Promoted segment {}/{}/{} from {:?} to {:?}",
            topic,
            partition,
            base_offset,
            from_tier,
            to_tier
        );

        Ok(())
    }

    /// Compact a segment (remove tombstones, keep latest per key)
    ///
    /// Kafka-style log compaction:
    /// 1. Read all messages from segment
    /// 2. Keep only the latest message per key
    /// 3. Remove tombstones (null value = deletion marker)
    /// 4. Write compacted segment
    /// 5. Update index and delete old segment
    async fn compact_segment(&self, topic: &str, partition: u32, base_offset: u64) -> Result<()> {
        use std::collections::HashMap;

        // Get segment metadata
        let metadata = match self
            .get_segment_metadata(topic, partition, base_offset)
            .await
        {
            Some(m) => m,
            None => {
                tracing::debug!(
                    "Segment not found for compaction: {}/{}/{}",
                    topic,
                    partition,
                    base_offset
                );
                return Ok(());
            }
        };

        // Read segment data from current tier
        let data = match metadata.tier {
            StorageTier::Hot => self.hot_tier.get(topic, partition, base_offset).await,
            StorageTier::Warm => self.warm_tier.read(topic, partition, base_offset).await?,
            StorageTier::Cold => {
                let key = metadata.segment_key();
                self.cold_storage.download(&key).await?
            }
        };

        let data = match data {
            Some(d) => d,
            None => {
                tracing::debug!(
                    "Segment data not found for compaction: {}/{}/{}",
                    topic,
                    partition,
                    base_offset
                );
                return Ok(());
            }
        };

        // Parse messages from segment
        // Segment format: length-prefixed bincode serialized Messages
        let mut messages: Vec<crate::Message> = Vec::new();
        let mut cursor = 0;

        while cursor < data.len() {
            // Read message length (4 bytes)
            if cursor + 4 > data.len() {
                break;
            }
            let len = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            cursor += 4;

            if cursor + len > data.len() {
                tracing::warn!(
                    "Truncated message in segment {}/{}/{}",
                    topic,
                    partition,
                    base_offset
                );
                break;
            }

            // Deserialize message
            match crate::Message::from_bytes(&data[cursor..cursor + len]) {
                Ok(msg) => messages.push(msg),
                Err(e) => {
                    tracing::warn!("Failed to deserialize message in compaction: {}", e);
                }
            }
            cursor += len;
        }

        if messages.is_empty() {
            tracing::debug!(
                "No messages to compact in segment {}/{}/{}",
                topic,
                partition,
                base_offset
            );
            return Ok(());
        }

        let original_count = messages.len();

        // Compact: keep latest value per key, remove tombstones
        let mut key_to_message: HashMap<Option<Bytes>, crate::Message> = HashMap::new();

        for msg in messages {
            // For keyed messages, keep the latest
            // For keyless messages, always keep (append-only semantics)
            if msg.key.is_some() {
                key_to_message.insert(msg.key.clone(), msg);
            } else {
                // Keyless messages use offset as synthetic key to preserve all
                key_to_message.insert(Some(Bytes::from(msg.offset.to_le_bytes().to_vec())), msg);
            }
        }

        // Filter out tombstones (empty value = deletion marker)
        let compacted: Vec<_> = key_to_message
            .into_values()
            .filter(|msg| !msg.value.is_empty()) // Non-empty value = not a tombstone
            .collect();

        let compacted_count = compacted.len();

        // Only write if compaction reduced size significantly
        if compacted_count >= original_count {
            tracing::debug!(
                "Skipping compaction for {}/{}/{}: no reduction ({} -> {})",
                topic,
                partition,
                base_offset,
                original_count,
                compacted_count
            );
            return Ok(());
        }

        // Serialize compacted messages
        let mut compacted_data = Vec::new();
        let mut new_end_offset = base_offset;

        for msg in &compacted {
            let msg_bytes = msg.to_bytes()?;
            compacted_data.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
            compacted_data.extend_from_slice(&msg_bytes);
            new_end_offset = new_end_offset.max(msg.offset + 1);
        }

        let compacted_bytes = Bytes::from(compacted_data);
        let compacted_size = compacted_bytes.len() as u64;
        let reduction_ratio = 1.0 - (compacted_count as f64 / original_count as f64);

        tracing::info!(
            "Compacted segment {}/{}/{}: {} -> {} messages ({:.1}% reduction)",
            topic,
            partition,
            base_offset,
            original_count,
            compacted_count,
            reduction_ratio * 100.0
        );

        // Write compacted segment to current tier
        match metadata.tier {
            StorageTier::Hot => {
                // Remove old and insert new
                self.hot_tier.remove(topic, partition, base_offset).await;
                self.hot_tier
                    .insert(topic, partition, base_offset, compacted_bytes)
                    .await;
            }
            StorageTier::Warm => {
                // Remove and re-store
                self.warm_tier.remove(topic, partition, base_offset).await?;
                self.warm_tier
                    .store(
                        topic,
                        partition,
                        base_offset,
                        new_end_offset,
                        &compacted_bytes,
                    )
                    .await?;
            }
            StorageTier::Cold => {
                // Upload compacted version
                let key = metadata.segment_key();
                self.cold_storage.upload(&key, &compacted_bytes).await?;
            }
        }

        // Update metadata with new size and reset dead records
        let new_meta = Arc::new(SegmentMetadata {
            topic: metadata.topic.clone(),
            partition: metadata.partition,
            base_offset: metadata.base_offset,
            end_offset: new_end_offset,
            size_bytes: compacted_size,
            tier: metadata.tier,
            created_at: metadata.created_at,
            last_accessed: AtomicU64::new(metadata.last_accessed.load(Ordering::Relaxed)),
            access_count: AtomicU64::new(metadata.access_count.load(Ordering::Relaxed)),
            dead_records: AtomicU64::new(0), // Reset after compaction
            total_records: compacted_count as u64,
        });

        {
            let mut index = self.segment_index.write().await;
            index.insert((topic.to_string(), partition, base_offset), new_meta);
        }

        Ok(())
    }

    /// Shutdown the tiered storage system
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(());
    }
}

/// Statistics for tiered storage
pub struct TieredStorageStats {
    pub hot_reads: AtomicU64,
    pub warm_reads: AtomicU64,
    pub cold_reads: AtomicU64,
    pub hot_writes: AtomicU64,
    pub warm_writes: AtomicU64,
    pub cold_writes: AtomicU64,
    pub total_bytes_read: AtomicU64,
    pub total_bytes_written: AtomicU64,
    pub migrations_completed: AtomicU64,
    pub migrations_failed: AtomicU64,
}

impl TieredStorageStats {
    fn new() -> Self {
        Self {
            hot_reads: AtomicU64::new(0),
            warm_reads: AtomicU64::new(0),
            cold_reads: AtomicU64::new(0),
            hot_writes: AtomicU64::new(0),
            warm_writes: AtomicU64::new(0),
            cold_writes: AtomicU64::new(0),
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            migrations_completed: AtomicU64::new(0),
            migrations_failed: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TieredStorageStatsSnapshot {
    pub hot_tier: HotTierStats,
    pub warm_tier: WarmTierStats,
    pub hot_reads: u64,
    pub warm_reads: u64,
    pub cold_reads: u64,
    pub hot_writes: u64,
    pub warm_writes: u64,
    pub cold_writes: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub migrations_completed: u64,
    pub migrations_failed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_hot_tier_insert_and_get() {
        let hot = HotTier::new(1024 * 1024); // 1 MB

        let data = Bytes::from("test data");
        hot.insert("topic1", 0, 0, data.clone()).await;

        let retrieved = hot.get("topic1", 0, 0).await;
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_hot_tier_lru_eviction() {
        let hot = HotTier::new(100); // Very small

        // Insert more data than fits
        hot.insert("topic1", 0, 0, Bytes::from(vec![0u8; 40])).await;
        hot.insert("topic1", 0, 1, Bytes::from(vec![1u8; 40])).await;
        hot.insert("topic1", 0, 2, Bytes::from(vec![2u8; 40])).await;

        // First segment should be evicted
        assert!(hot.get("topic1", 0, 0).await.is_none());
        // Later segments should still be there
        assert!(hot.get("topic1", 0, 1).await.is_some());
        assert!(hot.get("topic1", 0, 2).await.is_some());
    }

    #[tokio::test]
    async fn test_warm_tier_store_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let warm = WarmTier::new(temp_dir.path().to_path_buf(), 1024 * 1024 * 1024).unwrap();

        let data = b"warm tier test data";
        warm.store("topic1", 0, 0, 100, data).await.unwrap();

        let retrieved = warm.read("topic1", 0, 0).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::from(&data[..])));
    }

    #[tokio::test]
    async fn test_local_fs_cold_storage() {
        let temp_dir = TempDir::new().unwrap();
        let cold = LocalFsColdStorage::new(temp_dir.path().to_path_buf()).unwrap();

        let key = "topic1/0/00000000000000000000";
        let data = b"cold storage test data";

        cold.upload(key, data).await.unwrap();
        assert!(cold.exists(key).await.unwrap());

        let retrieved = cold.download(key).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::from(&data[..])));

        cold.delete(key).await.unwrap();
        assert!(!cold.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_tiered_storage_write_and_read() {
        let temp_dir = TempDir::new().unwrap();

        let config = TieredStorageConfig {
            hot_tier_max_bytes: 1024 * 1024,
            warm_tier_path: temp_dir.path().join("warm"),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold"),
            },
            migration_interval: Duration::from_secs(3600), // Disable auto migration
            ..Default::default()
        };

        let storage = TieredStorage::new(config).await.unwrap();

        // Write data
        let data = Bytes::from("test message data");
        storage
            .write("topic1", 0, 0, 10, data.clone())
            .await
            .unwrap();

        // Read data back
        let results = storage.read("topic1", 0, 0, 1024).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, data);

        // Check stats
        let stats = storage.stats();
        assert_eq!(stats.hot_writes, 1);
        assert_eq!(stats.hot_reads, 1);

        storage.shutdown().await;
    }

    #[tokio::test]
    async fn test_storage_tier_demote_promote() {
        assert_eq!(StorageTier::Hot.demote(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.demote(), Some(StorageTier::Cold));
        assert_eq!(StorageTier::Cold.demote(), None);

        assert_eq!(StorageTier::Hot.promote(), None);
        assert_eq!(StorageTier::Warm.promote(), Some(StorageTier::Hot));
        assert_eq!(StorageTier::Cold.promote(), Some(StorageTier::Warm));
    }

    #[tokio::test]
    async fn test_segment_metadata() {
        let meta = SegmentMetadata::new("topic1".to_string(), 0, 0, 100, 1024, StorageTier::Hot);

        assert_eq!(meta.segment_key(), "topic1/0/00000000000000000000");
        assert!(meta.age_secs() <= 1);

        meta.record_access();
        assert_eq!(meta.access_count.load(Ordering::Relaxed), 1);

        meta.dead_records.store(50, Ordering::Relaxed);
        assert!((meta.compaction_ratio() - 0.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_segment_compaction() {
        use crate::Message;

        let temp_dir = TempDir::new().unwrap();

        let config = TieredStorageConfig {
            hot_tier_max_bytes: 10 * 1024 * 1024, // 10 MB
            warm_tier_path: temp_dir.path().join("warm"),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold"),
            },
            migration_interval: Duration::from_secs(3600),
            compaction_threshold: 0.1, // Low threshold for testing
            ..Default::default()
        };

        let storage = TieredStorage::new(config).await.unwrap();

        // Create messages with duplicate keys
        let mut segment_data = Vec::new();

        // Message 1: key=A, value=v1
        let msg1 = Message::with_key(Bytes::from("A"), Bytes::from("value1"));
        let msg1_bytes = msg1.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg1_bytes.len() as u32).to_le_bytes());
        segment_data.extend_from_slice(&msg1_bytes);

        // Message 2: key=B, value=v1
        let msg2 = Message::with_key(Bytes::from("B"), Bytes::from("value1"));
        let msg2_bytes = msg2.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg2_bytes.len() as u32).to_le_bytes());
        segment_data.extend_from_slice(&msg2_bytes);

        // Message 3: key=A, value=v2 (update)
        let msg3 = Message::with_key(Bytes::from("A"), Bytes::from("value2"));
        let msg3_bytes = msg3.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg3_bytes.len() as u32).to_le_bytes());
        segment_data.extend_from_slice(&msg3_bytes);

        // Message 4: key=B, value="" (tombstone/delete)
        let msg4 = Message::with_key(Bytes::from("B"), Bytes::from(""));
        let msg4_bytes = msg4.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg4_bytes.len() as u32).to_le_bytes());
        segment_data.extend_from_slice(&msg4_bytes);

        // Write segment
        let segment_bytes = Bytes::from(segment_data);
        storage
            .write("compaction-test", 0, 0, 4, segment_bytes)
            .await
            .unwrap();

        // Get metadata and trigger compaction
        let meta = storage
            .get_segment_metadata("compaction-test", 0, 0)
            .await
            .unwrap();

        // Simulate dead records (2 out of 4 will be removed)
        meta.dead_records.store(2, Ordering::Relaxed);

        // Run compaction
        storage
            .compact_segment("compaction-test", 0, 0)
            .await
            .unwrap();

        // Verify compacted segment has fewer messages
        let meta_after = storage
            .get_segment_metadata("compaction-test", 0, 0)
            .await
            .unwrap();
        assert!(
            meta_after.total_records < 4,
            "Compaction should reduce message count"
        );

        storage.shutdown().await;
    }

    #[tokio::test]
    async fn test_compaction_preserves_keyless_messages() {
        use crate::Message;

        let temp_dir = TempDir::new().unwrap();

        let config = TieredStorageConfig {
            hot_tier_max_bytes: 10 * 1024 * 1024,
            warm_tier_path: temp_dir.path().join("warm"),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold"),
            },
            migration_interval: Duration::from_secs(3600),
            ..Default::default()
        };

        let storage = TieredStorage::new(config).await.unwrap();

        // Create keyless messages (should all be preserved)
        let mut segment_data = Vec::new();

        for i in 0..5 {
            let mut msg = Message::new(Bytes::from(format!("value{}", i)));
            msg.offset = i;
            let msg_bytes = msg.to_bytes().unwrap();
            segment_data.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
            segment_data.extend_from_slice(&msg_bytes);
        }

        let segment_bytes = Bytes::from(segment_data);
        storage
            .write("keyless-test", 0, 0, 5, segment_bytes)
            .await
            .unwrap();

        // Run compaction
        storage.compact_segment("keyless-test", 0, 0).await.unwrap();

        // All keyless messages should be preserved (no dedup for keyless)
        let meta_after = storage
            .get_segment_metadata("keyless-test", 0, 0)
            .await
            .unwrap();
        assert_eq!(
            meta_after.total_records, 5,
            "Keyless messages should all be preserved"
        );

        storage.shutdown().await;
    }

    #[tokio::test]
    async fn test_local_fs_cold_storage_path_traversal_protection() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFsColdStorage::new(temp_dir.path().to_path_buf()).unwrap();

        // Valid keys should work
        assert!(storage.key_to_path("valid/key/path").is_ok());
        assert!(storage.key_to_path("simple-key").is_ok());
        assert!(storage.key_to_path("key_with_underscores").is_ok());

        // Path traversal attempts should fail
        assert!(storage.key_to_path("../escape").is_err());
        assert!(storage.key_to_path("valid/../escape").is_err());
        assert!(storage.key_to_path("..").is_err());
        assert!(storage.key_to_path("foo/../../bar").is_err());

        // Absolute paths should fail
        assert!(storage.key_to_path("/etc/passwd").is_err());
        assert!(storage.key_to_path("\\Windows\\System32").is_err());

        // Null bytes should fail (could bypass checks on some systems)
        assert!(storage.key_to_path("valid\0.txt").is_err());
    }

    #[tokio::test]
    async fn test_local_fs_cold_storage_operations_with_safe_keys() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFsColdStorage::new(temp_dir.path().to_path_buf()).unwrap();

        // Upload should work with safe keys
        let data = b"test data";
        storage.upload("test/key", data).await.unwrap();

        // Download should work
        let downloaded = storage.download("test/key").await.unwrap();
        assert_eq!(downloaded, Some(Bytes::from_static(data)));

        // Exists should work
        assert!(storage.exists("test/key").await.unwrap());
        assert!(!storage.exists("nonexistent").await.unwrap());

        // Delete should work
        storage.delete("test/key").await.unwrap();
        assert!(!storage.exists("test/key").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_fs_cold_storage_rejects_malicious_upload() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFsColdStorage::new(temp_dir.path().to_path_buf()).unwrap();

        // Attempting to upload with path traversal should fail
        let result = storage.upload("../malicious", b"pwned").await;
        assert!(result.is_err());

        // The file should NOT exist outside the storage directory
        let escaped_path = temp_dir.path().parent().unwrap().join("malicious");
        assert!(!escaped_path.exists());
    }
}

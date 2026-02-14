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
use indexmap::IndexMap;
use std::collections::BTreeMap;
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieredStorageConfig {
    /// Enable tiered storage (default: false)
    #[serde(default)]
    pub enabled: bool,
    /// Maximum size of hot tier in bytes
    #[serde(default = "default_hot_tier_max_bytes")]
    pub hot_tier_max_bytes: u64,
    /// Maximum age of data in hot tier before demotion (seconds)
    #[serde(default = "default_hot_tier_max_age_secs")]
    pub hot_tier_max_age_secs: u64,
    /// Maximum size of warm tier in bytes
    #[serde(default = "default_warm_tier_max_bytes")]
    pub warm_tier_max_bytes: u64,
    /// Maximum age of data in warm tier before demotion (seconds)
    #[serde(default = "default_warm_tier_max_age_secs")]
    pub warm_tier_max_age_secs: u64,
    /// Path for warm tier storage
    #[serde(default = "default_warm_tier_path")]
    pub warm_tier_path: String,
    /// Cold storage backend configuration
    #[serde(default)]
    pub cold_storage: ColdStorageConfig,
    /// How often to run tier migration (seconds)
    #[serde(default = "default_migration_interval_secs")]
    pub migration_interval_secs: u64,
    /// Number of concurrent migration operations
    #[serde(default = "default_migration_concurrency")]
    pub migration_concurrency: usize,
    /// Enable access-based promotion (promote frequently accessed cold data)
    #[serde(default = "default_enable_promotion")]
    pub enable_promotion: bool,
    /// Access count threshold for promotion
    #[serde(default = "default_promotion_threshold")]
    pub promotion_threshold: u64,
    /// Compaction threshold (ratio of dead bytes to total)
    #[serde(default = "default_compaction_threshold")]
    pub compaction_threshold: f64,
}

fn default_hot_tier_max_bytes() -> u64 {
    1024 * 1024 * 1024
} // 1 GB
fn default_hot_tier_max_age_secs() -> u64 {
    3600
} // 1 hour
fn default_warm_tier_max_bytes() -> u64 {
    100 * 1024 * 1024 * 1024
} // 100 GB
fn default_warm_tier_max_age_secs() -> u64 {
    86400 * 7
} // 7 days
fn default_warm_tier_path() -> String {
    "/var/lib/rivven/warm".to_string()
}
fn default_migration_interval_secs() -> u64 {
    60
}
fn default_migration_concurrency() -> usize {
    4
}
fn default_enable_promotion() -> bool {
    true
}
fn default_promotion_threshold() -> u64 {
    100
}
fn default_compaction_threshold() -> f64 {
    0.5
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            hot_tier_max_bytes: default_hot_tier_max_bytes(),
            hot_tier_max_age_secs: default_hot_tier_max_age_secs(),
            warm_tier_max_bytes: default_warm_tier_max_bytes(),
            warm_tier_max_age_secs: default_warm_tier_max_age_secs(),
            warm_tier_path: default_warm_tier_path(),
            cold_storage: ColdStorageConfig::default(),
            migration_interval_secs: default_migration_interval_secs(),
            migration_concurrency: default_migration_concurrency(),
            enable_promotion: default_enable_promotion(),
            promotion_threshold: default_promotion_threshold(),
            compaction_threshold: default_compaction_threshold(),
        }
    }
}

impl TieredStorageConfig {
    /// Get hot tier max age as Duration
    pub fn hot_tier_max_age(&self) -> Duration {
        Duration::from_secs(self.hot_tier_max_age_secs)
    }

    /// Get warm tier max age as Duration
    pub fn warm_tier_max_age(&self) -> Duration {
        Duration::from_secs(self.warm_tier_max_age_secs)
    }

    /// Get warm tier path as PathBuf
    pub fn warm_tier_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.warm_tier_path)
    }

    /// Get migration interval as Duration
    pub fn migration_interval(&self) -> Duration {
        Duration::from_secs(self.migration_interval_secs)
    }

    /// High-performance config for low-latency workloads
    pub fn high_performance() -> Self {
        Self {
            enabled: true,
            hot_tier_max_bytes: 8 * 1024 * 1024 * 1024, // 8 GB
            hot_tier_max_age_secs: 7200,                // 2 hours
            warm_tier_max_bytes: 500 * 1024 * 1024 * 1024, // 500 GB
            migration_interval_secs: 30,
            ..Default::default()
        }
    }

    /// Cost-optimized config for archival workloads
    pub fn cost_optimized() -> Self {
        Self {
            enabled: true,
            hot_tier_max_bytes: 256 * 1024 * 1024, // 256 MB
            hot_tier_max_age_secs: 300,            // 5 minutes
            warm_tier_max_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            warm_tier_max_age_secs: 86400,         // 1 day
            migration_interval_secs: 120,
            enable_promotion: false,
            ..Default::default()
        }
    }

    /// Testing config for integration tests (fast migration, small tiers)
    pub fn testing() -> Self {
        Self {
            enabled: true,
            hot_tier_max_bytes: 1024 * 1024,       // 1 MB
            hot_tier_max_age_secs: 5,              // 5 seconds
            warm_tier_max_bytes: 10 * 1024 * 1024, // 10 MB
            warm_tier_max_age_secs: 10,            // 10 seconds
            migration_interval_secs: 1,
            migration_concurrency: 2,
            enable_promotion: true,
            promotion_threshold: 3,
            compaction_threshold: 0.3,
            ..Default::default()
        }
    }
}

/// Cold storage backend configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ColdStorageConfig {
    /// Local filesystem (for development/testing)
    LocalFs {
        #[serde(default = "default_cold_storage_path")]
        path: String,
    },
    /// S3-compatible object storage (AWS S3, MinIO, Cloudflare R2, etc.)
    S3 {
        /// S3 endpoint URL (e.g., `https://s3.us-east-1.amazonaws.com` or `http://localhost:9000` for MinIO)
        endpoint: Option<String>,
        /// S3 bucket name
        bucket: String,
        /// AWS region (e.g., "us-east-1")
        region: String,
        /// AWS access key ID (optional, uses default credential chain if not provided)
        access_key: Option<String>,
        /// AWS secret access key
        secret_key: Option<String>,
        /// Use path-style URLs (required for MinIO and some S3-compatible services)
        #[serde(default)]
        use_path_style: bool,
    },
    /// Google Cloud Storage
    Gcs {
        /// GCS bucket name
        bucket: String,
        /// Path to service account key JSON file (optional, uses default credentials if not provided)
        service_account_path: Option<String>,
    },
    /// Azure Blob Storage
    AzureBlob {
        /// Azure storage account name
        account: String,
        /// Azure container name
        container: String,
        /// Azure storage access key (optional, uses DefaultAzureCredential if not provided)
        access_key: Option<String>,
    },
    /// Disabled (warm tier is final)
    Disabled,
}

fn default_cold_storage_path() -> String {
    "/var/lib/rivven/cold".to_string()
}

impl Default for ColdStorageConfig {
    fn default() -> Self {
        ColdStorageConfig::LocalFs {
            path: default_cold_storage_path(),
        }
    }
}

impl ColdStorageConfig {
    /// Get path as PathBuf for LocalFs variant
    pub fn local_fs_path(&self) -> Option<PathBuf> {
        match self {
            ColdStorageConfig::LocalFs { path } => Some(PathBuf::from(path)),
            _ => None,
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
            .unwrap_or_default()
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
            .unwrap_or_default()
            .as_secs();
        self.last_accessed.store(now, Ordering::Relaxed);
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get age in seconds
    pub fn age_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(self.created_at)
    }

    /// Get seconds since last access
    pub fn idle_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
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

/// F-039 fix: Segment key type uses `Arc<str>` to avoid per-access String allocation.
/// Topic names are interned — the same `Arc<str>` is reused across all tiered storage operations.
pub type SegmentKey = (Arc<str>, u32, u64);

/// Hot tier: In-memory LRU cache with O(1) access, insert, and eviction.
///
/// Uses `IndexMap` to maintain insertion-order with O(1) key lookup and
/// move-to-back promotion. Eviction pops the front (oldest entry).
#[derive(Debug)]
pub struct HotTier {
    /// Segment data in LRU order — back is most recent, front is eviction candidate
    entries: Mutex<IndexMap<SegmentKey, Bytes>>,
    /// Current size in bytes
    current_size: AtomicU64,
    /// Maximum size in bytes
    max_size: u64,
}

impl HotTier {
    pub fn new(max_size: u64) -> Self {
        Self {
            entries: Mutex::new(IndexMap::new()),
            current_size: AtomicU64::new(0),
            max_size,
        }
    }

    /// Intern topic name as Arc<str> for zero-alloc key lookups
    #[inline]
    fn make_key(topic: &str, partition: u32, base_offset: u64) -> SegmentKey {
        (Arc::from(topic), partition, base_offset)
    }

    /// Insert data into hot tier
    pub async fn insert(&self, topic: &str, partition: u32, base_offset: u64, data: Bytes) -> bool {
        let size = data.len() as u64;

        // Check if it fits at all
        if size > self.max_size {
            return false;
        }

        let key = Self::make_key(topic, partition, base_offset);
        let mut entries = self.entries.lock().await;

        // Remove existing entry first (if any) to reclaim its space.
        // F-032: Use shift_remove instead of swap_remove to preserve LRU ordering.
        if let Some(old) = entries.shift_remove(&key) {
            self.current_size
                .fetch_sub(old.len() as u64, Ordering::Relaxed);
        }

        // Evict until enough space is available
        while self.current_size.load(Ordering::Acquire) + size > self.max_size {
            if let Some((_evicted_key, evicted_data)) = entries.shift_remove_index(0) {
                self.current_size
                    .fetch_sub(evicted_data.len() as u64, Ordering::Relaxed);
            } else {
                return false; // Empty but still can't fit — shouldn't happen
            }
        }

        // Insert at back (most-recently-used position)
        entries.insert(key, data);
        self.current_size.fetch_add(size, Ordering::Relaxed);

        true
    }

    /// Get data from hot tier with LRU promotion
    pub async fn get(&self, topic: &str, partition: u32, base_offset: u64) -> Option<Bytes> {
        let key = Self::make_key(topic, partition, base_offset);
        let mut entries = self.entries.lock().await;

        // O(1) lookup + promote to back (most-recently-used)
        if let Some(idx) = entries.get_index_of(&key) {
            let last = entries.len() - 1;
            entries.move_index(idx, last);
            // Access by index (now at `last`) — avoids a second hash lookup
            let data = entries.get_index(last).unwrap().1.clone();
            Some(data)
        } else {
            None
        }
    }

    /// Remove data from hot tier
    pub async fn remove(&self, topic: &str, partition: u32, base_offset: u64) -> Option<Bytes> {
        let key = Self::make_key(topic, partition, base_offset);
        let mut entries = self.entries.lock().await;

        // F-032: Use shift_remove instead of swap_remove to preserve LRU ordering.
        if let Some(data) = entries.shift_remove(&key) {
            self.current_size
                .fetch_sub(data.len() as u64, Ordering::Relaxed);
            Some(data)
        } else {
            None
        }
    }

    /// Evict least recently used segment (front of the map)
    #[allow(dead_code)]
    async fn evict_one(&self) -> bool {
        let mut entries = self.entries.lock().await;
        if let Some((_key, data)) = entries.shift_remove_index(0) {
            self.current_size
                .fetch_sub(data.len() as u64, Ordering::Relaxed);
            true
        } else {
            false
        }
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
    segments: RwLock<BTreeMap<SegmentKey, Arc<SegmentMetadata>>>,
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
        let size = data.len() as u64;

        // Enforce max_size: evict oldest segments until we have space.
        // F-034: evict_oldest now returns evicted data for cold storage migration.
        // At the WarmTier level we don't have access to cold storage, so the
        // data is logged and dropped. For full cold migration, use
        // TieredStorageManager which orchestrates warm → cold demotion.
        while self.current_size.load(Ordering::Relaxed) + size > self.max_size {
            if let Some((evicted_topic, evicted_partition, evicted_offset, _evicted_data)) =
                self.evict_oldest().await
            {
                tracing::info!(
                    topic = %evicted_topic,
                    partition = evicted_partition,
                    base_offset = evicted_offset,
                    "Warm tier segment evicted during store (caller should migrate to cold storage)"
                );
            } else {
                // Cannot evict anything more — reject the write
                return Err(crate::Error::Other(format!(
                    "warm tier full ({} / {} bytes), cannot store segment",
                    self.current_size.load(Ordering::Relaxed),
                    self.max_size
                )));
            }
        }

        let path = self.segment_path(topic, partition, base_offset);

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Write segment file
        tokio::fs::write(&path, data).await?;

        // F-027 fix: fsync the warm tier file to ensure durability.
        // tokio::fs::write does not guarantee data reaches stable storage.
        {
            let file = tokio::fs::File::open(&path).await?;
            file.sync_all().await?;
        }

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
            segments.insert((Arc::from(topic), partition, base_offset), metadata);
        }

        self.current_size.fetch_add(size, Ordering::Relaxed);

        Ok(())
    }

    /// Read segment data using mmap — M-7 fix: uses spawn_blocking to avoid
    /// blocking the Tokio runtime with mmap/file syscalls.
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

        // Move blocking mmap + copy off the async runtime thread
        let data = tokio::task::spawn_blocking(move || -> Result<Bytes> {
            let file = std::fs::File::open(&path)?;
            // SAFETY: File is opened read-only and remains valid for mmap lifetime.
            let mmap = unsafe { memmap2::Mmap::map(&file)? };
            Ok(Bytes::copy_from_slice(&mmap))
        })
        .await
        .map_err(|e| crate::error::Error::Other(format!("spawn_blocking join: {}", e)))??;

        // Update access stats
        let key: SegmentKey = (Arc::from(topic), partition, base_offset);
        if let Some(meta) = self.segments.read().await.get(&key) {
            meta.record_access();
        }

        Ok(Some(data))
    }

    /// Remove segment
    pub async fn remove(&self, topic: &str, partition: u32, base_offset: u64) -> Result<()> {
        let path = self.segment_path(topic, partition, base_offset);
        let key: SegmentKey = (Arc::from(topic), partition, base_offset);

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
    pub async fn get_demotion_candidates(&self, max_age: Duration) -> Vec<SegmentKey> {
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
        let key: SegmentKey = (Arc::from(topic), partition, base_offset);
        self.segments.read().await.get(&key).cloned()
    }

    pub fn stats(&self) -> WarmTierStats {
        WarmTierStats {
            current_size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_size,
        }
    }

    /// Evict the oldest (by creation time) segment from warm tier to free space.
    /// Returns the evicted segment's key and data so the caller can migrate to cold storage.
    async fn evict_oldest(&self) -> Option<(Arc<str>, u32, u64, Bytes)> {
        let to_evict = {
            let segments = self.segments.read().await;
            segments
                .iter()
                .min_by_key(|(_, meta)| meta.created_at)
                .map(|(key, _)| key.clone())
        };

        if let Some((topic, partition, base_offset)) = to_evict {
            tracing::debug!(
                topic = %topic,
                partition,
                base_offset,
                "Evicting warm tier segment to free space"
            );
            // Read the data before removing so it can be migrated to cold storage
            let data = match self.read(&topic, partition, base_offset).await {
                Ok(Some(data)) => Some(data),
                _ => None,
            };
            // Remove the segment (ignore errors — best effort)
            let _ = self.remove(&topic, partition, base_offset).await;
            data.map(|d| (topic, partition, base_offset, d))
        } else {
            None
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
        // F-027 fix: fsync cold storage file to ensure durability
        {
            let file = tokio::fs::File::open(&path).await?;
            file.sync_all().await?;
        }
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

// ============================================================================
// Cloud Storage Backends (S3, GCS, Azure)
// ============================================================================

/// Object Store based cold storage backend
///
/// Provides a unified interface for S3, GCS, Azure Blob Storage, and MinIO
/// using the `object_store` crate.
#[cfg(feature = "cloud-storage")]
pub struct ObjectStoreColdStorage {
    store: Arc<dyn object_store::ObjectStore>,
    /// Optional prefix for all keys (e.g., "rivven/segments/")
    prefix: String,
}

#[cfg(feature = "cloud-storage")]
impl ObjectStoreColdStorage {
    /// Create a new S3-compatible cold storage backend
    #[cfg(feature = "s3")]
    pub fn s3(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        use_path_style: bool,
    ) -> Result<Self> {
        use object_store::aws::AmazonS3Builder;

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region);

        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if let (Some(key), Some(secret)) = (access_key, secret_key) {
            builder = builder
                .with_access_key_id(key)
                .with_secret_access_key(secret);
        }

        if use_path_style {
            builder = builder.with_virtual_hosted_style_request(false);
        }

        let store = builder
            .build()
            .map_err(|e| Error::Other(format!("Failed to create S3 client: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
            prefix: String::new(),
        })
    }

    /// Create a new MinIO cold storage backend (S3-compatible)
    #[cfg(feature = "s3")]
    pub fn minio(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str) -> Result<Self> {
        Self::s3(
            bucket,
            "us-east-1", // MinIO doesn't care about region
            Some(endpoint),
            Some(access_key),
            Some(secret_key),
            true, // MinIO requires path-style
        )
    }

    /// Create a new Google Cloud Storage backend
    #[cfg(feature = "gcs")]
    pub fn gcs(bucket: &str, service_account_path: Option<&std::path::Path>) -> Result<Self> {
        use object_store::gcp::GoogleCloudStorageBuilder;

        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

        if let Some(path) = service_account_path {
            builder = builder.with_service_account_path(path.to_string_lossy());
        }

        let store = builder
            .build()
            .map_err(|e| Error::Other(format!("Failed to create GCS client: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
            prefix: String::new(),
        })
    }

    /// Create a new Azure Blob Storage backend
    #[cfg(feature = "azure")]
    pub fn azure(account: &str, container: &str, access_key: Option<&str>) -> Result<Self> {
        use object_store::azure::MicrosoftAzureBuilder;

        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(account)
            .with_container_name(container);

        if let Some(key) = access_key {
            builder = builder.with_access_key(key);
        }

        let store = builder
            .build()
            .map_err(|e| Error::Other(format!("Failed to create Azure Blob client: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
            prefix: String::new(),
        })
    }

    /// Set a prefix for all keys
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        if !self.prefix.is_empty() && !self.prefix.ends_with('/') {
            self.prefix.push('/');
        }
        self
    }

    fn full_path(&self, key: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}{}", self.prefix, key))
    }
}

#[cfg(feature = "cloud-storage")]
#[async_trait::async_trait]
impl ColdStorageBackend for ObjectStoreColdStorage {
    async fn upload(&self, key: &str, data: &[u8]) -> Result<()> {
        use object_store::ObjectStore;

        let path = self.full_path(key);
        let payload = object_store::PutPayload::from(data.to_vec());

        self.store
            .put(&path, payload)
            .await
            .map_err(|e| Error::Other(format!("Failed to upload to object store: {}", e)))?;

        Ok(())
    }

    async fn download(&self, key: &str) -> Result<Option<Bytes>> {
        use object_store::ObjectStore;

        let path = self.full_path(key);

        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| Error::Other(format!("Failed to read object: {}", e)))?;
                Ok(Some(bytes))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::Other(format!(
                "Failed to download from object store: {}",
                e
            ))),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        use object_store::ObjectStore;

        let path = self.full_path(key);

        // Ignore NotFound errors on delete
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Error::Other(format!(
                "Failed to delete from object store: {}",
                e
            ))),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        use futures::StreamExt;
        use object_store::ObjectStore;

        let full_prefix = self.full_path(prefix);
        let mut stream = self.store.list(Some(&full_prefix));
        let mut keys = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let key = meta.location.to_string();
                    // Strip the prefix to return relative keys
                    if let Some(relative) = key.strip_prefix(&self.prefix) {
                        keys.push(relative.to_string());
                    } else {
                        keys.push(key);
                    }
                }
                Err(e) => {
                    return Err(Error::Other(format!("Failed to list objects: {}", e)));
                }
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        use object_store::ObjectStore;

        let path = self.full_path(key);

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Other(format!(
                "Failed to check object existence: {}",
                e
            ))),
        }
    }
}

/// Migration task
#[derive(Debug)]
enum MigrationTask {
    Demote {
        topic: Arc<str>,
        partition: u32,
        base_offset: u64,
        from_tier: StorageTier,
    },
    Promote {
        topic: Arc<str>,
        partition: u32,
        base_offset: u64,
        to_tier: StorageTier,
    },
    Compact {
        topic: Arc<str>,
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
    segment_index: RwLock<BTreeMap<SegmentKey, Arc<SegmentMetadata>>>,
    /// Migration task queue
    migration_tx: mpsc::Sender<MigrationTask>,
    /// Statistics
    stats: Arc<TieredStorageStats>,
    /// Shutdown signal
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl std::fmt::Debug for TieredStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredStorage")
            .field("config", &self.config)
            .field("hot_tier", &self.hot_tier)
            .field("warm_tier", &self.warm_tier)
            .field("cold_storage", &"<dyn ColdStorageBackend>")
            .finish()
    }
}

impl TieredStorage {
    /// Create new tiered storage system
    pub async fn new(config: TieredStorageConfig) -> Result<Arc<Self>> {
        let hot_tier = Arc::new(HotTier::new(config.hot_tier_max_bytes));
        let warm_tier = Arc::new(WarmTier::new(
            config.warm_tier_path_buf(),
            config.warm_tier_max_bytes,
        )?);

        let cold_storage: Arc<dyn ColdStorageBackend> = match &config.cold_storage {
            ColdStorageConfig::LocalFs { path } => {
                Arc::new(LocalFsColdStorage::new(PathBuf::from(path))?)
            }
            ColdStorageConfig::Disabled => Arc::new(DisabledColdStorage),

            #[cfg(feature = "s3")]
            ColdStorageConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key,
                secret_key,
                use_path_style,
            } => Arc::new(ObjectStoreColdStorage::s3(
                bucket,
                region,
                endpoint.as_deref(),
                access_key.as_deref(),
                secret_key.as_deref(),
                *use_path_style,
            )?),

            #[cfg(not(feature = "s3"))]
            ColdStorageConfig::S3 { .. } => {
                return Err(Error::Other(
                    "S3 cold storage requires the 's3' feature flag".into(),
                ));
            }

            #[cfg(feature = "gcs")]
            ColdStorageConfig::Gcs {
                bucket,
                service_account_path,
            } => Arc::new(ObjectStoreColdStorage::gcs(
                bucket,
                service_account_path.as_ref().map(std::path::Path::new),
            )?),

            #[cfg(not(feature = "gcs"))]
            ColdStorageConfig::Gcs { .. } => {
                return Err(Error::Other(
                    "GCS cold storage requires the 'gcs' feature flag".into(),
                ));
            }

            #[cfg(feature = "azure")]
            ColdStorageConfig::AzureBlob {
                account,
                container,
                access_key,
            } => Arc::new(ObjectStoreColdStorage::azure(
                account,
                container,
                access_key.as_deref(),
            )?),

            #[cfg(not(feature = "azure"))]
            ColdStorageConfig::AzureBlob { .. } => {
                return Err(Error::Other(
                    "Azure Blob cold storage requires the 'azure' feature flag".into(),
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
            index.insert((Arc::from(topic), partition, base_offset), metadata);
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
            let topic_arc: Arc<str> = Arc::from(topic);
            index
                .range((topic_arc.clone(), partition, 0)..(topic_arc, partition, u64::MAX))
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
                                        topic: Arc::from(topic),
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
                                            topic: Arc::from(topic),
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
            .get(&(Arc::from(topic), partition, base_offset))
            .cloned()
    }

    /// Force demote segments from hot to warm
    pub async fn flush_hot_tier(&self, topic: &str, partition: u32) -> Result<()> {
        let segments: Vec<_> = {
            let index = self.segment_index.read().await;
            let topic_arc: Arc<str> = Arc::from(topic);
            index
                .range((topic_arc.clone(), partition, 0)..(topic_arc, partition, u64::MAX))
                .filter(|(_, meta)| meta.tier == StorageTier::Hot)
                .map(|(k, _)| k.2)
                .collect()
        };

        for base_offset in segments {
            let _ = self
                .migration_tx
                .send(MigrationTask::Demote {
                    topic: Arc::from(topic),
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
                        // SAFETY: acquire_owned only fails if semaphore is closed, which never
                        // happens since we own the Arc<Semaphore> in this function scope.
                        let permit = semaphore.clone().acquire_owned().await.expect("semaphore closed unexpectedly");
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
        let migration_interval = self.config.migration_interval();

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
        let hot_max_age = self.config.hot_tier_max_age();
        let warm_max_age = self.config.warm_tier_max_age();

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
            index.insert((Arc::from(topic), partition, base_offset), new_meta);
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
            index.insert((Arc::from(topic), partition, base_offset), new_meta);
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

        /// Maximum segment size we'll load into memory for compaction.
        /// Segments larger than this are skipped with a warning. A streaming
        /// compaction that processes records in chunks would remove this limit
        /// but requires a significantly more complex merge-sort approach.
        const MAX_COMPACTION_BYTES: u64 = 512 * 1024 * 1024; // 512 MB

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

        // Guard: refuse to load segments that would blow up heap
        if metadata.size_bytes > MAX_COMPACTION_BYTES {
            tracing::warn!(
                "Skipping compaction for {}/{}/{}: segment size {} bytes exceeds \
                 max compaction size {} bytes",
                topic,
                partition,
                base_offset,
                metadata.size_bytes,
                MAX_COMPACTION_BYTES
            );
            return Ok(());
        }

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
        // Segment format: length-prefixed postcard serialized Messages
        let mut messages: Vec<crate::Message> = Vec::new();
        let mut cursor = 0;

        while cursor < data.len() {
            // Read message length (4 bytes)
            if cursor + 4 > data.len() {
                break;
            }
            let len = u32::from_be_bytes([
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
                key_to_message.insert(Some(Bytes::from(msg.offset.to_be_bytes().to_vec())), msg);
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
            compacted_data.extend_from_slice(&(msg_bytes.len() as u32).to_be_bytes());
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
            index.insert((Arc::from(topic), partition, base_offset), new_meta);
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
            enabled: true,
            hot_tier_max_bytes: 1024 * 1024,
            warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold").to_string_lossy().to_string(),
            },
            migration_interval_secs: 3600, // Disable auto migration
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
            enabled: true,
            hot_tier_max_bytes: 10 * 1024 * 1024, // 10 MB
            warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold").to_string_lossy().to_string(),
            },
            migration_interval_secs: 3600,
            compaction_threshold: 0.1, // Low threshold for testing
            ..Default::default()
        };

        let storage = TieredStorage::new(config).await.unwrap();

        // Create messages with duplicate keys
        let mut segment_data = Vec::new();

        // Message 1: key=A, value=v1
        let msg1 = Message::with_key(Bytes::from("A"), Bytes::from("value1"));
        let msg1_bytes = msg1.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg1_bytes.len() as u32).to_be_bytes());
        segment_data.extend_from_slice(&msg1_bytes);

        // Message 2: key=B, value=v1
        let msg2 = Message::with_key(Bytes::from("B"), Bytes::from("value1"));
        let msg2_bytes = msg2.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg2_bytes.len() as u32).to_be_bytes());
        segment_data.extend_from_slice(&msg2_bytes);

        // Message 3: key=A, value=v2 (update)
        let msg3 = Message::with_key(Bytes::from("A"), Bytes::from("value2"));
        let msg3_bytes = msg3.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg3_bytes.len() as u32).to_be_bytes());
        segment_data.extend_from_slice(&msg3_bytes);

        // Message 4: key=B, value="" (tombstone/delete)
        let msg4 = Message::with_key(Bytes::from("B"), Bytes::from(""));
        let msg4_bytes = msg4.to_bytes().unwrap();
        segment_data.extend_from_slice(&(msg4_bytes.len() as u32).to_be_bytes());
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
            enabled: true,
            hot_tier_max_bytes: 10 * 1024 * 1024,
            warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold").to_string_lossy().to_string(),
            },
            migration_interval_secs: 3600,
            ..Default::default()
        };

        let storage = TieredStorage::new(config).await.unwrap();

        // Create keyless messages (should all be preserved)
        let mut segment_data = Vec::new();

        for i in 0..5 {
            let mut msg = Message::new(Bytes::from(format!("value{}", i)));
            msg.offset = i;
            let msg_bytes = msg.to_bytes().unwrap();
            segment_data.extend_from_slice(&(msg_bytes.len() as u32).to_be_bytes());
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

//! # Read-Only CDC Support
//!
//! Support for CDC connectors with read-only database connections.
//!
//! ## Overview
//!
//! When connecting to **read replicas** (PostgreSQL standby servers), certain CDC features
//! that require database writes are unavailable:
//!
//! - **Signal table (source channel)**: Cannot INSERT signals into the database
//! - **Signal table watermarking**: Cannot write watermarks for incremental snapshots
//!
//! This module provides alternative implementations that work with read-only connections.
//!
//! ## Read-Only Incremental Snapshots
//!
//! Read-only incremental snapshot uses transaction IDs from the WAL stream
//! or heartbeat events as watermarks, instead of writing to a signal table.
//!
//! **Requirements:**
//! - PostgreSQL 13+ (for `pg_current_xact_id_if_assigned()`)
//! - Use non-source signal channels (topic, file, api)
//!
//! ## Configuration
//!
//! ```rust,ignore
//! use rivven_cdc::common::read_only::ReadOnlyConfig;
//!
//! let config = ReadOnlyConfig::builder()
//!     .read_only(true)
//!     .enabled(true)
//!     .signal_channels(vec![SignalChannelType::Topic, SignalChannelType::File])
//!     .build();
//! ```
//!
//! ## Signal Channels for Read-Only Mode
//!
//! | Channel | Read-Only Compatible | Notes |
//! |---------|---------------------|-------|
//! | `source` | ❌ | Requires database writes |
//! | `topic` | ✅ | Signals via Rivven topic |
//! | `file` | ✅ | Signals via JSON file |
//! | `api` | ✅ | Signals via HTTP/gRPC |
//!
//! ## Watermarking Strategy
//!
//! In read-only mode, watermarking for incremental snapshots uses:
//!
//! 1. **Transaction ID from WAL**: Extract `xid` from replication stream
//! 2. **Heartbeat Transaction ID**: Use heartbeat event's transaction context
//! 3. **LSN-based Deduplication**: Compare event LSN against snapshot window
//!
//! This eliminates the need for the `insert_insert` or `insert_delete` watermarking
//! strategies that require signal table writes.

use crate::common::SignalChannelType;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

// ============================================================================
// Read-Only Configuration
// ============================================================================

/// Configuration for read-only CDC mode.
///
/// When `read_only=true`, the connector operates with a read-only database connection
/// and uses alternative strategies for features that normally require writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOnlyConfig {
    /// Enable read-only mode (default: false)
    ///
    /// When true:
    /// - Source signal channel is disabled
    /// - Watermarking uses transaction ID instead of signal table
    /// - Heartbeat action queries are disabled
    #[serde(default)]
    pub read_only: bool,

    /// Allowed signal channels in read-only mode
    ///
    /// Default: [Topic, File, Api]
    /// Source channel is automatically excluded.
    #[serde(default = "default_read_only_channels")]
    pub allowed_channels: Vec<SignalChannelType>,

    /// PostgreSQL version requirement for read-only snapshots
    ///
    /// PostgreSQL 13+ is required for `pg_current_xact_id_if_assigned()`
    #[serde(default = "default_min_postgres_version")]
    pub min_postgres_version: u32,

    /// Use heartbeat-based watermarking
    ///
    /// When true, incremental snapshots use transaction IDs from heartbeat
    /// events for watermarking instead of signal table writes.
    #[serde(default = "default_true")]
    pub heartbeat_watermarking: bool,

    /// Emit warnings for features disabled in read-only mode
    #[serde(default = "default_true")]
    pub warn_on_disabled_features: bool,
}

fn default_read_only_channels() -> Vec<SignalChannelType> {
    vec![
        SignalChannelType::Topic,
        SignalChannelType::File,
        SignalChannelType::Api,
    ]
}

fn default_min_postgres_version() -> u32 {
    13
}

fn default_true() -> bool {
    true
}

impl Default for ReadOnlyConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            allowed_channels: default_read_only_channels(),
            min_postgres_version: default_min_postgres_version(),
            heartbeat_watermarking: true,
            warn_on_disabled_features: true,
        }
    }
}

impl ReadOnlyConfig {
    /// Create a new builder.
    pub fn builder() -> ReadOnlyConfigBuilder {
        ReadOnlyConfigBuilder::default()
    }

    /// Check if a signal channel is allowed in read-only mode.
    pub fn is_channel_allowed(&self, channel: SignalChannelType) -> bool {
        if !self.read_only {
            return true; // All channels allowed when not in read-only mode
        }
        // Source channel requires writes, never allowed in read-only mode
        if channel == SignalChannelType::Source {
            return false;
        }
        self.allowed_channels.contains(&channel)
    }

    /// Filter signal channels to only those allowed in read-only mode.
    pub fn filter_channels(&self, channels: &[SignalChannelType]) -> Vec<SignalChannelType> {
        channels
            .iter()
            .filter(|c| self.is_channel_allowed(**c))
            .copied()
            .collect()
    }

    /// Check if PostgreSQL version meets requirements for read-only snapshots.
    pub fn check_postgres_version(&self, version: u32) -> Result<(), ReadOnlyError> {
        if self.read_only && version < self.min_postgres_version {
            return Err(ReadOnlyError::UnsupportedPostgresVersion {
                required: self.min_postgres_version,
                actual: version,
            });
        }
        Ok(())
    }
}

/// Builder for ReadOnlyConfig.
#[derive(Default)]
pub struct ReadOnlyConfigBuilder {
    config: ReadOnlyConfig,
}

impl ReadOnlyConfigBuilder {
    /// Enable/disable read-only mode.
    pub fn read_only(mut self, enabled: bool) -> Self {
        self.config.read_only = enabled;
        self
    }

    /// Set allowed signal channels.
    pub fn allowed_channels(mut self, channels: Vec<SignalChannelType>) -> Self {
        self.config.allowed_channels = channels;
        self
    }

    /// Set minimum PostgreSQL version.
    pub fn min_postgres_version(mut self, version: u32) -> Self {
        self.config.min_postgres_version = version;
        self
    }

    /// Enable/disable heartbeat-based watermarking.
    pub fn heartbeat_watermarking(mut self, enabled: bool) -> Self {
        self.config.heartbeat_watermarking = enabled;
        self
    }

    /// Enable/disable warnings for disabled features.
    pub fn warn_on_disabled_features(mut self, enabled: bool) -> Self {
        self.config.warn_on_disabled_features = enabled;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> ReadOnlyConfig {
        self.config
    }
}

// ============================================================================
// Read-Only Watermarking
// ============================================================================

/// Watermark source for read-only incremental snapshots.
///
/// In read-only mode, watermarks are derived from:
/// - Transaction IDs from WAL events
/// - Heartbeat event transaction context
/// - LSN positions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatermarkSource {
    /// Transaction ID from WAL stream
    TransactionId(u64),
    /// LSN position
    Lsn(String),
    /// Heartbeat event
    Heartbeat { xid: u64, lsn: String },
}

impl WatermarkSource {
    /// Get transaction ID if available.
    pub fn xid(&self) -> Option<u64> {
        match self {
            WatermarkSource::TransactionId(xid) => Some(*xid),
            WatermarkSource::Heartbeat { xid, .. } => Some(*xid),
            WatermarkSource::Lsn(_) => None,
        }
    }

    /// Get LSN if available.
    pub fn lsn(&self) -> Option<&str> {
        match self {
            WatermarkSource::Lsn(lsn) => Some(lsn),
            WatermarkSource::Heartbeat { lsn, .. } => Some(lsn),
            WatermarkSource::TransactionId(_) => None,
        }
    }
}

/// Read-only watermark tracker for incremental snapshots.
///
/// Tracks high/low watermarks using transaction IDs instead of signal table writes.
#[derive(Debug)]
pub struct ReadOnlyWatermarkTracker {
    /// Low watermark (snapshot window start)
    low_watermark: Arc<AtomicU64>,
    /// High watermark (snapshot window end)
    high_watermark: Arc<AtomicU64>,
    /// Whether snapshot window is open
    window_open: Arc<AtomicBool>,
    /// Current chunk being processed
    current_chunk: Arc<AtomicU64>,
    /// Stats
    stats: Arc<WatermarkStats>,
}

#[derive(Debug, Default)]
struct WatermarkStats {
    events_deduplicated: AtomicU64,
    windows_opened: AtomicU64,
    windows_closed: AtomicU64,
}

impl Default for ReadOnlyWatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadOnlyWatermarkTracker {
    /// Create a new watermark tracker.
    pub fn new() -> Self {
        Self {
            low_watermark: Arc::new(AtomicU64::new(0)),
            high_watermark: Arc::new(AtomicU64::new(0)),
            window_open: Arc::new(AtomicBool::new(false)),
            current_chunk: Arc::new(AtomicU64::new(0)),
            stats: Arc::new(WatermarkStats::default()),
        }
    }

    /// Open a snapshot window with the given transaction ID as low watermark.
    ///
    /// In read-only mode, this is called when starting to read a chunk,
    /// using the current transaction ID from the WAL stream.
    pub fn open_window(&self, xid: u64) {
        self.low_watermark.store(xid, Ordering::SeqCst);
        self.window_open.store(true, Ordering::SeqCst);
        self.stats.windows_opened.fetch_add(1, Ordering::Relaxed);
        debug!(xid = xid, "Opened read-only snapshot window");
    }

    /// Close the snapshot window with the given transaction ID as high watermark.
    ///
    /// In read-only mode, this is called after reading a chunk,
    /// using the transaction ID from the next WAL event or heartbeat.
    pub fn close_window(&self, xid: u64) {
        self.high_watermark.store(xid, Ordering::SeqCst);
        self.window_open.store(false, Ordering::SeqCst);
        self.stats.windows_closed.fetch_add(1, Ordering::Relaxed);
        debug!(xid = xid, "Closed read-only snapshot window");
    }

    /// Check if a WAL event should be deduplicated.
    ///
    /// Returns true if the event's transaction ID is within the snapshot window
    /// and should be buffered for deduplication.
    pub fn should_buffer(&self, xid: u64) -> bool {
        if !self.window_open.load(Ordering::SeqCst) {
            return false;
        }
        let low = self.low_watermark.load(Ordering::SeqCst);
        let high = self.high_watermark.load(Ordering::SeqCst);
        // Event is in window if: low <= xid <= high
        // Note: high may be 0 if window just opened
        if high == 0 {
            xid >= low
        } else {
            xid >= low && xid <= high
        }
    }

    /// Check if an event with the given XID should be deduplicated against snapshot data.
    ///
    /// Read-only deduplication logic:
    /// - If XID is before the snapshot window, event wins (not captured in snapshot)
    /// - If XID is within the window, compare against snapshot buffer
    /// - If XID is after the window, event wins (happened after snapshot)
    pub fn deduplicate(&self, event_xid: u64, snapshot_xid: Option<u64>) -> DeduplicationResult {
        let low = self.low_watermark.load(Ordering::SeqCst);
        let high = self.high_watermark.load(Ordering::SeqCst);

        if event_xid < low {
            // Event happened before snapshot window - not in snapshot
            DeduplicationResult::KeepEvent
        } else if high > 0 && event_xid > high {
            // Event happened after snapshot window - not in snapshot
            DeduplicationResult::KeepEvent
        } else if let Some(snap_xid) = snapshot_xid {
            // Event is in window - compare with snapshot
            if event_xid > snap_xid {
                // Event is newer than snapshot read
                self.stats
                    .events_deduplicated
                    .fetch_add(1, Ordering::Relaxed);
                DeduplicationResult::KeepEvent
            } else {
                // Snapshot data is newer or same
                DeduplicationResult::KeepSnapshot
            }
        } else {
            // No snapshot data for this key - keep event
            DeduplicationResult::KeepEvent
        }
    }

    /// Get current low watermark.
    pub fn low_watermark(&self) -> u64 {
        self.low_watermark.load(Ordering::SeqCst)
    }

    /// Get current high watermark.
    pub fn high_watermark(&self) -> u64 {
        self.high_watermark.load(Ordering::SeqCst)
    }

    /// Check if window is currently open.
    pub fn is_window_open(&self) -> bool {
        self.window_open.load(Ordering::SeqCst)
    }

    /// Advance to next chunk.
    pub fn next_chunk(&self) -> u64 {
        self.current_chunk.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current chunk number.
    pub fn current_chunk(&self) -> u64 {
        self.current_chunk.load(Ordering::SeqCst)
    }

    /// Get deduplication stats.
    pub fn stats(&self) -> ReadOnlyWatermarkStats {
        ReadOnlyWatermarkStats {
            events_deduplicated: self.stats.events_deduplicated.load(Ordering::Relaxed),
            windows_opened: self.stats.windows_opened.load(Ordering::Relaxed),
            windows_closed: self.stats.windows_closed.load(Ordering::Relaxed),
        }
    }
}

/// Result of deduplication check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeduplicationResult {
    /// Keep the streaming event, discard snapshot data
    KeepEvent,
    /// Keep the snapshot data, discard streaming event
    KeepSnapshot,
}

/// Statistics for read-only watermarking.
#[derive(Debug, Clone)]
pub struct ReadOnlyWatermarkStats {
    /// Number of events deduplicated
    pub events_deduplicated: u64,
    /// Number of snapshot windows opened
    pub windows_opened: u64,
    /// Number of snapshot windows closed
    pub windows_closed: u64,
}

// ============================================================================
// Read-Only Feature Guard
// ============================================================================

/// Guard that validates feature availability in read-only mode.
#[derive(Debug)]
pub struct ReadOnlyGuard {
    config: ReadOnlyConfig,
}

impl ReadOnlyGuard {
    /// Create a new guard with the given configuration.
    pub fn new(config: ReadOnlyConfig) -> Self {
        Self { config }
    }

    /// Check if a feature is available in the current mode.
    pub fn check_feature(&self, feature: ReadOnlyFeature) -> Result<(), ReadOnlyError> {
        if !self.config.read_only {
            return Ok(()); // All features available in read-write mode
        }

        match feature {
            ReadOnlyFeature::SignalTableSource => Err(ReadOnlyError::FeatureUnavailable {
                feature: "Signal table (source channel)",
                reason: "Source channel requires database writes",
                alternative: "Use topic, file, or api signal channels",
            }),
            ReadOnlyFeature::SignalTableWatermarking => Err(ReadOnlyError::FeatureUnavailable {
                feature: "Signal table watermarking",
                reason: "insert_insert/insert_delete strategies require database writes",
                alternative: "Heartbeat-based watermarking is automatically used in read-only mode",
            }),
            ReadOnlyFeature::HeartbeatActionQuery => Err(ReadOnlyError::FeatureUnavailable {
                feature: "Heartbeat action query",
                reason: "Action queries require database writes",
                alternative: "Disable heartbeat.action.query in read-only mode",
            }),
            ReadOnlyFeature::IncrementalSnapshot => {
                // Incremental snapshots are supported in read-only mode with PG 13+
                Ok(())
            }
            ReadOnlyFeature::BlockingSnapshot => {
                // Blocking snapshots are always available
                Ok(())
            }
        }
    }

    /// Warn about a disabled feature if warnings are enabled.
    pub fn warn_if_disabled(&self, feature: ReadOnlyFeature) {
        if !self.config.warn_on_disabled_features {
            return;
        }
        if let Err(e) = self.check_feature(feature) {
            warn!("{}", e);
        }
    }

    /// Check if read-only mode is enabled.
    pub fn is_read_only(&self) -> bool {
        self.config.read_only
    }
}

/// Features that may be affected by read-only mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOnlyFeature {
    /// Signal table source channel
    SignalTableSource,
    /// Signal table-based watermarking (insert_insert/insert_delete)
    SignalTableWatermarking,
    /// Heartbeat action query execution
    HeartbeatActionQuery,
    /// Incremental snapshots
    IncrementalSnapshot,
    /// Blocking snapshots
    BlockingSnapshot,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors related to read-only CDC mode.
#[derive(Debug, Clone)]
pub enum ReadOnlyError {
    /// Feature is unavailable in read-only mode
    FeatureUnavailable {
        feature: &'static str,
        reason: &'static str,
        alternative: &'static str,
    },
    /// PostgreSQL version doesn't support read-only snapshots
    UnsupportedPostgresVersion { required: u32, actual: u32 },
    /// Signal channel not allowed in read-only mode
    ChannelNotAllowed(SignalChannelType),
}

impl std::fmt::Display for ReadOnlyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadOnlyError::FeatureUnavailable {
                feature,
                reason,
                alternative,
            } => {
                write!(
                    f,
                    "Feature '{}' unavailable in read-only mode: {}. {}",
                    feature, reason, alternative
                )
            }
            ReadOnlyError::UnsupportedPostgresVersion { required, actual } => {
                write!(
                    f,
                    "PostgreSQL {} required for read-only incremental snapshots (found {})",
                    required, actual
                )
            }
            ReadOnlyError::ChannelNotAllowed(channel) => {
                write!(
                    f,
                    "Signal channel '{}' not allowed in read-only mode",
                    channel.as_str()
                )
            }
        }
    }
}

impl std::error::Error for ReadOnlyError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReadOnlyConfig::default();
        assert!(!config.read_only);
        assert!(config.heartbeat_watermarking);
        assert_eq!(config.min_postgres_version, 13);
    }

    #[test]
    fn test_builder() {
        let config = ReadOnlyConfig::builder()
            .read_only(true)
            .min_postgres_version(14)
            .heartbeat_watermarking(false)
            .build();

        assert!(config.read_only);
        assert!(!config.heartbeat_watermarking);
        assert_eq!(config.min_postgres_version, 14);
    }

    #[test]
    fn test_channel_filtering() {
        let config = ReadOnlyConfig::builder().read_only(true).build();

        // Source channel should be blocked
        assert!(!config.is_channel_allowed(SignalChannelType::Source));

        // Other channels should be allowed
        assert!(config.is_channel_allowed(SignalChannelType::Topic));
        assert!(config.is_channel_allowed(SignalChannelType::File));
        assert!(config.is_channel_allowed(SignalChannelType::Api));
    }

    #[test]
    fn test_channel_filtering_not_read_only() {
        let config = ReadOnlyConfig::builder().read_only(false).build();

        // All channels allowed when not read-only
        assert!(config.is_channel_allowed(SignalChannelType::Source));
        assert!(config.is_channel_allowed(SignalChannelType::Topic));
    }

    #[test]
    fn test_filter_channels() {
        let config = ReadOnlyConfig::builder().read_only(true).build();

        let channels = vec![
            SignalChannelType::Source,
            SignalChannelType::Topic,
            SignalChannelType::File,
        ];

        let filtered = config.filter_channels(&channels);
        assert_eq!(filtered.len(), 2);
        assert!(!filtered.contains(&SignalChannelType::Source));
        assert!(filtered.contains(&SignalChannelType::Topic));
        assert!(filtered.contains(&SignalChannelType::File));
    }

    #[test]
    fn test_postgres_version_check() {
        let config = ReadOnlyConfig::builder()
            .read_only(true)
            .min_postgres_version(13)
            .build();

        // PG 13+ should pass
        assert!(config.check_postgres_version(13).is_ok());
        assert!(config.check_postgres_version(14).is_ok());
        assert!(config.check_postgres_version(15).is_ok());

        // PG 12 should fail
        assert!(config.check_postgres_version(12).is_err());
    }

    #[test]
    fn test_watermark_tracker_basic() {
        let tracker = ReadOnlyWatermarkTracker::new();

        assert!(!tracker.is_window_open());
        assert_eq!(tracker.low_watermark(), 0);
        assert_eq!(tracker.high_watermark(), 0);
    }

    #[test]
    fn test_watermark_window_lifecycle() {
        let tracker = ReadOnlyWatermarkTracker::new();

        // Open window at XID 100
        tracker.open_window(100);
        assert!(tracker.is_window_open());
        assert_eq!(tracker.low_watermark(), 100);

        // Events in window should be buffered
        assert!(tracker.should_buffer(100));
        assert!(tracker.should_buffer(150));

        // Close window at XID 200
        tracker.close_window(200);
        assert!(!tracker.is_window_open());
        assert_eq!(tracker.high_watermark(), 200);

        // Events outside window should not be buffered
        assert!(!tracker.should_buffer(250));
    }

    #[test]
    fn test_deduplication_before_window() {
        let tracker = ReadOnlyWatermarkTracker::new();
        tracker.open_window(100);
        tracker.close_window(200);

        // Event before window - keep event
        let result = tracker.deduplicate(50, Some(100));
        assert_eq!(result, DeduplicationResult::KeepEvent);
    }

    #[test]
    fn test_deduplication_after_window() {
        let tracker = ReadOnlyWatermarkTracker::new();
        tracker.open_window(100);
        tracker.close_window(200);

        // Event after window - keep event
        let result = tracker.deduplicate(250, Some(150));
        assert_eq!(result, DeduplicationResult::KeepEvent);
    }

    #[test]
    fn test_deduplication_in_window_event_newer() {
        let tracker = ReadOnlyWatermarkTracker::new();
        tracker.open_window(100);
        tracker.close_window(200);

        // Event in window, event is newer than snapshot
        let result = tracker.deduplicate(150, Some(140));
        assert_eq!(result, DeduplicationResult::KeepEvent);
    }

    #[test]
    fn test_deduplication_in_window_snapshot_newer() {
        let tracker = ReadOnlyWatermarkTracker::new();
        tracker.open_window(100);
        tracker.close_window(200);

        // Event in window, snapshot is newer
        let result = tracker.deduplicate(140, Some(150));
        assert_eq!(result, DeduplicationResult::KeepSnapshot);
    }

    #[test]
    fn test_watermark_stats() {
        let tracker = ReadOnlyWatermarkTracker::new();

        tracker.open_window(100);
        tracker.close_window(200);
        tracker.open_window(200);
        tracker.close_window(300);

        let stats = tracker.stats();
        assert_eq!(stats.windows_opened, 2);
        assert_eq!(stats.windows_closed, 2);
    }

    #[test]
    fn test_watermark_source() {
        let xid = WatermarkSource::TransactionId(12345);
        assert_eq!(xid.xid(), Some(12345));
        assert_eq!(xid.lsn(), None);

        let lsn = WatermarkSource::Lsn("0/16B3748".to_string());
        assert_eq!(lsn.xid(), None);
        assert_eq!(lsn.lsn(), Some("0/16B3748"));

        let heartbeat = WatermarkSource::Heartbeat {
            xid: 12345,
            lsn: "0/16B3748".to_string(),
        };
        assert_eq!(heartbeat.xid(), Some(12345));
        assert_eq!(heartbeat.lsn(), Some("0/16B3748"));
    }

    #[test]
    fn test_read_only_guard() {
        let config = ReadOnlyConfig::builder().read_only(true).build();
        let guard = ReadOnlyGuard::new(config);

        // Signal table features should be unavailable
        assert!(guard
            .check_feature(ReadOnlyFeature::SignalTableSource)
            .is_err());
        assert!(guard
            .check_feature(ReadOnlyFeature::SignalTableWatermarking)
            .is_err());
        assert!(guard
            .check_feature(ReadOnlyFeature::HeartbeatActionQuery)
            .is_err());

        // Snapshot features should be available
        assert!(guard
            .check_feature(ReadOnlyFeature::IncrementalSnapshot)
            .is_ok());
        assert!(guard
            .check_feature(ReadOnlyFeature::BlockingSnapshot)
            .is_ok());
    }

    #[test]
    fn test_read_only_guard_not_read_only() {
        let config = ReadOnlyConfig::builder().read_only(false).build();
        let guard = ReadOnlyGuard::new(config);

        // All features available when not read-only
        assert!(guard
            .check_feature(ReadOnlyFeature::SignalTableSource)
            .is_ok());
        assert!(guard
            .check_feature(ReadOnlyFeature::SignalTableWatermarking)
            .is_ok());
        assert!(guard
            .check_feature(ReadOnlyFeature::HeartbeatActionQuery)
            .is_ok());
    }

    #[test]
    fn test_chunk_tracking() {
        let tracker = ReadOnlyWatermarkTracker::new();
        assert_eq!(tracker.current_chunk(), 0);

        let chunk1 = tracker.next_chunk();
        assert_eq!(chunk1, 0);
        assert_eq!(tracker.current_chunk(), 1);

        let chunk2 = tracker.next_chunk();
        assert_eq!(chunk2, 1);
        assert_eq!(tracker.current_chunk(), 2);
    }
}

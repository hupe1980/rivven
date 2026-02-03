//! # CDC Notifications
//!
//! Notification system for CDC progress and status updates.
//!
//! ## Features
//!
//! - **Snapshot Progress**: Track initial and incremental snapshot progress
//! - **Streaming Status**: Monitor streaming health and lag
//! - **Error Notifications**: Immediate alerts on failures
//! - **Multiple Channels**: Sink, log, metrics, webhook support
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::notification::{Notifier, Notification, NotificationType};
//!
//! let notifier = Notifier::new();
//!
//! // Subscribe to notifications
//! let mut rx = notifier.subscribe();
//!
//! // Send a notification
//! notifier.notify(Notification::snapshot_started("connector-1", 10));
//!
//! // Receive notifications
//! while let Some(notif) = rx.recv().await {
//!     println!("Received: {:?}", notif);
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Notification types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NotificationType {
    /// Initial snapshot started
    InitialSnapshotStarted,
    /// Initial snapshot in progress
    InitialSnapshotInProgress,
    /// Initial snapshot table completed
    InitialSnapshotTableCompleted,
    /// Initial snapshot completed
    InitialSnapshotCompleted,
    /// Incremental snapshot started
    IncrementalSnapshotStarted,
    /// Incremental snapshot paused
    IncrementalSnapshotPaused,
    /// Incremental snapshot resumed
    IncrementalSnapshotResumed,
    /// Incremental snapshot in progress
    IncrementalSnapshotInProgress,
    /// Incremental snapshot completed
    IncrementalSnapshotCompleted,
    /// Streaming started
    StreamingStarted,
    /// Streaming paused
    StreamingPaused,
    /// Streaming resumed
    StreamingResumed,
    /// Streaming stopped
    StreamingStopped,
    /// Connection established
    Connected,
    /// Connection lost
    Disconnected,
    /// Error occurred
    Error,
    /// Warning
    Warning,
    /// Connector status change
    StatusChange,
    /// Custom notification type
    Custom(String),
}

impl NotificationType {
    /// Check if this is a snapshot notification.
    pub fn is_snapshot(&self) -> bool {
        matches!(
            self,
            NotificationType::InitialSnapshotStarted
                | NotificationType::InitialSnapshotInProgress
                | NotificationType::InitialSnapshotTableCompleted
                | NotificationType::InitialSnapshotCompleted
                | NotificationType::IncrementalSnapshotStarted
                | NotificationType::IncrementalSnapshotPaused
                | NotificationType::IncrementalSnapshotResumed
                | NotificationType::IncrementalSnapshotInProgress
                | NotificationType::IncrementalSnapshotCompleted
        )
    }

    /// Check if this is a streaming notification.
    pub fn is_streaming(&self) -> bool {
        matches!(
            self,
            NotificationType::StreamingStarted
                | NotificationType::StreamingPaused
                | NotificationType::StreamingResumed
                | NotificationType::StreamingStopped
        )
    }

    /// Check if this is an error or warning.
    pub fn is_alert(&self) -> bool {
        matches!(
            self,
            NotificationType::Error | NotificationType::Warning | NotificationType::Disconnected
        )
    }
}

/// Notification severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum Severity {
    /// Informational notification
    #[default]
    Info,
    /// Warning - may require attention
    Warning,
    /// Error - requires immediate attention
    Error,
    /// Critical - system failure
    Critical,
}

/// Additional data for notifications.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationData {
    /// Total items to process (for progress)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_count: Option<u64>,
    /// Items processed so far
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_count: Option<u64>,
    /// Current table/collection being processed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table: Option<String>,
    /// Error message if applicable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Current position/offset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
    /// Lag in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_ms: Option<i64>,
    /// Additional properties
    #[serde(default, flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

impl NotificationData {
    /// Create empty data.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create data with progress info.
    pub fn with_progress(total: u64, completed: u64) -> Self {
        Self {
            total_count: Some(total),
            completed_count: Some(completed),
            ..Default::default()
        }
    }

    /// Create data with table info.
    pub fn with_table(table: &str) -> Self {
        Self {
            current_table: Some(table.to_string()),
            ..Default::default()
        }
    }

    /// Create data with error.
    pub fn with_error(error: &str) -> Self {
        Self {
            error: Some(error.to_string()),
            ..Default::default()
        }
    }

    /// Set current table.
    pub fn table(mut self, table: &str) -> Self {
        self.current_table = Some(table.to_string());
        self
    }

    /// Set position.
    pub fn position(mut self, position: &str) -> Self {
        self.position = Some(position.to_string());
        self
    }

    /// Set lag.
    pub fn lag(mut self, lag_ms: i64) -> Self {
        self.lag_ms = Some(lag_ms);
        self
    }

    /// Add a property.
    pub fn property(mut self, key: &str, value: serde_json::Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self
    }

    /// Get progress as percentage.
    pub fn progress_percent(&self) -> Option<f64> {
        match (self.total_count, self.completed_count) {
            (Some(total), Some(completed)) if total > 0 => {
                Some((completed as f64 / total as f64) * 100.0)
            }
            _ => None,
        }
    }
}

/// A CDC notification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    /// Unique notification ID
    pub id: String,
    /// Notification type
    #[serde(rename = "type")]
    pub notification_type: NotificationType,
    /// Connector name
    pub connector: String,
    /// Severity level
    pub severity: Severity,
    /// Human-readable message
    pub message: String,
    /// Timestamp (epoch millis)
    pub timestamp: i64,
    /// Additional data
    #[serde(default)]
    pub data: NotificationData,
}

impl Notification {
    /// Create a new notification.
    pub fn new(notification_type: NotificationType, connector: &str, message: &str) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            notification_type,
            connector: connector.to_string(),
            severity: Severity::Info,
            message: message.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            data: NotificationData::empty(),
        }
    }

    /// Create initial snapshot started notification.
    pub fn snapshot_started(connector: &str, total_tables: u64) -> Self {
        Self::new(
            NotificationType::InitialSnapshotStarted,
            connector,
            &format!("Initial snapshot started for {} tables", total_tables),
        )
        .with_data(NotificationData::with_progress(total_tables, 0))
    }

    /// Create initial snapshot progress notification.
    pub fn snapshot_progress(connector: &str, table: &str, completed: u64, total: u64) -> Self {
        Self::new(
            NotificationType::InitialSnapshotInProgress,
            connector,
            &format!("Snapshot in progress: {}/{} tables", completed, total),
        )
        .with_data(NotificationData::with_progress(total, completed).table(table))
    }

    /// Create table completed notification.
    pub fn table_completed(
        connector: &str,
        table: &str,
        rows: u64,
        completed: u64,
        total: u64,
    ) -> Self {
        Self::new(
            NotificationType::InitialSnapshotTableCompleted,
            connector,
            &format!("Completed snapshot of {} ({} rows)", table, rows),
        )
        .with_data(
            NotificationData::with_progress(total, completed)
                .table(table)
                .property("rows", serde_json::json!(rows)),
        )
    }

    /// Create initial snapshot completed notification.
    pub fn snapshot_completed(connector: &str, total_rows: u64) -> Self {
        Self::new(
            NotificationType::InitialSnapshotCompleted,
            connector,
            &format!("Initial snapshot completed ({} total rows)", total_rows),
        )
        .with_data(NotificationData::empty().property("total_rows", serde_json::json!(total_rows)))
    }

    /// Create incremental snapshot started notification.
    pub fn incremental_snapshot_started(connector: &str, tables: &[String]) -> Self {
        Self::new(
            NotificationType::IncrementalSnapshotStarted,
            connector,
            &format!("Incremental snapshot started for {} tables", tables.len()),
        )
        .with_data(
            NotificationData::with_progress(tables.len() as u64, 0)
                .property("tables", serde_json::json!(tables)),
        )
    }

    /// Create incremental snapshot paused notification.
    pub fn incremental_snapshot_paused(connector: &str) -> Self {
        Self::new(
            NotificationType::IncrementalSnapshotPaused,
            connector,
            "Incremental snapshot paused",
        )
    }

    /// Create incremental snapshot resumed notification.
    pub fn incremental_snapshot_resumed(connector: &str) -> Self {
        Self::new(
            NotificationType::IncrementalSnapshotResumed,
            connector,
            "Incremental snapshot resumed",
        )
    }

    /// Create incremental snapshot progress notification.
    pub fn incremental_snapshot_progress(
        connector: &str,
        table: &str,
        chunk: u64,
        total_chunks: u64,
    ) -> Self {
        let percent = if total_chunks > 0 {
            (chunk as f64 / total_chunks as f64 * 100.0) as u64
        } else {
            0
        };
        Self::new(
            NotificationType::IncrementalSnapshotInProgress,
            connector,
            &format!(
                "Incremental snapshot: {} chunk {}/{} ({}%)",
                table, chunk, total_chunks, percent
            ),
        )
        .with_data(NotificationData::with_progress(total_chunks, chunk).table(table))
    }

    /// Create incremental snapshot completed notification.
    pub fn incremental_snapshot_completed(connector: &str, tables: &[String]) -> Self {
        Self::new(
            NotificationType::IncrementalSnapshotCompleted,
            connector,
            &format!("Incremental snapshot completed for {} tables", tables.len()),
        )
        .with_data(NotificationData::empty().property("tables", serde_json::json!(tables)))
    }

    /// Create streaming started notification.
    pub fn streaming_started(connector: &str, position: &str) -> Self {
        Self::new(
            NotificationType::StreamingStarted,
            connector,
            &format!("Streaming started at position {}", position),
        )
        .with_data(NotificationData::empty().position(position))
    }

    /// Create streaming paused notification.
    pub fn streaming_paused(connector: &str) -> Self {
        Self::new(
            NotificationType::StreamingPaused,
            connector,
            "Streaming paused",
        )
    }

    /// Create streaming resumed notification.
    pub fn streaming_resumed(connector: &str) -> Self {
        Self::new(
            NotificationType::StreamingResumed,
            connector,
            "Streaming resumed",
        )
    }

    /// Create streaming stopped notification.
    pub fn streaming_stopped(connector: &str) -> Self {
        Self::new(
            NotificationType::StreamingStopped,
            connector,
            "Streaming stopped",
        )
    }

    /// Create connected notification.
    pub fn connected(connector: &str, server: &str) -> Self {
        Self::new(
            NotificationType::Connected,
            connector,
            &format!("Connected to {}", server),
        )
    }

    /// Create disconnected notification.
    pub fn disconnected(connector: &str, reason: &str) -> Self {
        Self::new(
            NotificationType::Disconnected,
            connector,
            &format!("Disconnected: {}", reason),
        )
        .with_severity(Severity::Warning)
    }

    /// Create error notification.
    pub fn error(connector: &str, error: &str) -> Self {
        Self::new(NotificationType::Error, connector, error)
            .with_severity(Severity::Error)
            .with_data(NotificationData::with_error(error))
    }

    /// Create warning notification.
    pub fn warning(connector: &str, message: &str) -> Self {
        Self::new(NotificationType::Warning, connector, message).with_severity(Severity::Warning)
    }

    /// Set severity.
    pub fn with_severity(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }

    /// Set data.
    pub fn with_data(mut self, data: NotificationData) -> Self {
        self.data = data;
        self
    }

    /// Add data to existing data.
    pub fn add_data(mut self, key: &str, value: serde_json::Value) -> Self {
        self.data.properties.insert(key.to_string(), value);
        self
    }
}

/// Notification statistics.
#[derive(Debug, Default)]
pub struct NotificationStats {
    /// Total notifications sent
    total_sent: AtomicU64,
    /// Snapshot notifications
    snapshot_notifications: AtomicU64,
    /// Streaming notifications
    streaming_notifications: AtomicU64,
    /// Error notifications
    error_notifications: AtomicU64,
    /// Dropped notifications (no subscribers)
    dropped: AtomicU64,
}

impl NotificationStats {
    /// Record a notification.
    pub fn record(&self, notif: &Notification, dropped: bool) {
        self.total_sent.fetch_add(1, Ordering::Relaxed);

        if notif.notification_type.is_snapshot() {
            self.snapshot_notifications.fetch_add(1, Ordering::Relaxed);
        }
        if notif.notification_type.is_streaming() {
            self.streaming_notifications.fetch_add(1, Ordering::Relaxed);
        }
        if notif.notification_type.is_alert() {
            self.error_notifications.fetch_add(1, Ordering::Relaxed);
        }
        if dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get total sent.
    pub fn total_sent(&self) -> u64 {
        self.total_sent.load(Ordering::Relaxed)
    }

    /// Get snapshot notifications count.
    pub fn snapshot_notifications(&self) -> u64 {
        self.snapshot_notifications.load(Ordering::Relaxed)
    }

    /// Get error notifications count.
    pub fn error_notifications(&self) -> u64 {
        self.error_notifications.load(Ordering::Relaxed)
    }

    /// Get dropped count.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Notification channel for CDC.
pub struct Notifier {
    /// Broadcast sender
    sender: broadcast::Sender<Notification>,
    /// Statistics
    stats: Arc<NotificationStats>,
    /// Connector name
    connector: String,
}

impl Notifier {
    /// Create a new notifier.
    pub fn new(connector: &str) -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            sender,
            stats: Arc::new(NotificationStats::default()),
            connector: connector.to_string(),
        }
    }

    /// Create a new notifier with custom buffer size.
    pub fn with_capacity(connector: &str, capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            stats: Arc::new(NotificationStats::default()),
            connector: connector.to_string(),
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> &Arc<NotificationStats> {
        &self.stats
    }

    /// Subscribe to notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<Notification> {
        self.sender.subscribe()
    }

    /// Get number of subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Send a notification.
    pub fn notify(&self, notification: Notification) {
        let dropped = self.sender.send(notification.clone()).is_err();
        self.stats.record(&notification, dropped);

        // Also log based on severity
        match notification.severity {
            Severity::Error | Severity::Critical => {
                warn!(
                    "CDC notification [{}]: {}",
                    notification.connector, notification.message
                );
            }
            Severity::Warning => {
                warn!(
                    "CDC notification [{}]: {}",
                    notification.connector, notification.message
                );
            }
            Severity::Info => {
                info!(
                    "CDC notification [{}]: {}",
                    notification.connector, notification.message
                );
            }
        }

        if dropped {
            debug!("Notification dropped (no subscribers): {}", notification.id);
        }
    }

    /// Send snapshot started notification.
    pub fn snapshot_started(&self, total_tables: u64) {
        self.notify(Notification::snapshot_started(
            &self.connector,
            total_tables,
        ));
    }

    /// Send snapshot progress notification.
    pub fn snapshot_progress(&self, table: &str, completed: u64, total: u64) {
        self.notify(Notification::snapshot_progress(
            &self.connector,
            table,
            completed,
            total,
        ));
    }

    /// Send table completed notification.
    pub fn table_completed(&self, table: &str, rows: u64, completed: u64, total: u64) {
        self.notify(Notification::table_completed(
            &self.connector,
            table,
            rows,
            completed,
            total,
        ));
    }

    /// Send snapshot completed notification.
    pub fn snapshot_completed(&self, total_rows: u64) {
        self.notify(Notification::snapshot_completed(
            &self.connector,
            total_rows,
        ));
    }

    /// Send streaming started notification.
    pub fn streaming_started(&self, position: &str) {
        self.notify(Notification::streaming_started(&self.connector, position));
    }

    /// Send error notification.
    pub fn error(&self, error: &str) {
        self.notify(Notification::error(&self.connector, error));
    }

    /// Send warning notification.
    pub fn warning(&self, message: &str) {
        self.notify(Notification::warning(&self.connector, message));
    }
}

impl Clone for Notifier {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            stats: self.stats.clone(),
            connector: self.connector.clone(),
        }
    }
}

/// Notification sink for receiving and processing notifications.
pub trait NotificationSink: Send + Sync {
    /// Process a notification.
    fn process(&self, notification: &Notification) -> impl std::future::Future<Output = ()> + Send;
}

/// Log sink - logs notifications at appropriate levels.
pub struct LogSink;

impl NotificationSink for LogSink {
    async fn process(&self, notification: &Notification) {
        match notification.severity {
            Severity::Critical | Severity::Error => {
                tracing::error!(
                    notification_id = %notification.id,
                    connector = %notification.connector,
                    notification_type = ?notification.notification_type,
                    "{}",
                    notification.message
                );
            }
            Severity::Warning => {
                tracing::warn!(
                    notification_id = %notification.id,
                    connector = %notification.connector,
                    notification_type = ?notification.notification_type,
                    "{}",
                    notification.message
                );
            }
            Severity::Info => {
                tracing::info!(
                    notification_id = %notification.id,
                    connector = %notification.connector,
                    notification_type = ?notification.notification_type,
                    "{}",
                    notification.message
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_type_is_snapshot() {
        assert!(NotificationType::InitialSnapshotStarted.is_snapshot());
        assert!(NotificationType::InitialSnapshotInProgress.is_snapshot());
        assert!(NotificationType::InitialSnapshotCompleted.is_snapshot());
        assert!(NotificationType::IncrementalSnapshotStarted.is_snapshot());
        assert!(!NotificationType::StreamingStarted.is_snapshot());
        assert!(!NotificationType::Error.is_snapshot());
    }

    #[test]
    fn test_notification_type_is_streaming() {
        assert!(NotificationType::StreamingStarted.is_streaming());
        assert!(NotificationType::StreamingPaused.is_streaming());
        assert!(NotificationType::StreamingResumed.is_streaming());
        assert!(NotificationType::StreamingStopped.is_streaming());
        assert!(!NotificationType::InitialSnapshotStarted.is_streaming());
    }

    #[test]
    fn test_notification_type_is_alert() {
        assert!(NotificationType::Error.is_alert());
        assert!(NotificationType::Warning.is_alert());
        assert!(NotificationType::Disconnected.is_alert());
        assert!(!NotificationType::Connected.is_alert());
        assert!(!NotificationType::StreamingStarted.is_alert());
    }

    #[test]
    fn test_notification_data_empty() {
        let data = NotificationData::empty();
        assert!(data.total_count.is_none());
        assert!(data.completed_count.is_none());
        assert!(data.progress_percent().is_none());
    }

    #[test]
    fn test_notification_data_with_progress() {
        let data = NotificationData::with_progress(100, 50);
        assert_eq!(data.total_count, Some(100));
        assert_eq!(data.completed_count, Some(50));
        assert_eq!(data.progress_percent(), Some(50.0));
    }

    #[test]
    fn test_notification_data_progress_zero_total() {
        let data = NotificationData::with_progress(0, 0);
        assert_eq!(data.progress_percent(), None);
    }

    #[test]
    fn test_notification_data_builder() {
        let data = NotificationData::empty()
            .table("public.users")
            .position("0/16B3748")
            .lag(1000)
            .property("custom", serde_json::json!("value"));

        assert_eq!(data.current_table, Some("public.users".to_string()));
        assert_eq!(data.position, Some("0/16B3748".to_string()));
        assert_eq!(data.lag_ms, Some(1000));
        assert!(data.properties.contains_key("custom"));
    }

    #[test]
    fn test_notification_new() {
        let notif = Notification::new(
            NotificationType::StreamingStarted,
            "my-connector",
            "Streaming has started",
        );

        assert_eq!(notif.notification_type, NotificationType::StreamingStarted);
        assert_eq!(notif.connector, "my-connector");
        assert_eq!(notif.message, "Streaming has started");
        assert_eq!(notif.severity, Severity::Info);
        assert!(notif.timestamp > 0);
    }

    #[test]
    fn test_notification_snapshot_started() {
        let notif = Notification::snapshot_started("connector-1", 10);

        assert_eq!(
            notif.notification_type,
            NotificationType::InitialSnapshotStarted
        );
        assert_eq!(notif.data.total_count, Some(10));
        assert_eq!(notif.data.completed_count, Some(0));
    }

    #[test]
    fn test_notification_snapshot_progress() {
        let notif = Notification::snapshot_progress("connector-1", "public.users", 3, 10);

        assert_eq!(
            notif.notification_type,
            NotificationType::InitialSnapshotInProgress
        );
        assert_eq!(notif.data.current_table, Some("public.users".to_string()));
        assert_eq!(notif.data.progress_percent(), Some(30.0));
    }

    #[test]
    fn test_notification_table_completed() {
        let notif = Notification::table_completed("connector-1", "public.orders", 1000, 5, 10);

        assert_eq!(
            notif.notification_type,
            NotificationType::InitialSnapshotTableCompleted
        );
        assert!(notif.message.contains("1000 rows"));
    }

    #[test]
    fn test_notification_snapshot_completed() {
        let notif = Notification::snapshot_completed("connector-1", 50000);

        assert_eq!(
            notif.notification_type,
            NotificationType::InitialSnapshotCompleted
        );
        assert!(notif.message.contains("50000"));
    }

    #[test]
    fn test_notification_incremental_snapshot() {
        let tables = vec!["public.users".to_string(), "public.orders".to_string()];

        let started = Notification::incremental_snapshot_started("conn", &tables);
        assert_eq!(
            started.notification_type,
            NotificationType::IncrementalSnapshotStarted
        );

        let paused = Notification::incremental_snapshot_paused("conn");
        assert_eq!(
            paused.notification_type,
            NotificationType::IncrementalSnapshotPaused
        );

        let resumed = Notification::incremental_snapshot_resumed("conn");
        assert_eq!(
            resumed.notification_type,
            NotificationType::IncrementalSnapshotResumed
        );

        let progress = Notification::incremental_snapshot_progress("conn", "public.users", 5, 10);
        assert_eq!(
            progress.notification_type,
            NotificationType::IncrementalSnapshotInProgress
        );
        assert!(progress.message.contains("50%"));

        let completed = Notification::incremental_snapshot_completed("conn", &tables);
        assert_eq!(
            completed.notification_type,
            NotificationType::IncrementalSnapshotCompleted
        );
    }

    #[test]
    fn test_notification_streaming() {
        let started = Notification::streaming_started("conn", "0/16B3748");
        assert_eq!(
            started.notification_type,
            NotificationType::StreamingStarted
        );
        assert_eq!(started.data.position, Some("0/16B3748".to_string()));

        let paused = Notification::streaming_paused("conn");
        assert_eq!(paused.notification_type, NotificationType::StreamingPaused);

        let resumed = Notification::streaming_resumed("conn");
        assert_eq!(
            resumed.notification_type,
            NotificationType::StreamingResumed
        );

        let stopped = Notification::streaming_stopped("conn");
        assert_eq!(
            stopped.notification_type,
            NotificationType::StreamingStopped
        );
    }

    #[test]
    fn test_notification_connected() {
        let notif = Notification::connected("conn", "pg-server:5432");
        assert_eq!(notif.notification_type, NotificationType::Connected);
        assert!(notif.message.contains("pg-server:5432"));
    }

    #[test]
    fn test_notification_disconnected() {
        let notif = Notification::disconnected("conn", "connection timeout");
        assert_eq!(notif.notification_type, NotificationType::Disconnected);
        assert_eq!(notif.severity, Severity::Warning);
    }

    #[test]
    fn test_notification_error() {
        let notif = Notification::error("conn", "Database connection failed");
        assert_eq!(notif.notification_type, NotificationType::Error);
        assert_eq!(notif.severity, Severity::Error);
        assert_eq!(
            notif.data.error,
            Some("Database connection failed".to_string())
        );
    }

    #[test]
    fn test_notification_warning() {
        let notif = Notification::warning("conn", "High replication lag");
        assert_eq!(notif.notification_type, NotificationType::Warning);
        assert_eq!(notif.severity, Severity::Warning);
    }

    #[test]
    fn test_notification_with_severity() {
        let notif = Notification::new(NotificationType::StatusChange, "conn", "Status changed")
            .with_severity(Severity::Critical);

        assert_eq!(notif.severity, Severity::Critical);
    }

    #[test]
    fn test_notification_add_data() {
        let notif = Notification::new(NotificationType::StatusChange, "conn", "Test")
            .add_data("key1", serde_json::json!("value1"))
            .add_data("key2", serde_json::json!(42));

        assert!(notif.data.properties.contains_key("key1"));
        assert!(notif.data.properties.contains_key("key2"));
    }

    #[test]
    fn test_notification_stats() {
        let stats = NotificationStats::default();

        let snapshot_notif = Notification::snapshot_started("conn", 10);
        stats.record(&snapshot_notif, false);

        let error_notif = Notification::error("conn", "error");
        stats.record(&error_notif, true);

        assert_eq!(stats.total_sent(), 2);
        assert_eq!(stats.snapshot_notifications(), 1);
        assert_eq!(stats.error_notifications(), 1);
        assert_eq!(stats.dropped(), 1);
    }

    #[test]
    fn test_notifier_new() {
        let notifier = Notifier::new("test-connector");
        assert_eq!(notifier.subscriber_count(), 0);
    }

    #[test]
    fn test_notifier_subscribe() {
        let notifier = Notifier::new("test-connector");
        let _rx1 = notifier.subscribe();
        let _rx2 = notifier.subscribe();
        assert_eq!(notifier.subscriber_count(), 2);
    }

    #[tokio::test]
    async fn test_notifier_notify() {
        let notifier = Notifier::new("test-connector");
        let mut rx = notifier.subscribe();

        notifier.notify(Notification::streaming_started("test-connector", "0/0"));

        let notif = rx.recv().await.unwrap();
        assert_eq!(notif.notification_type, NotificationType::StreamingStarted);
    }

    #[tokio::test]
    async fn test_notifier_helper_methods() {
        let notifier = Notifier::new("test-connector");
        let mut rx = notifier.subscribe();

        notifier.snapshot_started(5);
        let n1 = rx.recv().await.unwrap();
        assert_eq!(
            n1.notification_type,
            NotificationType::InitialSnapshotStarted
        );

        notifier.snapshot_progress("table1", 1, 5);
        let n2 = rx.recv().await.unwrap();
        assert_eq!(
            n2.notification_type,
            NotificationType::InitialSnapshotInProgress
        );

        notifier.table_completed("table1", 100, 1, 5);
        let n3 = rx.recv().await.unwrap();
        assert_eq!(
            n3.notification_type,
            NotificationType::InitialSnapshotTableCompleted
        );

        notifier.snapshot_completed(500);
        let n4 = rx.recv().await.unwrap();
        assert_eq!(
            n4.notification_type,
            NotificationType::InitialSnapshotCompleted
        );
    }

    #[tokio::test]
    async fn test_notifier_error_warning() {
        let notifier = Notifier::new("test-connector");
        let mut rx = notifier.subscribe();

        notifier.error("Something went wrong");
        let err = rx.recv().await.unwrap();
        assert_eq!(err.severity, Severity::Error);

        notifier.warning("Caution advised");
        let warn = rx.recv().await.unwrap();
        assert_eq!(warn.severity, Severity::Warning);
    }

    #[test]
    fn test_notifier_clone() {
        let notifier1 = Notifier::new("conn");
        let notifier2 = notifier1.clone();

        // Both share the same stats
        notifier1.notify(Notification::streaming_started("conn", "0/0"));
        assert_eq!(notifier1.stats().total_sent(), 1);
        assert_eq!(notifier2.stats().total_sent(), 1);
    }

    #[test]
    fn test_notification_serialization() {
        let notif = Notification::snapshot_progress("conn", "public.users", 5, 10);
        let json = serde_json::to_string(&notif).unwrap();

        assert!(json.contains("INITIAL_SNAPSHOT_IN_PROGRESS"));
        assert!(json.contains("public.users"));

        let parsed: Notification = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.notification_type,
            NotificationType::InitialSnapshotInProgress
        );
    }

    #[test]
    fn test_custom_notification_type() {
        let notif = Notification::new(
            NotificationType::Custom("my-custom-type".to_string()),
            "conn",
            "Custom notification",
        );

        assert_eq!(
            notif.notification_type,
            NotificationType::Custom("my-custom-type".to_string())
        );
    }

    #[tokio::test]
    async fn test_log_sink() {
        let sink = LogSink;

        // These should not panic
        sink.process(&Notification::error("conn", "Error message"))
            .await;
        sink.process(&Notification::warning("conn", "Warning"))
            .await;
        sink.process(&Notification::streaming_started("conn", "0/0"))
            .await;
    }
}

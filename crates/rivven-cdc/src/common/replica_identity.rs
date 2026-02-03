//! # REPLICA IDENTITY Enforcement
//!
//! PostgreSQL REPLICA IDENTITY configuration detection and enforcement for CDC.
//!
//! ## Overview
//!
//! PostgreSQL tables have a REPLICA IDENTITY setting that determines what data
//! is included in the WAL for UPDATE and DELETE operations:
//!
//! - **DEFAULT**: Only primary key columns in before image
//! - **NOTHING**: No before image (DELETE/UPDATE won't have old row data)
//! - **FULL**: All columns in before image (required for proper CDC)
//! - **INDEX**: Columns from specified unique index
//!
//! ## Why REPLICA IDENTITY FULL Matters
//!
//! Without REPLICA IDENTITY FULL:
//! - UPDATE events may lack "before" image (only key columns)
//! - DELETE events may lack full row data for downstream processing
//! - Audit logs and compliance requirements may not be met
//! - Log compaction keys may be incomplete
//!
//! ## Enforcement
//!
//! This module provides enforcement with configurable behavior:
//!
//! - **Warn**: Log warning but continue (default)
//! - **Skip**: Skip events from tables without FULL
//! - **Fail**: Fail the connector if any table lacks FULL
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::replica_identity::{
//!     ReplicaIdentity, ReplicaIdentityConfig, ReplicaIdentityEnforcer
//! };
//!
//! let config = ReplicaIdentityConfig::builder()
//!     .require_full(true)
//!     .enforcement_mode(EnforcementMode::Warn)
//!     .exclude_tables(vec!["audit.*".to_string()])
//!     .build();
//!
//! let enforcer = ReplicaIdentityEnforcer::new(config);
//!
//! // Check a table's replica identity
//! let identity = ReplicaIdentity::Full;
//! let result = enforcer.check("public", "users", identity);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// PostgreSQL REPLICA IDENTITY setting.
///
/// Determines what columns are included in the WAL for UPDATE/DELETE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicaIdentity {
    /// DEFAULT - Only primary key columns (most common)
    Default,
    /// NOTHING - No old row data at all
    Nothing,
    /// FULL - All columns included (recommended for CDC)
    Full,
    /// INDEX - Columns from a specified unique index
    Index,
    /// Unknown identity value
    Unknown(u8),
}

impl ReplicaIdentity {
    /// Parse from PostgreSQL protocol byte value.
    ///
    /// PostgreSQL uses these character codes in the RELATION message:
    /// - 'd' (100) = DEFAULT
    /// - 'n' (110) = NOTHING
    /// - 'f' (102) = FULL
    /// - 'i' (105) = INDEX
    pub fn from_byte(b: u8) -> Self {
        match b {
            b'd' => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::Nothing,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            _ => ReplicaIdentity::Unknown(b),
        }
    }

    /// Convert to PostgreSQL protocol byte value.
    pub fn to_byte(self) -> u8 {
        match self {
            ReplicaIdentity::Default => b'd',
            ReplicaIdentity::Nothing => b'n',
            ReplicaIdentity::Full => b'f',
            ReplicaIdentity::Index => b'i',
            ReplicaIdentity::Unknown(b) => b,
        }
    }

    /// Check if this identity provides full row data.
    pub fn is_full(&self) -> bool {
        matches!(self, ReplicaIdentity::Full)
    }

    /// Check if this identity provides any before image.
    pub fn has_before_image(&self) -> bool {
        !matches!(self, ReplicaIdentity::Nothing)
    }

    /// Get human-readable name for the identity.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicaIdentity::Default => "DEFAULT",
            ReplicaIdentity::Nothing => "NOTHING",
            ReplicaIdentity::Full => "FULL",
            ReplicaIdentity::Index => "INDEX",
            ReplicaIdentity::Unknown(_) => "UNKNOWN",
        }
    }

    /// Get the SQL command to change a table's replica identity.
    pub fn alter_table_sql(schema: &str, table: &str, identity: ReplicaIdentity) -> String {
        let identity_str = match identity {
            ReplicaIdentity::Default => "DEFAULT",
            ReplicaIdentity::Nothing => "NOTHING",
            ReplicaIdentity::Full => "FULL",
            ReplicaIdentity::Index => "USING INDEX index_name", // placeholder
            ReplicaIdentity::Unknown(_) => "DEFAULT",
        };
        format!(
            "ALTER TABLE \"{}\".\"{}\" REPLICA IDENTITY {}",
            schema, table, identity_str
        )
    }
}

impl std::fmt::Display for ReplicaIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Enforcement mode for REPLICA IDENTITY checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnforcementMode {
    /// Log a warning but continue processing (default)
    #[default]
    Warn,
    /// Skip events from tables without required identity
    Skip,
    /// Fail the connector if any table lacks required identity
    Fail,
    /// Disabled - no enforcement
    Disabled,
}

impl EnforcementMode {
    /// Parse from string (case-insensitive).
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "warn" => EnforcementMode::Warn,
            "skip" => EnforcementMode::Skip,
            "fail" | "error" => EnforcementMode::Fail,
            "disabled" | "none" | "off" => EnforcementMode::Disabled,
            _ => EnforcementMode::Warn,
        }
    }
}

/// Configuration for REPLICA IDENTITY enforcement.
#[derive(Debug, Clone)]
pub struct ReplicaIdentityConfig {
    /// Require REPLICA IDENTITY FULL for all tables.
    pub require_full: bool,
    /// Enforcement mode when requirement is not met.
    pub enforcement_mode: EnforcementMode,
    /// Tables to exclude from enforcement (glob patterns).
    pub exclude_tables: Vec<String>,
    /// Allow REPLICA IDENTITY INDEX as acceptable alternative.
    pub allow_index: bool,
    /// Log the SQL command to fix the issue.
    pub suggest_fix: bool,
}

impl Default for ReplicaIdentityConfig {
    fn default() -> Self {
        Self {
            require_full: true,
            enforcement_mode: EnforcementMode::Warn,
            exclude_tables: Vec::new(),
            allow_index: true,
            suggest_fix: true,
        }
    }
}

impl ReplicaIdentityConfig {
    /// Create a new builder.
    pub fn builder() -> ReplicaIdentityConfigBuilder {
        ReplicaIdentityConfigBuilder::default()
    }

    /// Strict configuration - fail on any non-FULL table.
    pub fn strict() -> Self {
        Self {
            require_full: true,
            enforcement_mode: EnforcementMode::Fail,
            exclude_tables: Vec::new(),
            allow_index: false,
            suggest_fix: true,
        }
    }

    /// Permissive configuration - warn only.
    pub fn permissive() -> Self {
        Self {
            require_full: true,
            enforcement_mode: EnforcementMode::Warn,
            exclude_tables: Vec::new(),
            allow_index: true,
            suggest_fix: true,
        }
    }

    /// Disabled configuration - no enforcement.
    pub fn disabled() -> Self {
        Self {
            require_full: false,
            enforcement_mode: EnforcementMode::Disabled,
            exclude_tables: Vec::new(),
            allow_index: true,
            suggest_fix: false,
        }
    }
}

/// Builder for ReplicaIdentityConfig.
#[derive(Default)]
pub struct ReplicaIdentityConfigBuilder {
    config: ReplicaIdentityConfig,
}

impl ReplicaIdentityConfigBuilder {
    /// Require REPLICA IDENTITY FULL.
    pub fn require_full(mut self, require: bool) -> Self {
        self.config.require_full = require;
        self
    }

    /// Set enforcement mode.
    pub fn enforcement_mode(mut self, mode: EnforcementMode) -> Self {
        self.config.enforcement_mode = mode;
        self
    }

    /// Add tables to exclude from enforcement.
    pub fn exclude_tables(mut self, patterns: Vec<String>) -> Self {
        self.config.exclude_tables = patterns;
        self
    }

    /// Add a single table exclusion pattern.
    pub fn exclude_table(mut self, pattern: impl Into<String>) -> Self {
        self.config.exclude_tables.push(pattern.into());
        self
    }

    /// Allow REPLICA IDENTITY INDEX as acceptable.
    pub fn allow_index(mut self, allow: bool) -> Self {
        self.config.allow_index = allow;
        self
    }

    /// Suggest SQL fix commands.
    pub fn suggest_fix(mut self, suggest: bool) -> Self {
        self.config.suggest_fix = suggest;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> ReplicaIdentityConfig {
        self.config
    }
}

/// Result of a replica identity check.
#[derive(Debug, Clone)]
pub enum CheckResult {
    /// Identity meets requirements
    Ok,
    /// Warning - identity doesn't meet requirements but continuing
    Warning(String),
    /// Skip - events from this table should be skipped
    Skip(String),
    /// Fail - connector should stop
    Fail(String),
}

impl CheckResult {
    /// Check if the result is OK.
    pub fn is_ok(&self) -> bool {
        matches!(self, CheckResult::Ok)
    }

    /// Check if the result indicates events should be skipped.
    pub fn should_skip(&self) -> bool {
        matches!(self, CheckResult::Skip(_))
    }

    /// Check if the result indicates failure.
    pub fn is_fail(&self) -> bool {
        matches!(self, CheckResult::Fail(_))
    }
}

/// Statistics for replica identity enforcement.
#[derive(Debug, Default)]
pub struct ReplicaIdentityStats {
    /// Tables with FULL identity
    pub full_tables: AtomicU64,
    /// Tables with DEFAULT identity
    pub default_tables: AtomicU64,
    /// Tables with INDEX identity
    pub index_tables: AtomicU64,
    /// Tables with NOTHING identity
    pub nothing_tables: AtomicU64,
    /// Warnings issued
    pub warnings: AtomicU64,
    /// Events skipped due to identity
    pub skipped_events: AtomicU64,
}

impl ReplicaIdentityStats {
    /// Create new stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a table's identity.
    pub fn record_identity(&self, identity: ReplicaIdentity) {
        match identity {
            ReplicaIdentity::Full => self.full_tables.fetch_add(1, Ordering::Relaxed),
            ReplicaIdentity::Default => self.default_tables.fetch_add(1, Ordering::Relaxed),
            ReplicaIdentity::Index => self.index_tables.fetch_add(1, Ordering::Relaxed),
            ReplicaIdentity::Nothing => self.nothing_tables.fetch_add(1, Ordering::Relaxed),
            ReplicaIdentity::Unknown(_) => 0,
        };
    }

    /// Record a warning.
    pub fn record_warning(&self) {
        self.warnings.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a skipped event.
    pub fn record_skip(&self) {
        self.skipped_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Get summary string.
    pub fn summary(&self) -> String {
        format!(
            "FULL: {}, DEFAULT: {}, INDEX: {}, NOTHING: {}, warnings: {}, skipped: {}",
            self.full_tables.load(Ordering::Relaxed),
            self.default_tables.load(Ordering::Relaxed),
            self.index_tables.load(Ordering::Relaxed),
            self.nothing_tables.load(Ordering::Relaxed),
            self.warnings.load(Ordering::Relaxed),
            self.skipped_events.load(Ordering::Relaxed),
        )
    }
}

/// Table identity information.
#[derive(Debug, Clone)]
pub struct TableIdentity {
    pub schema: String,
    pub table: String,
    pub identity: ReplicaIdentity,
    pub checked: bool,
}

/// Enforcer for REPLICA IDENTITY requirements.
pub struct ReplicaIdentityEnforcer {
    config: ReplicaIdentityConfig,
    stats: ReplicaIdentityStats,
    /// Cache of table identities (schema.table -> identity)
    table_cache: Arc<RwLock<HashMap<String, TableIdentity>>>,
    /// Tables that have been warned about (to avoid duplicate warnings)
    warned_tables: Arc<RwLock<std::collections::HashSet<String>>>,
}

impl ReplicaIdentityEnforcer {
    /// Create a new enforcer with the given configuration.
    pub fn new(config: ReplicaIdentityConfig) -> Self {
        Self {
            config,
            stats: ReplicaIdentityStats::new(),
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            warned_tables: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }

    /// Check a table's replica identity and return the enforcement result.
    pub async fn check(&self, schema: &str, table: &str, identity: ReplicaIdentity) -> CheckResult {
        let qualified_name = format!("{}.{}", schema, table);

        // Record identity in stats
        self.stats.record_identity(identity);

        // Cache the identity
        {
            let mut cache = self.table_cache.write().await;
            cache.insert(
                qualified_name.clone(),
                TableIdentity {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    identity,
                    checked: true,
                },
            );
        }

        // Check if enforcement is disabled
        if !self.config.require_full || self.config.enforcement_mode == EnforcementMode::Disabled {
            return CheckResult::Ok;
        }

        // Check if table is excluded
        if self.is_excluded(schema, table) {
            debug!(
                "Table {} excluded from REPLICA IDENTITY enforcement",
                qualified_name
            );
            return CheckResult::Ok;
        }

        // Check if identity meets requirements
        let meets_requirements = identity.is_full()
            || (self.config.allow_index && matches!(identity, ReplicaIdentity::Index));

        if meets_requirements {
            return CheckResult::Ok;
        }

        // Identity doesn't meet requirements - apply enforcement
        let message = self.build_warning_message(schema, table, identity);

        match self.config.enforcement_mode {
            EnforcementMode::Warn => {
                // Only warn once per table
                let mut warned = self.warned_tables.write().await;
                if !warned.contains(&qualified_name) {
                    warn!("{}", message);
                    warned.insert(qualified_name);
                    self.stats.record_warning();
                }
                CheckResult::Warning(message)
            }
            EnforcementMode::Skip => {
                self.stats.record_skip();
                CheckResult::Skip(message)
            }
            EnforcementMode::Fail => {
                error!("{}", message);
                CheckResult::Fail(message)
            }
            EnforcementMode::Disabled => CheckResult::Ok,
        }
    }

    /// Quick synchronous check (without caching).
    pub fn check_sync(&self, schema: &str, table: &str, identity: ReplicaIdentity) -> CheckResult {
        // Record identity in stats
        self.stats.record_identity(identity);

        // Check if enforcement is disabled
        if !self.config.require_full || self.config.enforcement_mode == EnforcementMode::Disabled {
            return CheckResult::Ok;
        }

        // Check if table is excluded
        if self.is_excluded(schema, table) {
            return CheckResult::Ok;
        }

        // Check if identity meets requirements
        let meets_requirements = identity.is_full()
            || (self.config.allow_index && matches!(identity, ReplicaIdentity::Index));

        if meets_requirements {
            return CheckResult::Ok;
        }

        // Identity doesn't meet requirements
        let message = self.build_warning_message(schema, table, identity);

        match self.config.enforcement_mode {
            EnforcementMode::Warn => {
                warn!("{}", message);
                self.stats.record_warning();
                CheckResult::Warning(message)
            }
            EnforcementMode::Skip => {
                self.stats.record_skip();
                CheckResult::Skip(message)
            }
            EnforcementMode::Fail => {
                error!("{}", message);
                CheckResult::Fail(message)
            }
            EnforcementMode::Disabled => CheckResult::Ok,
        }
    }

    /// Check if a table is excluded from enforcement.
    fn is_excluded(&self, schema: &str, table: &str) -> bool {
        let qualified = format!("{}.{}", schema, table);
        self.config.exclude_tables.iter().any(|pattern| {
            if pattern.contains('*') {
                glob_match(pattern, &qualified)
            } else {
                pattern == &qualified
            }
        })
    }

    /// Build the warning/error message.
    fn build_warning_message(
        &self,
        schema: &str,
        table: &str,
        identity: ReplicaIdentity,
    ) -> String {
        let mut msg = format!(
            "Table \"{}\".\"{}\" has REPLICA IDENTITY {} (not FULL). \
             UPDATE/DELETE events may not include complete 'before' image.",
            schema,
            table,
            identity.as_str()
        );

        if self.config.suggest_fix {
            msg.push_str(&format!(
                " Fix with: {}",
                ReplicaIdentity::alter_table_sql(schema, table, ReplicaIdentity::Full)
            ));
        }

        msg
    }

    /// Get enforcement statistics.
    pub fn stats(&self) -> &ReplicaIdentityStats {
        &self.stats
    }

    /// Get configuration.
    pub fn config(&self) -> &ReplicaIdentityConfig {
        &self.config
    }

    /// Get all cached table identities.
    pub async fn table_identities(&self) -> HashMap<String, TableIdentity> {
        self.table_cache.read().await.clone()
    }

    /// Log a summary of table identities.
    pub async fn log_summary(&self) {
        let cache = self.table_cache.read().await;
        let full_count = cache.values().filter(|t| t.identity.is_full()).count();
        let total = cache.len();

        if full_count < total {
            warn!(
                "REPLICA IDENTITY summary: {}/{} tables have FULL identity. Stats: {}",
                full_count,
                total,
                self.stats.summary()
            );

            // Log tables without FULL identity
            for (name, info) in cache.iter() {
                if !info.identity.is_full() {
                    warn!(
                        "  - {} has REPLICA IDENTITY {}",
                        name,
                        info.identity.as_str()
                    );
                }
            }
        } else {
            info!("REPLICA IDENTITY: All {} tables have FULL identity", total);
        }
    }
}

impl Default for ReplicaIdentityEnforcer {
    fn default() -> Self {
        Self::new(ReplicaIdentityConfig::default())
    }
}

/// Simple glob pattern matching.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    fn match_impl(pattern: &[char], text: &[char]) -> bool {
        if pattern.is_empty() {
            return text.is_empty();
        }

        if pattern[0] == '*' {
            for i in 0..=text.len() {
                if match_impl(&pattern[1..], &text[i..]) {
                    return true;
                }
            }
            return false;
        }

        if text.is_empty() {
            return false;
        }

        if pattern[0] == '?' || pattern[0] == text[0] {
            return match_impl(&pattern[1..], &text[1..]);
        }

        false
    }

    match_impl(&pattern_chars, &text_chars)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_identity_from_byte() {
        assert_eq!(ReplicaIdentity::from_byte(b'd'), ReplicaIdentity::Default);
        assert_eq!(ReplicaIdentity::from_byte(b'n'), ReplicaIdentity::Nothing);
        assert_eq!(ReplicaIdentity::from_byte(b'f'), ReplicaIdentity::Full);
        assert_eq!(ReplicaIdentity::from_byte(b'i'), ReplicaIdentity::Index);
        assert!(matches!(
            ReplicaIdentity::from_byte(b'x'),
            ReplicaIdentity::Unknown(b'x')
        ));
    }

    #[test]
    fn test_replica_identity_to_byte() {
        assert_eq!(ReplicaIdentity::Default.to_byte(), b'd');
        assert_eq!(ReplicaIdentity::Nothing.to_byte(), b'n');
        assert_eq!(ReplicaIdentity::Full.to_byte(), b'f');
        assert_eq!(ReplicaIdentity::Index.to_byte(), b'i');
    }

    #[test]
    fn test_is_full() {
        assert!(ReplicaIdentity::Full.is_full());
        assert!(!ReplicaIdentity::Default.is_full());
        assert!(!ReplicaIdentity::Nothing.is_full());
        assert!(!ReplicaIdentity::Index.is_full());
    }

    #[test]
    fn test_has_before_image() {
        assert!(ReplicaIdentity::Full.has_before_image());
        assert!(ReplicaIdentity::Default.has_before_image());
        assert!(ReplicaIdentity::Index.has_before_image());
        assert!(!ReplicaIdentity::Nothing.has_before_image());
    }

    #[test]
    fn test_alter_table_sql() {
        let sql = ReplicaIdentity::alter_table_sql("public", "users", ReplicaIdentity::Full);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"users\" REPLICA IDENTITY FULL"
        );
    }

    #[test]
    fn test_enforcement_mode_parse() {
        assert_eq!(EnforcementMode::parse("warn"), EnforcementMode::Warn);
        assert_eq!(EnforcementMode::parse("WARN"), EnforcementMode::Warn);
        assert_eq!(EnforcementMode::parse("skip"), EnforcementMode::Skip);
        assert_eq!(EnforcementMode::parse("fail"), EnforcementMode::Fail);
        assert_eq!(EnforcementMode::parse("error"), EnforcementMode::Fail);
        assert_eq!(
            EnforcementMode::parse("disabled"),
            EnforcementMode::Disabled
        );
    }

    #[test]
    fn test_default_config() {
        let config = ReplicaIdentityConfig::default();
        assert!(config.require_full);
        assert_eq!(config.enforcement_mode, EnforcementMode::Warn);
        assert!(config.allow_index);
        assert!(config.suggest_fix);
    }

    #[test]
    fn test_strict_config() {
        let config = ReplicaIdentityConfig::strict();
        assert!(config.require_full);
        assert_eq!(config.enforcement_mode, EnforcementMode::Fail);
        assert!(!config.allow_index);
    }

    #[test]
    fn test_config_builder() {
        let config = ReplicaIdentityConfig::builder()
            .require_full(true)
            .enforcement_mode(EnforcementMode::Skip)
            .allow_index(false)
            .exclude_table("audit.*")
            .build();

        assert!(config.require_full);
        assert_eq!(config.enforcement_mode, EnforcementMode::Skip);
        assert!(!config.allow_index);
        assert_eq!(config.exclude_tables, vec!["audit.*".to_string()]);
    }

    #[test]
    fn test_enforcer_full_identity_ok() {
        let enforcer = ReplicaIdentityEnforcer::default();
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Full);
        assert!(result.is_ok());
    }

    #[test]
    fn test_enforcer_default_identity_warns() {
        let enforcer = ReplicaIdentityEnforcer::default();
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Default);
        assert!(matches!(result, CheckResult::Warning(_)));
    }

    #[test]
    fn test_enforcer_index_with_allow_index() {
        let config = ReplicaIdentityConfig::builder().allow_index(true).build();
        let enforcer = ReplicaIdentityEnforcer::new(config);
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Index);
        assert!(result.is_ok());
    }

    #[test]
    fn test_enforcer_index_without_allow_index() {
        let config = ReplicaIdentityConfig::builder().allow_index(false).build();
        let enforcer = ReplicaIdentityEnforcer::new(config);
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Index);
        assert!(matches!(result, CheckResult::Warning(_)));
    }

    #[test]
    fn test_enforcer_skip_mode() {
        let config = ReplicaIdentityConfig::builder()
            .enforcement_mode(EnforcementMode::Skip)
            .build();
        let enforcer = ReplicaIdentityEnforcer::new(config);
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Default);
        assert!(result.should_skip());
    }

    #[test]
    fn test_enforcer_fail_mode() {
        let config = ReplicaIdentityConfig::builder()
            .enforcement_mode(EnforcementMode::Fail)
            .build();
        let enforcer = ReplicaIdentityEnforcer::new(config);
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Nothing);
        assert!(result.is_fail());
    }

    #[test]
    fn test_enforcer_disabled() {
        let config = ReplicaIdentityConfig::disabled();
        let enforcer = ReplicaIdentityEnforcer::new(config);
        let result = enforcer.check_sync("public", "users", ReplicaIdentity::Nothing);
        assert!(result.is_ok());
    }

    #[test]
    fn test_enforcer_excluded_table() {
        let config = ReplicaIdentityConfig::builder()
            .exclude_table("audit.*")
            .build();
        let enforcer = ReplicaIdentityEnforcer::new(config);
        let result = enforcer.check_sync("audit", "log", ReplicaIdentity::Nothing);
        assert!(result.is_ok());
    }

    #[test]
    fn test_stats_tracking() {
        let enforcer = ReplicaIdentityEnforcer::default();

        enforcer.check_sync("s", "t1", ReplicaIdentity::Full);
        enforcer.check_sync("s", "t2", ReplicaIdentity::Default);
        enforcer.check_sync("s", "t3", ReplicaIdentity::Index);
        enforcer.check_sync("s", "t4", ReplicaIdentity::Nothing);

        let stats = enforcer.stats();
        assert_eq!(stats.full_tables.load(Ordering::Relaxed), 1);
        assert_eq!(stats.default_tables.load(Ordering::Relaxed), 1);
        assert_eq!(stats.index_tables.load(Ordering::Relaxed), 1);
        assert_eq!(stats.nothing_tables.load(Ordering::Relaxed), 1);
        assert_eq!(stats.warnings.load(Ordering::Relaxed), 2); // DEFAULT and NOTHING
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("audit.*", "audit.log"));
        assert!(glob_match("audit.*", "audit.events"));
        assert!(!glob_match("audit.*", "users.audit"));
        assert!(glob_match("*.audit", "schema.audit"));
        assert!(glob_match("*", "anything"));
    }

    #[tokio::test]
    async fn test_async_check_caching() {
        let enforcer = ReplicaIdentityEnforcer::default();

        // First check
        let _ = enforcer
            .check("public", "users", ReplicaIdentity::Full)
            .await;

        // Verify caching
        let identities = enforcer.table_identities().await;
        assert!(identities.contains_key("public.users"));
        assert_eq!(
            identities.get("public.users").unwrap().identity,
            ReplicaIdentity::Full
        );
    }

    #[test]
    fn test_check_result_methods() {
        assert!(CheckResult::Ok.is_ok());
        assert!(!CheckResult::Ok.should_skip());
        assert!(!CheckResult::Ok.is_fail());

        assert!(!CheckResult::Warning("test".to_string()).is_ok());
        assert!(!CheckResult::Warning("test".to_string()).should_skip());

        assert!(CheckResult::Skip("test".to_string()).should_skip());
        assert!(CheckResult::Fail("test".to_string()).is_fail());
    }
}

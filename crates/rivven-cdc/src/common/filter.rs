//! Table and column filtering for CDC events
//!
//! Provides production-grade filtering capabilities:
//! - Include/exclude tables by pattern (glob syntax)
//! - Include/exclude columns per table
//! - Regex-based matching
//! - Column masking for sensitive data (PII redaction)
//!
//! # Example
//!
//! ```rust
//! use rivven_cdc::CdcFilterConfig;
//!
//! let config = CdcFilterConfig {
//!     include_tables: vec!["public.*".to_string()],
//!     exclude_tables: vec!["*.audit_log".to_string()],
//!     mask_columns: vec!["password".to_string(), "ssn".to_string()],
//!     ..Default::default()
//! };
//! ```

use crate::common::CdcEvent;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Filter configuration for CDC tables and columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcFilterConfig {
    /// Tables to include (supports glob patterns like "public.*")
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude (evaluated after includes)
    #[serde(default)]
    pub exclude_tables: Vec<String>,

    /// Per-table column configuration
    #[serde(default)]
    pub table_columns: HashMap<String, TableColumnConfig>,

    /// Global column excludes (applied to all tables)
    #[serde(default)]
    pub global_exclude_columns: Vec<String>,

    /// Columns to mask (show as "***REDACTED***")
    #[serde(default)]
    pub mask_columns: Vec<String>,
}

/// Column configuration for a specific table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumnConfig {
    /// Columns to include (if empty, include all)
    #[serde(default)]
    pub include: Vec<String>,

    /// Columns to exclude
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Columns to mask with "***REDACTED***"
    #[serde(default)]
    pub mask: Vec<String>,
}

impl Default for CdcFilterConfig {
    fn default() -> Self {
        Self {
            include_tables: vec!["*".to_string()], // Include all by default
            exclude_tables: vec![],
            table_columns: HashMap::new(),
            global_exclude_columns: vec![],
            mask_columns: vec![],
        }
    }
}

/// Compiled filter for efficient runtime evaluation
pub struct CdcFilter {
    config: CdcFilterConfig,
    include_patterns: Vec<Regex>,
    exclude_patterns: Vec<Regex>,
    global_exclude_set: HashSet<String>,
    mask_set: HashSet<String>,
}

impl CdcFilter {
    /// Create a new filter from configuration
    pub fn new(config: CdcFilterConfig) -> Result<Self, regex::Error> {
        let include_patterns = config
            .include_tables
            .iter()
            .map(|p| Self::glob_to_regex(p))
            .collect::<Result<Vec<_>, _>>()?;

        let exclude_patterns = config
            .exclude_tables
            .iter()
            .map(|p| Self::glob_to_regex(p))
            .collect::<Result<Vec<_>, _>>()?;

        let global_exclude_set: HashSet<String> = config
            .global_exclude_columns
            .iter()
            .map(|s| s.to_lowercase())
            .collect();

        let mask_set: HashSet<String> = config
            .mask_columns
            .iter()
            .map(|s| s.to_lowercase())
            .collect();

        Ok(Self {
            config,
            include_patterns,
            exclude_patterns,
            global_exclude_set,
            mask_set,
        })
    }

    /// Convert a glob pattern to regex
    fn glob_to_regex(pattern: &str) -> Result<Regex, regex::Error> {
        let escaped = regex::escape(pattern);
        let regex_pattern = escaped.replace(r"\*", ".*").replace(r"\?", ".");
        Regex::new(&format!("^{}$", regex_pattern))
    }

    /// Check if a table should be included
    pub fn should_include_table(&self, schema: &str, table: &str) -> bool {
        let full_name = format!("{}.{}", schema, table);

        // Check excludes first (higher priority)
        for pattern in &self.exclude_patterns {
            if pattern.is_match(&full_name) || pattern.is_match(table) {
                return false;
            }
        }

        // Check includes
        for pattern in &self.include_patterns {
            if pattern.is_match(&full_name) || pattern.is_match(table) {
                return true;
            }
        }

        // Default: exclude if no patterns match
        self.include_patterns.is_empty()
    }

    /// Filter and transform a CDC event
    ///
    /// Returns true if the event should be processed, false if filtered out.
    /// Modifies the event in place to remove/mask columns.
    pub fn filter_event(&self, event: &mut CdcEvent) -> bool {
        // Table filter
        if !self.should_include_table(&event.schema, &event.table) {
            return false;
        }

        // Column filtering
        let table_key = format!("{}.{}", event.schema, event.table);
        let table_config = self.config.table_columns.get(&table_key);

        // Filter 'before' payload
        if let Some(ref mut before) = event.before {
            self.filter_json(before, table_config);
        }

        // Filter 'after' payload
        if let Some(ref mut after) = event.after {
            self.filter_json(after, table_config);
        }

        true
    }

    /// Filter and mask columns in a JSON object
    fn filter_json(
        &self,
        value: &mut serde_json::Value,
        table_config: Option<&TableColumnConfig>,
    ) {
        if let serde_json::Value::Object(map) = value {
            // Collect columns to remove
            let mut to_remove = Vec::new();
            let mut to_mask = Vec::new();

            for key in map.keys() {
                let key_lower = key.to_lowercase();

                // Global excludes
                if self.global_exclude_set.contains(&key_lower) {
                    to_remove.push(key.clone());
                    continue;
                }

                // Global masks
                if self.mask_set.contains(&key_lower) {
                    to_mask.push(key.clone());
                    continue;
                }

                // Table-specific config
                if let Some(tc) = table_config {
                    // Check table excludes
                    if tc.exclude.iter().any(|e| e.to_lowercase() == key_lower) {
                        to_remove.push(key.clone());
                        continue;
                    }

                    // Check table includes (if specified, only include those)
                    if !tc.include.is_empty()
                        && !tc.include.iter().any(|i| i.to_lowercase() == key_lower)
                    {
                        to_remove.push(key.clone());
                        continue;
                    }

                    // Check table masks
                    if tc.mask.iter().any(|m| m.to_lowercase() == key_lower) {
                        to_mask.push(key.clone());
                    }
                }
            }

            // Remove excluded columns
            for key in to_remove {
                map.remove(&key);
            }

            // Mask sensitive columns
            for key in to_mask {
                if let Some(v) = map.get_mut(&key) {
                    *v = serde_json::Value::String("***REDACTED***".to_string());
                }
            }
        }
    }

    /// Get the filter configuration
    pub fn config(&self) -> &CdcFilterConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    #[test]
    fn test_table_include_all() {
        let config = CdcFilterConfig::default();
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(filter.should_include_table("schema", "orders"));
    }

    #[test]
    fn test_table_include_pattern() {
        let config = CdcFilterConfig {
            include_tables: vec!["public.*".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(filter.should_include_table("public", "orders"));
        assert!(!filter.should_include_table("private", "secrets"));
    }

    #[test]
    fn test_table_exclude() {
        let config = CdcFilterConfig {
            include_tables: vec!["*".to_string()],
            exclude_tables: vec!["*.audit_log".to_string(), "private.*".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(!filter.should_include_table("public", "audit_log"));
        assert!(!filter.should_include_table("private", "secrets"));
    }

    #[test]
    fn test_column_filtering() {
        let config = CdcFilterConfig {
            global_exclude_columns: vec!["password".to_string(), "ssn".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        let mut event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "users".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "name": "Alice",
                "password": "secret123",
                "ssn": "123-45-6789"
            })),
            timestamp: 0,
            transaction: None,
        };

        assert!(filter.filter_event(&mut event));

        let after = event.after.as_ref().unwrap();
        assert!(after.get("id").is_some());
        assert!(after.get("name").is_some());
        assert!(after.get("password").is_none());
        assert!(after.get("ssn").is_none());
    }

    #[test]
    fn test_column_masking() {
        let config = CdcFilterConfig {
            mask_columns: vec!["email".to_string(), "phone".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        let mut event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "users".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "phone": "+1-555-1234"
            })),
            timestamp: 0,
            transaction: None,
        };

        assert!(filter.filter_event(&mut event));

        let after = event.after.as_ref().unwrap();
        assert_eq!(after.get("email").unwrap(), "***REDACTED***");
        assert_eq!(after.get("phone").unwrap(), "***REDACTED***");
        assert_eq!(after.get("name").unwrap(), "Alice");
    }

    #[test]
    fn test_table_specific_columns() {
        let mut table_columns = HashMap::new();
        table_columns.insert(
            "public.users".to_string(),
            TableColumnConfig {
                include: vec!["id".to_string(), "name".to_string()],
                exclude: vec![],
                mask: vec![],
            },
        );

        let config = CdcFilterConfig {
            table_columns,
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        let mut event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "users".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "created_at": "2024-01-01"
            })),
            timestamp: 0,
            transaction: None,
        };

        assert!(filter.filter_event(&mut event));

        let after = event.after.as_ref().unwrap();
        assert!(after.get("id").is_some());
        assert!(after.get("name").is_some());
        assert!(after.get("email").is_none()); // Not in include list
        assert!(after.get("created_at").is_none()); // Not in include list
    }

    #[test]
    fn test_filter_rejects_excluded_table() {
        let config = CdcFilterConfig {
            exclude_tables: vec!["audit_log".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        let mut event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "audit_log".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            timestamp: 0,
            transaction: None,
        };

        assert!(!filter.filter_event(&mut event));
    }
}

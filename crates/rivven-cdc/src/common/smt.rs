//! # Single Message Transforms (SMTs)
//!
//! Debezium-compatible message transformations for CDC events.
//!
//! ## Built-in Transforms (17 total)
//!
//! | Transform | Description |
//! |-----------|-------------|
//! | `ExtractNewRecordState` | Flatten envelope, extract "after" state |
//! | `ValueToKey` | Extract key fields from value |
//! | `TimestampConverter` | Convert timestamps (ISO8601, epoch, date) |
//! | `TimezoneConverter` | Convert between timezones (IANA names) |
//! | `MaskField` | Mask sensitive fields |
//! | `ReplaceField` | Rename, include, exclude fields |
//! | `InsertField` | Add static or computed fields |
//! | `Filter` | Drop events based on condition |
//! | `Cast` | Convert field types |
//! | `Flatten` | Flatten nested structures |
//! | `RegexRouter` | Route based on regex patterns |
//! | `ContentRouter` | Route based on field values |
//! | `ComputeField` | Compute new fields (concat, hash, etc.) |
//! | `ConditionalSmt` | Apply transforms conditionally (predicates) |
//! | `HeaderToValue` | Move envelope fields into record |
//! | `Unwrap` | Extract nested field to top level |
//! | `SetNull` | Conditionally nullify fields |
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::smt::*;
//!
//! // Create transform chain
//! let transforms = SmtChain::new()
//!     .add(ExtractNewRecordState::new())
//!     .add(MaskField::new(vec!["ssn", "credit_card"]))
//!     .add(TimestampConverter::new(["created_at"], TimestampFormat::Iso8601))
//!     .add(ContentRouter::new()
//!         .route("priority", "high", "priority-events")
//!         .default_topic("default-events"));
//!
//! // Apply to event
//! let transformed = transforms.apply(event)?;
//! ```

use crate::common::{CdcEvent, CdcOp};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

/// Convert CdcOp to Debezium-style operation code.
fn op_to_code(op: &CdcOp) -> &'static str {
    match op {
        CdcOp::Insert => "c",    // create
        CdcOp::Update => "u",    // update
        CdcOp::Delete => "d",    // delete
        CdcOp::Tombstone => "d", // tombstone (Debezium treats as delete)
        CdcOp::Truncate => "t",  // truncate
        CdcOp::Snapshot => "r",  // read (snapshot)
        CdcOp::Schema => "s",    // schema change (DDL)
    }
}

/// Trait for Single Message Transforms.
pub trait Smt: Send + Sync {
    /// Transform a CDC event. Returns None to drop the event.
    fn apply(&self, event: CdcEvent) -> Option<CdcEvent>;

    /// Get the transform name.
    fn name(&self) -> &'static str;
}

/// Chain of SMT transforms applied in sequence.
pub struct SmtChain {
    transforms: Vec<Arc<dyn Smt>>,
}

impl Default for SmtChain {
    fn default() -> Self {
        Self::new()
    }
}

impl SmtChain {
    /// Create a new empty chain.
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
        }
    }

    /// Add a transform to the chain (builder pattern).
    #[allow(clippy::should_implement_trait)] // Builder pattern, not std::ops::Add
    pub fn add<T: Smt + 'static>(mut self, transform: T) -> Self {
        self.transforms.push(Arc::new(transform));
        self
    }

    /// Add a boxed transform to the chain.
    pub fn add_boxed(mut self, transform: Arc<dyn Smt>) -> Self {
        self.transforms.push(transform);
        self
    }

    /// Apply all transforms in sequence.
    pub fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        for transform in &self.transforms {
            event = transform.apply(event)?;
        }
        Some(event)
    }

    /// Get number of transforms.
    pub fn len(&self) -> usize {
        self.transforms.len()
    }

    /// Check if chain is empty.
    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }

    /// Get transform names.
    pub fn names(&self) -> Vec<&'static str> {
        self.transforms.iter().map(|t| t.name()).collect()
    }
}

// ============================================================================
// ExtractNewRecordState - Flatten envelope to just the "after" data
// ============================================================================

/// Extract new record state - adds envelope fields to the data.
///
/// Debezium events have an envelope with "before", "after", "op", etc.
/// This transform can add envelope fields to the "after" data for downstream processing.
///
/// # Options
/// - `drop_tombstones`: Drop delete events entirely (default: false)
/// - `delete_handling`: How to handle deletes ("drop", "rewrite", "none")
/// - `add_fields`: Which envelope fields to add to the record
#[derive(Debug, Clone)]
pub struct ExtractNewRecordState {
    /// Drop delete events entirely
    drop_tombstones: bool,
    /// How to handle deletes
    delete_handling: DeleteHandling,
    /// Add operation field
    add_op: bool,
    /// Add table field
    add_table: bool,
    /// Add schema field
    add_schema: bool,
    /// Add timestamp field
    add_ts: bool,
    /// Header prefix for envelope fields
    header_prefix: String,
}

/// How to handle delete operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeleteHandling {
    /// Drop delete events
    Drop,
    /// Rewrite delete to include "__deleted" field
    Rewrite,
    /// No special handling
    #[default]
    None,
}

impl Default for ExtractNewRecordState {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtractNewRecordState {
    /// Create a new extractor with default settings.
    pub fn new() -> Self {
        Self {
            drop_tombstones: false,
            delete_handling: DeleteHandling::None,
            add_op: false,
            add_table: false,
            add_schema: false,
            add_ts: false,
            header_prefix: "__".to_string(),
        }
    }

    /// Drop tombstone (delete) events.
    pub fn drop_tombstones(mut self) -> Self {
        self.drop_tombstones = true;
        self
    }

    /// Set delete handling mode.
    pub fn delete_handling(mut self, mode: DeleteHandling) -> Self {
        self.delete_handling = mode;
        self
    }

    /// Add operation field to output.
    pub fn add_op_field(mut self) -> Self {
        self.add_op = true;
        self
    }

    /// Add table field to output.
    pub fn add_table_field(mut self) -> Self {
        self.add_table = true;
        self
    }

    /// Add schema field to output.
    pub fn add_schema_field(mut self) -> Self {
        self.add_schema = true;
        self
    }

    /// Add timestamp field to output.
    pub fn add_ts_field(mut self) -> Self {
        self.add_ts = true;
        self
    }

    /// Set header prefix for added fields.
    pub fn header_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.header_prefix = prefix.into();
        self
    }
}

impl Smt for ExtractNewRecordState {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        // Handle deletes
        if event.op == CdcOp::Delete {
            if self.drop_tombstones {
                return None;
            }

            match self.delete_handling {
                DeleteHandling::Drop => return None,
                DeleteHandling::Rewrite => {
                    // Use "before" as the record and add __deleted field
                    if let Some(before) = &event.before {
                        let mut record = before.clone();
                        if let Some(obj) = record.as_object_mut() {
                            obj.insert("__deleted".to_string(), Value::Bool(true));
                        }
                        event.after = Some(record);
                    }
                }
                DeleteHandling::None => {}
            }
        }

        // Add envelope fields to "after"
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                if self.add_op {
                    obj.insert(
                        format!("{}op", self.header_prefix),
                        Value::String(op_to_code(&event.op).to_string()),
                    );
                }
                if self.add_table {
                    obj.insert(
                        format!("{}table", self.header_prefix),
                        Value::String(event.table.clone()),
                    );
                }
                if self.add_schema {
                    obj.insert(
                        format!("{}schema", self.header_prefix),
                        Value::String(event.schema.clone()),
                    );
                }
                if self.add_ts {
                    obj.insert(
                        format!("{}ts_ms", self.header_prefix),
                        Value::Number((event.timestamp * 1000).into()),
                    );
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "ExtractNewRecordState"
    }
}

// ============================================================================
// ValueToKey - Extract key fields from value
// ============================================================================

/// Extract key fields from the value.
///
/// Useful for creating a message key from specific value fields.
/// Stores the key in the "after" data as "__key" field.
#[derive(Debug, Clone)]
pub struct ValueToKey {
    /// Fields to extract for the key
    fields: Vec<String>,
}

impl ValueToKey {
    /// Create a new ValueToKey with specified fields.
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }

    /// Create from field names.
    pub fn with_fields<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }

    /// Extract key from event.
    pub fn extract_key(&self, event: &CdcEvent) -> Option<Value> {
        let source = event.after.as_ref().or(event.before.as_ref())?;

        if let Some(obj) = source.as_object() {
            let mut key_obj = Map::new();
            for field in &self.fields {
                if let Some(value) = obj.get(field) {
                    key_obj.insert(field.clone(), value.clone());
                }
            }

            if !key_obj.is_empty() {
                return Some(Value::Object(key_obj));
            }
        }

        None
    }
}

impl Smt for ValueToKey {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(key) = self.extract_key(&event) {
            // Store key in after data
            if let Some(after) = &mut event.after {
                if let Some(obj) = after.as_object_mut() {
                    obj.insert("__key".to_string(), key);
                }
            }
        }
        Some(event)
    }

    fn name(&self) -> &'static str {
        "ValueToKey"
    }
}

// ============================================================================
// TimestampConverter - Convert timestamp formats
// ============================================================================

/// Target format for timestamp conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampFormat {
    /// ISO 8601 string (2024-01-15T10:30:00Z)
    #[default]
    Iso8601,
    /// Unix epoch seconds
    EpochSeconds,
    /// Unix epoch milliseconds
    EpochMillis,
    /// Unix epoch microseconds
    EpochMicros,
    /// Date only (2024-01-15)
    DateOnly,
    /// Time only (10:30:00)
    TimeOnly,
}

/// Convert timestamp fields between formats.
#[derive(Debug, Clone)]
pub struct TimestampConverter {
    /// Fields to convert
    fields: Vec<String>,
    /// Target format
    target_format: TimestampFormat,
}

impl TimestampConverter {
    /// Create a new converter for specific fields.
    pub fn new<I, S>(fields: I, format: TimestampFormat) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fields: fields.into_iter().map(Into::into).collect(),
            target_format: format,
        }
    }

    /// Convert a value to the target format.
    fn convert_value(&self, value: &Value) -> Option<Value> {
        let timestamp = self.parse_timestamp(value)?;

        Some(match self.target_format {
            TimestampFormat::Iso8601 => Value::String(timestamp.to_rfc3339()),
            TimestampFormat::EpochSeconds => Value::Number(timestamp.timestamp().into()),
            TimestampFormat::EpochMillis => Value::Number(timestamp.timestamp_millis().into()),
            TimestampFormat::EpochMicros => Value::Number(timestamp.timestamp_micros().into()),
            TimestampFormat::DateOnly => Value::String(timestamp.format("%Y-%m-%d").to_string()),
            TimestampFormat::TimeOnly => Value::String(timestamp.format("%H:%M:%S").to_string()),
        })
    }

    /// Parse various timestamp formats.
    fn parse_timestamp(&self, value: &Value) -> Option<DateTime<Utc>> {
        match value {
            Value::String(s) => {
                // Try ISO 8601
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    return Some(dt.with_timezone(&Utc));
                }
                // Try common formats
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return Some(Utc.from_utc_datetime(&dt));
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    return Some(Utc.from_utc_datetime(&dt));
                }
                None
            }
            Value::Number(n) => {
                // Assume milliseconds if large, seconds otherwise
                let ts = n.as_i64()?;
                if ts > 1_000_000_000_000 {
                    // Milliseconds
                    DateTime::from_timestamp_millis(ts)
                } else {
                    // Seconds
                    DateTime::from_timestamp(ts, 0)
                }
            }
            _ => None,
        }
    }
}

impl Smt for TimestampConverter {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        // Convert in "after"
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                for field in &self.fields {
                    if let Some(value) = obj.get(field).cloned() {
                        if let Some(converted) = self.convert_value(&value) {
                            obj.insert(field.clone(), converted);
                        }
                    }
                }
            }
        }

        // Convert in "before"
        if let Some(before) = &mut event.before {
            if let Some(obj) = before.as_object_mut() {
                for field in &self.fields {
                    if let Some(value) = obj.get(field).cloned() {
                        if let Some(converted) = self.convert_value(&value) {
                            obj.insert(field.clone(), converted);
                        }
                    }
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "TimestampConverter"
    }
}

// ============================================================================
// MaskField - Mask sensitive field values
// ============================================================================

/// Masking strategy for sensitive fields.
#[derive(Debug, Clone, Default)]
pub enum MaskStrategy {
    /// Replace with fixed string
    Fixed(String),
    /// Replace with asterisks, keeping length
    #[default]
    Asterisks,
    /// Hash the value (SHA-256, hex)
    Hash,
    /// Keep first N characters, mask rest
    PartialMask { keep_first: usize, keep_last: usize },
    /// Replace with null
    Null,
    /// Redact completely (remove field)
    Redact,
}

/// Mask sensitive fields in CDC events.
#[derive(Debug, Clone)]
pub struct MaskField {
    /// Fields to mask
    fields: HashSet<String>,
    /// Masking strategy
    strategy: MaskStrategy,
}

impl MaskField {
    /// Create a new masker with default asterisk strategy.
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fields: fields.into_iter().map(Into::into).collect(),
            strategy: MaskStrategy::Asterisks,
        }
    }

    /// Set masking strategy.
    pub fn with_strategy(mut self, strategy: MaskStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Mask a value.
    fn mask_value(&self, value: &Value) -> Value {
        match &self.strategy {
            MaskStrategy::Fixed(s) => Value::String(s.clone()),
            MaskStrategy::Asterisks => {
                if let Some(s) = value.as_str() {
                    Value::String("*".repeat(s.len().min(20)))
                } else {
                    Value::String("****".to_string())
                }
            }
            MaskStrategy::Hash => {
                use ring::digest::{digest, SHA256};
                let bytes = serde_json::to_vec(value).unwrap_or_default();
                let hash = digest(&SHA256, &bytes);
                Value::String(hex::encode(hash.as_ref()))
            }
            MaskStrategy::PartialMask {
                keep_first,
                keep_last,
            } => {
                if let Some(s) = value.as_str() {
                    let len = s.len();
                    if len <= keep_first + keep_last {
                        Value::String("*".repeat(len))
                    } else {
                        let first: String = s.chars().take(*keep_first).collect();
                        let last: String = s.chars().skip(len - keep_last).collect();
                        let middle = "*".repeat(len - keep_first - keep_last);
                        Value::String(format!("{}{}{}", first, middle, last))
                    }
                } else {
                    Value::String("****".to_string())
                }
            }
            MaskStrategy::Null => Value::Null,
            MaskStrategy::Redact => Value::Null, // Will be removed
        }
    }

    /// Apply masking to a JSON object.
    fn mask_object(&self, obj: &mut Map<String, Value>) {
        for field in &self.fields {
            if obj.contains_key(field) {
                if matches!(self.strategy, MaskStrategy::Redact) {
                    obj.remove(field);
                } else if let Some(value) = obj.get(field).cloned() {
                    obj.insert(field.clone(), self.mask_value(&value));
                }
            }
        }
    }
}

impl Smt for MaskField {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                self.mask_object(obj);
            }
        }

        if let Some(before) = &mut event.before {
            if let Some(obj) = before.as_object_mut() {
                self.mask_object(obj);
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "MaskField"
    }
}

// ============================================================================
// ReplaceField - Rename, include, or exclude fields
// ============================================================================

/// Replace, rename, include, or exclude fields.
#[derive(Debug, Clone, Default)]
pub struct ReplaceField {
    /// Fields to include (whitelist)
    include: Option<HashSet<String>>,
    /// Fields to exclude (blacklist)
    exclude: HashSet<String>,
    /// Field renames (old -> new)
    renames: HashMap<String, String>,
}

impl ReplaceField {
    /// Create a new empty replacer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Include only these fields (whitelist).
    pub fn include<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.include = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Exclude these fields (blacklist).
    pub fn exclude<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.exclude = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Rename a field.
    pub fn rename(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.renames.insert(from.into(), to.into());
        self
    }

    /// Apply replacements to an object.
    fn replace_object(&self, obj: &mut Map<String, Value>) {
        // Apply renames first
        for (old, new) in &self.renames {
            if let Some(value) = obj.remove(old) {
                obj.insert(new.clone(), value);
            }
        }

        // Apply include filter
        if let Some(include) = &self.include {
            let keys: Vec<_> = obj.keys().cloned().collect();
            for key in keys {
                if !include.contains(&key) {
                    obj.remove(&key);
                }
            }
        }

        // Apply exclude filter
        for field in &self.exclude {
            obj.remove(field);
        }
    }
}

impl Smt for ReplaceField {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                self.replace_object(obj);
            }
        }

        if let Some(before) = &mut event.before {
            if let Some(obj) = before.as_object_mut() {
                self.replace_object(obj);
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "ReplaceField"
    }
}

// ============================================================================
// InsertField - Add fields with static or computed values
// ============================================================================

/// Field value source for insertion.
#[derive(Debug, Clone)]
pub enum InsertValue {
    /// Static value
    Static(Value),
    /// Current timestamp (ISO 8601)
    CurrentTimestamp,
    /// Current date (YYYY-MM-DD)
    CurrentDate,
    /// Copy from another field
    CopyFrom(String),
}

/// Insert fields with static or computed values.
#[derive(Debug, Clone)]
pub struct InsertField {
    /// Fields to insert
    fields: Vec<(String, InsertValue)>,
}

impl InsertField {
    /// Create a new inserter.
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Add a static field.
    pub fn static_field(mut self, name: impl Into<String>, value: Value) -> Self {
        self.fields.push((name.into(), InsertValue::Static(value)));
        self
    }

    /// Add current timestamp field.
    pub fn timestamp_field(mut self, name: impl Into<String>) -> Self {
        self.fields
            .push((name.into(), InsertValue::CurrentTimestamp));
        self
    }

    /// Add current date field.
    pub fn date_field(mut self, name: impl Into<String>) -> Self {
        self.fields.push((name.into(), InsertValue::CurrentDate));
        self
    }

    /// Copy field from another field.
    pub fn copy_field(mut self, name: impl Into<String>, source: impl Into<String>) -> Self {
        self.fields
            .push((name.into(), InsertValue::CopyFrom(source.into())));
        self
    }

    /// Get value for insertion.
    fn get_value(&self, source: &InsertValue, obj: &Map<String, Value>) -> Value {
        match source {
            InsertValue::Static(v) => v.clone(),
            InsertValue::CurrentTimestamp => Value::String(chrono::Utc::now().to_rfc3339()),
            InsertValue::CurrentDate => {
                Value::String(chrono::Utc::now().format("%Y-%m-%d").to_string())
            }
            InsertValue::CopyFrom(field) => obj.get(field).cloned().unwrap_or(Value::Null),
        }
    }
}

impl Default for InsertField {
    fn default() -> Self {
        Self::new()
    }
}

impl Smt for InsertField {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                // Clone obj for reading while we insert
                let obj_clone = obj.clone();
                for (name, source) in &self.fields {
                    let value = self.get_value(source, &obj_clone);
                    obj.insert(name.clone(), value);
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "InsertField"
    }
}

// ============================================================================
// Filter - Drop events based on conditions
// ============================================================================

/// Filter condition.
pub enum FilterCondition {
    /// Field equals value
    Equals { field: String, value: Value },
    /// Field not equals value
    NotEquals { field: String, value: Value },
    /// Field is null
    IsNull { field: String },
    /// Field is not null
    IsNotNull { field: String },
    /// Field matches regex
    Matches { field: String, pattern: String },
    /// Field in list
    In { field: String, values: Vec<Value> },
    /// Custom predicate
    Custom(Arc<dyn Fn(&CdcEvent) -> bool + Send + Sync>),
    /// All conditions must match
    And(Vec<FilterCondition>),
    /// Any condition must match
    Or(Vec<FilterCondition>),
    /// Negate condition
    Not(Box<FilterCondition>),
}

impl std::fmt::Debug for FilterCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Equals { field, value } => f
                .debug_struct("Equals")
                .field("field", field)
                .field("value", value)
                .finish(),
            Self::NotEquals { field, value } => f
                .debug_struct("NotEquals")
                .field("field", field)
                .field("value", value)
                .finish(),
            Self::IsNull { field } => f.debug_struct("IsNull").field("field", field).finish(),
            Self::IsNotNull { field } => f.debug_struct("IsNotNull").field("field", field).finish(),
            Self::Matches { field, pattern } => f
                .debug_struct("Matches")
                .field("field", field)
                .field("pattern", pattern)
                .finish(),
            Self::In { field, values } => f
                .debug_struct("In")
                .field("field", field)
                .field("values", values)
                .finish(),
            Self::Custom(_) => f.debug_struct("Custom").field("fn", &"<closure>").finish(),
            Self::And(conditions) => f
                .debug_struct("And")
                .field("conditions", conditions)
                .finish(),
            Self::Or(conditions) => f
                .debug_struct("Or")
                .field("conditions", conditions)
                .finish(),
            Self::Not(condition) => f.debug_struct("Not").field("condition", condition).finish(),
        }
    }
}

/// Filter events based on conditions.
pub struct Filter {
    /// Condition to match
    condition: FilterCondition,
    /// Whether to keep matching events (true) or drop them (false)
    keep_matching: bool,
}

impl Filter {
    /// Keep events matching the condition.
    pub fn keep(condition: FilterCondition) -> Self {
        Self {
            condition,
            keep_matching: true,
        }
    }

    /// Drop events matching the condition.
    pub fn drop(condition: FilterCondition) -> Self {
        Self {
            condition,
            keep_matching: false,
        }
    }

    /// Check if condition matches.
    fn matches(&self, event: &CdcEvent) -> bool {
        self.check_condition(&self.condition, event)
    }

    fn check_condition(&self, condition: &FilterCondition, event: &CdcEvent) -> bool {
        match condition {
            FilterCondition::Equals { field, value } => self
                .get_field_value(event, field)
                .map(|v| v == value)
                .unwrap_or(false),
            FilterCondition::NotEquals { field, value } => self
                .get_field_value(event, field)
                .map(|v| v != value)
                .unwrap_or(true),
            FilterCondition::IsNull { field } => self
                .get_field_value(event, field)
                .map(|v| v.is_null())
                .unwrap_or(true),
            FilterCondition::IsNotNull { field } => self
                .get_field_value(event, field)
                .map(|v| !v.is_null())
                .unwrap_or(false),
            FilterCondition::Matches { field, pattern } => {
                if let Ok(re) = Regex::new(pattern) {
                    self.get_field_value(event, field)
                        .and_then(|v| v.as_str().map(|s| re.is_match(s)))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            FilterCondition::In { field, values } => self
                .get_field_value(event, field)
                .map(|v| values.contains(v))
                .unwrap_or(false),
            FilterCondition::Custom(f) => f(event),
            FilterCondition::And(conditions) => {
                conditions.iter().all(|c| self.check_condition(c, event))
            }
            FilterCondition::Or(conditions) => {
                conditions.iter().any(|c| self.check_condition(c, event))
            }
            FilterCondition::Not(c) => !self.check_condition(c, event),
        }
    }

    fn get_field_value<'a>(&self, event: &'a CdcEvent, field: &str) -> Option<&'a Value> {
        event
            .after
            .as_ref()
            .or(event.before.as_ref())
            .and_then(|v| v.as_object())
            .and_then(|obj| obj.get(field))
    }
}

impl Smt for Filter {
    fn apply(&self, event: CdcEvent) -> Option<CdcEvent> {
        let matches = self.matches(&event);
        if (self.keep_matching && matches) || (!self.keep_matching && !matches) {
            Some(event)
        } else {
            None
        }
    }

    fn name(&self) -> &'static str {
        "Filter"
    }
}

// ============================================================================
// Cast - Convert field types
// ============================================================================

/// Target type for casting.
#[derive(Debug, Clone, Copy)]
pub enum CastType {
    String,
    Integer,
    Float,
    Boolean,
    Json,
}

/// Cast field values to different types.
#[derive(Debug, Clone)]
pub struct Cast {
    /// Field -> target type mappings
    casts: HashMap<String, CastType>,
}

impl Cast {
    /// Create a new caster.
    pub fn new() -> Self {
        Self {
            casts: HashMap::new(),
        }
    }

    /// Add a cast rule.
    pub fn field(mut self, name: impl Into<String>, to: CastType) -> Self {
        self.casts.insert(name.into(), to);
        self
    }

    /// Cast a value.
    fn cast_value(&self, value: &Value, to: CastType) -> Value {
        match to {
            CastType::String => match value {
                Value::String(s) => Value::String(s.clone()),
                v => Value::String(v.to_string()),
            },
            CastType::Integer => match value {
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::Number(i.into())
                    } else if let Some(f) = n.as_f64() {
                        Value::Number((f as i64).into())
                    } else {
                        value.clone()
                    }
                }
                Value::String(s) => s
                    .parse::<i64>()
                    .map(|i| Value::Number(i.into()))
                    .unwrap_or(Value::Null),
                Value::Bool(b) => Value::Number(if *b { 1 } else { 0 }.into()),
                _ => Value::Null,
            },
            CastType::Float => match value {
                Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        serde_json::Number::from_f64(f)
                            .map(Value::Number)
                            .unwrap_or(value.clone())
                    } else {
                        value.clone()
                    }
                }
                Value::String(s) => s
                    .parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            },
            CastType::Boolean => match value {
                Value::Bool(b) => Value::Bool(*b),
                Value::Number(n) => Value::Bool(n.as_i64().map(|i| i != 0).unwrap_or(false)),
                Value::String(s) => {
                    let lower = s.to_lowercase();
                    Value::Bool(lower == "true" || lower == "1" || lower == "yes")
                }
                _ => Value::Bool(false),
            },
            CastType::Json => match value {
                Value::String(s) => serde_json::from_str(s).unwrap_or(Value::String(s.clone())),
                v => v.clone(),
            },
        }
    }
}

impl Default for Cast {
    fn default() -> Self {
        Self::new()
    }
}

impl Smt for Cast {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                for (field, cast_type) in &self.casts {
                    if let Some(value) = obj.get(field).cloned() {
                        obj.insert(field.clone(), self.cast_value(&value, *cast_type));
                    }
                }
            }
        }

        if let Some(before) = &mut event.before {
            if let Some(obj) = before.as_object_mut() {
                for (field, cast_type) in &self.casts {
                    if let Some(value) = obj.get(field).cloned() {
                        obj.insert(field.clone(), self.cast_value(&value, *cast_type));
                    }
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "Cast"
    }
}

// ============================================================================
// Flatten - Flatten nested JSON structures
// ============================================================================

/// Flatten nested JSON structures.
#[derive(Debug, Clone)]
pub struct Flatten {
    /// Delimiter between nested keys
    delimiter: String,
    /// Maximum depth to flatten (0 = unlimited)
    max_depth: usize,
}

impl Default for Flatten {
    fn default() -> Self {
        Self::new()
    }
}

impl Flatten {
    /// Create a new flattener with default settings.
    pub fn new() -> Self {
        Self {
            delimiter: ".".to_string(),
            max_depth: 0,
        }
    }

    /// Set key delimiter.
    pub fn delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.delimiter = delimiter.into();
        self
    }

    /// Set maximum depth.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    /// Flatten a JSON object.
    fn flatten_object(&self, obj: &Map<String, Value>) -> Map<String, Value> {
        let mut result = Map::new();
        self.flatten_recursive(obj, "", &mut result, 0);
        result
    }

    fn flatten_recursive(
        &self,
        obj: &Map<String, Value>,
        prefix: &str,
        result: &mut Map<String, Value>,
        depth: usize,
    ) {
        for (key, value) in obj {
            let new_key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}{}{}", prefix, self.delimiter, key)
            };

            if self.max_depth > 0 && depth >= self.max_depth {
                result.insert(new_key, value.clone());
            } else if let Some(nested) = value.as_object() {
                self.flatten_recursive(nested, &new_key, result, depth + 1);
            } else {
                result.insert(new_key, value.clone());
            }
        }
    }
}

impl Smt for Flatten {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &event.after {
            if let Some(obj) = after.as_object() {
                event.after = Some(Value::Object(self.flatten_object(obj)));
            }
        }

        if let Some(before) = &event.before {
            if let Some(obj) = before.as_object() {
                event.before = Some(Value::Object(self.flatten_object(obj)));
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "Flatten"
    }
}

// ============================================================================
// RegexRouter - Route events to topics based on regex patterns
// ============================================================================

/// Route events to topics based on regex patterns.
/// Stores the target topic in the "after" data as "__topic" field.
#[derive(Debug, Clone)]
pub struct RegexRouter {
    /// Routing rules (pattern -> topic)
    rules: Vec<(Regex, String)>,
    /// Default topic if no rules match
    default_topic: String,
}

impl RegexRouter {
    /// Create a new router with a default topic.
    pub fn new(default_topic: impl Into<String>) -> Self {
        Self {
            rules: Vec::new(),
            default_topic: default_topic.into(),
        }
    }

    /// Add a routing rule.
    pub fn route(mut self, pattern: &str, topic: impl Into<String>) -> Self {
        if let Ok(re) = Regex::new(pattern) {
            self.rules.push((re, topic.into()));
        } else {
            warn!("Invalid regex pattern: {}", pattern);
        }
        self
    }

    /// Get the target topic for a table.
    pub fn get_topic(&self, schema: &str, table: &str) -> &str {
        let full_name = format!("{}.{}", schema, table);

        for (pattern, topic) in &self.rules {
            if pattern.is_match(&full_name) {
                return topic;
            }
        }

        &self.default_topic
    }
}

impl Smt for RegexRouter {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        let topic = self.get_topic(&event.schema, &event.table);
        // Store topic in after as metadata (can be used by downstream)
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                obj.insert("__topic".to_string(), Value::String(topic.to_string()));
            }
        }
        Some(event)
    }

    fn name(&self) -> &'static str {
        "RegexRouter"
    }
}

// ============================================================================
// Predicate - Conditional transform application
// ============================================================================

/// Predicate for conditionally applying transforms.
///
/// Debezium-style predicates allow transforms to be applied only when
/// certain conditions are met.
pub enum Predicate {
    /// Apply only to specific tables
    Table { pattern: Regex },
    /// Apply only to specific schemas
    Schema { pattern: Regex },
    /// Apply only to specific operations
    Operation { ops: Vec<CdcOp> },
    /// Apply based on field value
    FieldValue { field: String, value: Value },
    /// Apply based on field existence
    FieldExists { field: String },
    /// Custom predicate function
    Custom(Arc<dyn Fn(&CdcEvent) -> bool + Send + Sync>),
    /// Combine predicates with AND
    And(Vec<Predicate>),
    /// Combine predicates with OR
    Or(Vec<Predicate>),
    /// Negate a predicate
    Not(Box<Predicate>),
}

impl Predicate {
    /// Create a table pattern predicate.
    pub fn table(pattern: &str) -> Option<Self> {
        Regex::new(pattern)
            .ok()
            .map(|re| Predicate::Table { pattern: re })
    }

    /// Create a schema pattern predicate.
    pub fn schema(pattern: &str) -> Option<Self> {
        Regex::new(pattern)
            .ok()
            .map(|re| Predicate::Schema { pattern: re })
    }

    /// Create an operation predicate.
    pub fn operation(ops: Vec<CdcOp>) -> Self {
        Predicate::Operation { ops }
    }

    /// Create a field value predicate.
    pub fn field_equals(field: impl Into<String>, value: Value) -> Self {
        Predicate::FieldValue {
            field: field.into(),
            value,
        }
    }

    /// Create a field exists predicate.
    pub fn field_exists(field: impl Into<String>) -> Self {
        Predicate::FieldExists {
            field: field.into(),
        }
    }

    /// Check if the predicate matches the event.
    pub fn matches(&self, event: &CdcEvent) -> bool {
        match self {
            Predicate::Table { pattern } => pattern.is_match(&event.table),
            Predicate::Schema { pattern } => pattern.is_match(&event.schema),
            Predicate::Operation { ops } => ops.contains(&event.op),
            Predicate::FieldValue { field, value } => event
                .after
                .as_ref()
                .or(event.before.as_ref())
                .and_then(|v| v.as_object())
                .and_then(|obj| obj.get(field))
                .map(|v| v == value)
                .unwrap_or(false),
            Predicate::FieldExists { field } => event
                .after
                .as_ref()
                .or(event.before.as_ref())
                .and_then(|v| v.as_object())
                .map(|obj| obj.contains_key(field))
                .unwrap_or(false),
            Predicate::Custom(f) => f(event),
            Predicate::And(predicates) => predicates.iter().all(|p| p.matches(event)),
            Predicate::Or(predicates) => predicates.iter().any(|p| p.matches(event)),
            Predicate::Not(p) => !p.matches(event),
        }
    }
}

impl std::fmt::Debug for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table { pattern } => f
                .debug_struct("Table")
                .field("pattern", &pattern.as_str())
                .finish(),
            Self::Schema { pattern } => f
                .debug_struct("Schema")
                .field("pattern", &pattern.as_str())
                .finish(),
            Self::Operation { ops } => f.debug_struct("Operation").field("ops", ops).finish(),
            Self::FieldValue { field, value } => f
                .debug_struct("FieldValue")
                .field("field", field)
                .field("value", value)
                .finish(),
            Self::FieldExists { field } => {
                f.debug_struct("FieldExists").field("field", field).finish()
            }
            Self::Custom(_) => f.debug_struct("Custom").field("fn", &"<closure>").finish(),
            Self::And(predicates) => f
                .debug_struct("And")
                .field("predicates", predicates)
                .finish(),
            Self::Or(predicates) => f
                .debug_struct("Or")
                .field("predicates", predicates)
                .finish(),
            Self::Not(predicate) => f.debug_struct("Not").field("predicate", predicate).finish(),
        }
    }
}

/// Wrapper to apply a transform only when a predicate matches.
pub struct ConditionalSmt {
    /// The transform to apply
    transform: Arc<dyn Smt>,
    /// The predicate that must match
    predicate: Predicate,
    /// Whether to negate (apply when predicate doesn't match)
    negate: bool,
}

impl ConditionalSmt {
    /// Apply transform only when predicate matches.
    pub fn when<T: Smt + 'static>(predicate: Predicate, transform: T) -> Self {
        Self {
            transform: Arc::new(transform),
            predicate,
            negate: false,
        }
    }

    /// Apply transform only when predicate does NOT match.
    pub fn unless<T: Smt + 'static>(predicate: Predicate, transform: T) -> Self {
        Self {
            transform: Arc::new(transform),
            predicate,
            negate: true,
        }
    }
}

impl Smt for ConditionalSmt {
    fn apply(&self, event: CdcEvent) -> Option<CdcEvent> {
        let matches = self.predicate.matches(&event);
        let should_apply = if self.negate { !matches } else { matches };

        if should_apply {
            self.transform.apply(event)
        } else {
            Some(event)
        }
    }

    fn name(&self) -> &'static str {
        "ConditionalSmt"
    }
}

// ============================================================================
// HeaderToValue - Move headers/metadata into record value
// ============================================================================

/// Move or copy envelope fields into the record value.
#[derive(Debug, Clone)]
pub struct HeaderToValue {
    /// Fields to insert: (target_field_name, source)
    fields: Vec<(String, HeaderSource)>,
    /// Operation mode
    mode: HeaderMode,
}

/// Source of header value.
#[derive(Debug, Clone)]
pub enum HeaderSource {
    /// Source database type (postgres, mysql)
    SourceType,
    /// Database name
    Database,
    /// Schema name
    Schema,
    /// Table name
    Table,
    /// Operation code
    Operation,
    /// Event timestamp
    Timestamp,
    /// Transaction ID (if available)
    TransactionId,
}

/// Mode for header operation.
#[derive(Debug, Clone, Copy, Default)]
pub enum HeaderMode {
    /// Copy values (keep in both places)
    #[default]
    Copy,
    /// Move values (only in target)
    Move,
}

impl HeaderToValue {
    /// Create a new HeaderToValue transform.
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            mode: HeaderMode::Copy,
        }
    }

    /// Add a field to copy/move.
    pub fn field(mut self, target: impl Into<String>, source: HeaderSource) -> Self {
        self.fields.push((target.into(), source));
        self
    }

    /// Set mode to move (not copy).
    pub fn move_mode(mut self) -> Self {
        self.mode = HeaderMode::Move;
        self
    }

    /// Add all standard fields with prefixed names.
    pub fn all_headers(self, prefix: &str) -> Self {
        self.field(format!("{}source_type", prefix), HeaderSource::SourceType)
            .field(format!("{}database", prefix), HeaderSource::Database)
            .field(format!("{}schema", prefix), HeaderSource::Schema)
            .field(format!("{}table", prefix), HeaderSource::Table)
            .field(format!("{}op", prefix), HeaderSource::Operation)
            .field(format!("{}ts", prefix), HeaderSource::Timestamp)
    }
}

impl Default for HeaderToValue {
    fn default() -> Self {
        Self::new()
    }
}

impl Smt for HeaderToValue {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                for (target, source) in &self.fields {
                    let value = match source {
                        HeaderSource::SourceType => Value::String(event.source_type.clone()),
                        HeaderSource::Database => Value::String(event.database.clone()),
                        HeaderSource::Schema => Value::String(event.schema.clone()),
                        HeaderSource::Table => Value::String(event.table.clone()),
                        HeaderSource::Operation => Value::String(op_to_code(&event.op).to_string()),
                        HeaderSource::Timestamp => Value::Number(event.timestamp.into()),
                        HeaderSource::TransactionId => event
                            .transaction
                            .as_ref()
                            .map(|t| Value::String(t.id.clone()))
                            .unwrap_or(Value::Null),
                    };
                    obj.insert(target.clone(), value);
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "HeaderToValue"
    }
}

// ============================================================================
// Unwrap - Extract nested field to top level
// ============================================================================

/// Unwrap/extract a nested JSON field to the top level.
#[derive(Debug, Clone)]
pub struct Unwrap {
    /// The field path to unwrap (e.g., "payload.data")
    field_path: Vec<String>,
    /// Whether to replace the entire value or merge
    replace: bool,
}

impl Unwrap {
    /// Create a new Unwrap transform.
    pub fn new(path: impl Into<String>) -> Self {
        let path_str = path.into();
        Self {
            field_path: path_str.split('.').map(String::from).collect(),
            replace: true,
        }
    }

    /// Merge unwrapped fields instead of replacing.
    pub fn merge(mut self) -> Self {
        self.replace = false;
        self
    }

    /// Extract value at path from a JSON value.
    fn extract_at_path<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut current = value;
        for key in &self.field_path {
            current = current.as_object()?.get(key)?;
        }
        Some(current)
    }
}

impl Smt for Unwrap {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &event.after {
            if let Some(extracted) = self.extract_at_path(after) {
                let extracted_clone = extracted.clone();
                if self.replace {
                    event.after = Some(extracted_clone);
                } else if let Some(extracted_obj) = extracted_clone.as_object() {
                    if let Some(obj) = event.after.as_mut().and_then(|v| v.as_object_mut()) {
                        for (k, v) in extracted_obj {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "Unwrap"
    }
}

// ============================================================================
// SetNull - Set fields to null based on condition
// ============================================================================

/// Set fields to null based on conditions.
#[derive(Debug, Clone)]
pub struct SetNull {
    /// Fields to potentially nullify
    fields: Vec<String>,
    /// Condition for nullification
    condition: NullCondition,
}

/// Condition for setting null.
#[derive(Debug, Clone)]
pub enum NullCondition {
    /// Always set to null
    Always,
    /// Set to null if empty string
    IfEmpty,
    /// Set to null if matches value
    IfEquals(Value),
    /// Set to null if matches pattern
    IfMatches(String),
}

impl SetNull {
    /// Create a new SetNull transform.
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fields: fields.into_iter().map(Into::into).collect(),
            condition: NullCondition::Always,
        }
    }

    /// Set condition for nullification.
    pub fn when(mut self, condition: NullCondition) -> Self {
        self.condition = condition;
        self
    }

    /// Check if value should be nullified.
    fn should_nullify(&self, value: &Value) -> bool {
        match &self.condition {
            NullCondition::Always => true,
            NullCondition::IfEmpty => value.as_str().map(|s| s.is_empty()).unwrap_or(false),
            NullCondition::IfEquals(target) => value == target,
            NullCondition::IfMatches(pattern) => {
                if let (Ok(re), Some(s)) = (Regex::new(pattern), value.as_str()) {
                    re.is_match(s)
                } else {
                    false
                }
            }
        }
    }
}

impl Smt for SetNull {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                for field in &self.fields {
                    if let Some(value) = obj.get(field).cloned() {
                        if self.should_nullify(&value) {
                            obj.insert(field.clone(), Value::Null);
                        }
                    }
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "SetNull"
    }
}

// ============================================================================
// TimezoneConverter - Convert timestamp fields between timezones
// ============================================================================

/// Convert timestamp fields from one timezone to another.
///
/// Debezium-compatible timezone conversion SMT.
///
/// # Example
/// ```rust,ignore
/// // Convert from UTC to EST
/// let smt = TimezoneConverter::new(["created_at", "updated_at"])
///     .from("UTC")
///     .to("America/New_York");
/// ```
#[derive(Clone)]
pub struct TimezoneConverter {
    /// Fields to convert
    fields: Vec<String>,
    /// Source timezone (default: UTC)
    source_tz: chrono_tz::Tz,
    /// Target timezone
    target_tz: chrono_tz::Tz,
    /// Include time component in output
    include_time: bool,
    /// Output format override (uses ISO8601 if None)
    format: Option<String>,
}

impl std::fmt::Debug for TimezoneConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimezoneConverter")
            .field("fields", &self.fields)
            .field("source_tz", &self.source_tz.to_string())
            .field("target_tz", &self.target_tz.to_string())
            .field("include_time", &self.include_time)
            .field("format", &self.format)
            .finish()
    }
}

impl TimezoneConverter {
    /// Create a new timezone converter for specific fields.
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fields: fields.into_iter().map(Into::into).collect(),
            source_tz: chrono_tz::UTC,
            target_tz: chrono_tz::UTC,
            include_time: true,
            format: None,
        }
    }

    /// Set source timezone.
    pub fn from(mut self, tz: &str) -> Self {
        if let Ok(parsed) = tz.parse::<chrono_tz::Tz>() {
            self.source_tz = parsed;
        } else {
            warn!("Invalid source timezone '{}', using UTC", tz);
        }
        self
    }

    /// Set target timezone.
    pub fn to(mut self, tz: &str) -> Self {
        if let Ok(parsed) = tz.parse::<chrono_tz::Tz>() {
            self.target_tz = parsed;
        } else {
            warn!("Invalid target timezone '{}', using UTC", tz);
        }
        self
    }

    /// Exclude time component (date only).
    pub fn date_only(mut self) -> Self {
        self.include_time = false;
        self
    }

    /// Set custom output format (strftime).
    pub fn format(mut self, fmt: impl Into<String>) -> Self {
        self.format = Some(fmt.into());
        self
    }

    /// Convert a timestamp value.
    fn convert_value(&self, value: &Value) -> Option<Value> {
        let dt = self.parse_timestamp(value)?;

        // Convert from source to target timezone
        let converted = dt.with_timezone(&self.target_tz);

        let result = if let Some(ref fmt) = self.format {
            converted.format(fmt).to_string()
        } else if self.include_time {
            converted.to_rfc3339()
        } else {
            converted.format("%Y-%m-%d").to_string()
        };

        Some(Value::String(result))
    }

    /// Parse various timestamp formats.
    fn parse_timestamp(&self, value: &Value) -> Option<DateTime<chrono_tz::Tz>> {
        match value {
            Value::String(s) => {
                // Try ISO 8601 with timezone
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    return Some(dt.with_timezone(&self.source_tz));
                }
                // Try without timezone (assume source_tz)
                if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return self.source_tz.from_local_datetime(&naive).single();
                }
                if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    return self.source_tz.from_local_datetime(&naive).single();
                }
                // Date only
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let naive = date.and_hms_opt(0, 0, 0)?;
                    return self.source_tz.from_local_datetime(&naive).single();
                }
                None
            }
            Value::Number(n) => {
                let ts = n.as_i64()?;
                let dt = if ts > 1_000_000_000_000 {
                    DateTime::from_timestamp_millis(ts)?
                } else {
                    DateTime::from_timestamp(ts, 0)?
                };
                Some(dt.with_timezone(&self.source_tz))
            }
            _ => None,
        }
    }
}

impl Smt for TimezoneConverter {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        // Convert in "after"
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                for field in &self.fields {
                    if let Some(value) = obj.get(field).cloned() {
                        if let Some(converted) = self.convert_value(&value) {
                            obj.insert(field.clone(), converted);
                        }
                    }
                }
            }
        }

        // Convert in "before"
        if let Some(before) = &mut event.before {
            if let Some(obj) = before.as_object_mut() {
                for field in &self.fields {
                    if let Some(value) = obj.get(field).cloned() {
                        if let Some(converted) = self.convert_value(&value) {
                            obj.insert(field.clone(), converted);
                        }
                    }
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "TimezoneConverter"
    }
}

// ============================================================================
// ContentRouter - Route events based on field content
// ============================================================================

/// Content-based routing SMT.
///
/// Routes events to different topics based on field values.
/// Adds `__routing_topic` field to the event for downstream processing.
///
/// # Example
/// ```rust,ignore
/// let router = ContentRouter::new()
///     .route("priority", "high", "priority-events")
///     .route("priority", "low", "batch-events")
///     .route_pattern("category", r"^urgent", "urgent-events")
///     .default_topic("default-events");
/// ```
#[derive(Clone)]
pub struct ContentRouter {
    /// Field-value to topic mappings
    routes: Vec<ContentRoute>,
    /// Default topic if no match
    default_topic: Option<String>,
    /// Topic field name to set
    topic_field: String,
}

/// A single content-based route.
#[derive(Clone)]
struct ContentRoute {
    field: String,
    matcher: RouteMatcher,
    topic: String,
}

/// Route matching strategy.
#[derive(Clone)]
enum RouteMatcher {
    /// Exact value match
    Exact(Value),
    /// Regex pattern match
    Pattern(Regex),
    /// Value in set
    In(HashSet<String>),
    /// Custom predicate
    Predicate(Arc<dyn Fn(&Value) -> bool + Send + Sync>),
}

impl std::fmt::Debug for ContentRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentRouter")
            .field("routes_count", &self.routes.len())
            .field("default_topic", &self.default_topic)
            .field("topic_field", &self.topic_field)
            .finish()
    }
}

impl Default for ContentRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentRouter {
    /// Create a new content router.
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            default_topic: None,
            topic_field: "__routing_topic".to_string(),
        }
    }

    /// Add exact value route.
    pub fn route(
        mut self,
        field: impl Into<String>,
        value: impl Into<Value>,
        topic: impl Into<String>,
    ) -> Self {
        self.routes.push(ContentRoute {
            field: field.into(),
            matcher: RouteMatcher::Exact(value.into()),
            topic: topic.into(),
        });
        self
    }

    /// Add pattern-based route.
    pub fn route_pattern(
        mut self,
        field: impl Into<String>,
        pattern: &str,
        topic: impl Into<String>,
    ) -> Self {
        if let Ok(re) = Regex::new(pattern) {
            self.routes.push(ContentRoute {
                field: field.into(),
                matcher: RouteMatcher::Pattern(re),
                topic: topic.into(),
            });
        }
        self
    }

    /// Add set membership route.
    pub fn route_in<I, S>(
        mut self,
        field: impl Into<String>,
        values: I,
        topic: impl Into<String>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.routes.push(ContentRoute {
            field: field.into(),
            matcher: RouteMatcher::In(values.into_iter().map(Into::into).collect()),
            topic: topic.into(),
        });
        self
    }

    /// Add predicate-based route.
    pub fn route_if<F>(
        mut self,
        field: impl Into<String>,
        predicate: F,
        topic: impl Into<String>,
    ) -> Self
    where
        F: Fn(&Value) -> bool + Send + Sync + 'static,
    {
        self.routes.push(ContentRoute {
            field: field.into(),
            matcher: RouteMatcher::Predicate(Arc::new(predicate)),
            topic: topic.into(),
        });
        self
    }

    /// Set default topic.
    pub fn default_topic(mut self, topic: impl Into<String>) -> Self {
        self.default_topic = Some(topic.into());
        self
    }

    /// Set the field name for routing topic.
    pub fn topic_field(mut self, field: impl Into<String>) -> Self {
        self.topic_field = field.into();
        self
    }

    /// Find matching topic for event.
    fn find_topic(&self, event: &CdcEvent) -> Option<&str> {
        let after = event.after.as_ref()?;
        let obj = after.as_object()?;

        for route in &self.routes {
            if let Some(value) = obj.get(&route.field) {
                let matches = match &route.matcher {
                    RouteMatcher::Exact(expected) => value == expected,
                    RouteMatcher::Pattern(re) => {
                        value.as_str().map(|s| re.is_match(s)).unwrap_or(false)
                    }
                    RouteMatcher::In(set) => {
                        value.as_str().map(|s| set.contains(s)).unwrap_or(false)
                    }
                    RouteMatcher::Predicate(f) => f(value),
                };

                if matches {
                    return Some(&route.topic);
                }
            }
        }

        self.default_topic.as_deref()
    }
}

impl Smt for ContentRouter {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(topic) = self.find_topic(&event) {
            if let Some(after) = &mut event.after {
                if let Some(obj) = after.as_object_mut() {
                    obj.insert(self.topic_field.clone(), Value::String(topic.to_string()));
                }
            }
        }
        Some(event)
    }

    fn name(&self) -> &'static str {
        "ContentRouter"
    }
}

// ============================================================================
// ComputeField - Compute new fields from expressions
// ============================================================================

/// Compute fields from expressions.
///
/// Supports various computation types:
/// - String concatenation
/// - Numeric operations
/// - JSON path extraction
/// - Hash computation
/// - Timestamp operations
///
/// # Example
/// ```rust,ignore
/// let smt = ComputeField::new()
///     .concat("full_name", ["first_name", " ", "last_name"])
///     .hash("user_hash", ["id", "email"])
///     .coalesce("display_name", ["nickname", "username", "email"]);
/// ```
#[derive(Clone)]
pub struct ComputeField {
    /// Computations to apply
    computations: Vec<FieldComputation>,
}

/// A single field computation.
#[derive(Clone)]
struct FieldComputation {
    target: String,
    operation: ComputeOp,
}

/// Computation operation.
#[derive(Clone)]
enum ComputeOp {
    /// Concatenate fields/literals
    Concat(Vec<ConcatPart>),
    /// Hash multiple fields (SHA-256)
    Hash(Vec<String>),
    /// First non-null value
    Coalesce(Vec<String>),
    /// Numeric sum
    Sum(Vec<String>),
    /// String length
    Length(String),
    /// Uppercase
    Upper(String),
    /// Lowercase
    Lower(String),
    /// Substring
    Substring(String, usize, Option<usize>),
    /// Replace pattern
    Replace(String, String, String),
    /// Current timestamp
    CurrentTimestamp,
    /// UUID v4
    Uuid,
    /// JSON path extraction
    JsonPath(String, String),
    /// Conditional value
    Conditional(ComputeCondition, Value, Value),
}

/// Part of a concatenation.
#[derive(Clone)]
enum ConcatPart {
    Field(String),
    Literal(String),
}

/// Condition for conditional compute.
#[derive(Clone)]
pub enum ComputeCondition {
    /// Field equals value
    FieldEquals(String, Value),
    /// Field is null
    FieldIsNull(String),
    /// Field matches pattern
    FieldMatches(String, String),
}

impl std::fmt::Debug for ComputeField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeField")
            .field("computations_count", &self.computations.len())
            .finish()
    }
}

impl Default for ComputeField {
    fn default() -> Self {
        Self::new()
    }
}

impl ComputeField {
    /// Create a new compute field transform.
    pub fn new() -> Self {
        Self {
            computations: Vec::new(),
        }
    }

    /// Concatenate fields and literals.
    pub fn concat<I>(mut self, target: impl Into<String>, parts: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let parts: Vec<ConcatPart> = parts
            .into_iter()
            .map(|p| {
                let s: String = p.into();
                // If starts with $, it's a field reference
                if let Some(field_name) = s.strip_prefix('$') {
                    ConcatPart::Field(field_name.to_string())
                } else {
                    ConcatPart::Literal(s)
                }
            })
            .collect();

        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Concat(parts),
        });
        self
    }

    /// Hash multiple fields together (SHA-256).
    pub fn hash<I, S>(mut self, target: impl Into<String>, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Hash(fields.into_iter().map(Into::into).collect()),
        });
        self
    }

    /// First non-null value from fields.
    pub fn coalesce<I, S>(mut self, target: impl Into<String>, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Coalesce(fields.into_iter().map(Into::into).collect()),
        });
        self
    }

    /// Sum numeric fields.
    pub fn sum<I, S>(mut self, target: impl Into<String>, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Sum(fields.into_iter().map(Into::into).collect()),
        });
        self
    }

    /// String length of field.
    pub fn length(mut self, target: impl Into<String>, field: impl Into<String>) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Length(field.into()),
        });
        self
    }

    /// Uppercase field value.
    pub fn upper(mut self, target: impl Into<String>, field: impl Into<String>) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Upper(field.into()),
        });
        self
    }

    /// Lowercase field value.
    pub fn lower(mut self, target: impl Into<String>, field: impl Into<String>) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Lower(field.into()),
        });
        self
    }

    /// Substring of field.
    pub fn substring(
        mut self,
        target: impl Into<String>,
        field: impl Into<String>,
        start: usize,
        len: Option<usize>,
    ) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Substring(field.into(), start, len),
        });
        self
    }

    /// Replace pattern in field.
    pub fn replace(
        mut self,
        target: impl Into<String>,
        field: impl Into<String>,
        pattern: impl Into<String>,
        replacement: impl Into<String>,
    ) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Replace(field.into(), pattern.into(), replacement.into()),
        });
        self
    }

    /// Add current timestamp.
    pub fn current_timestamp(mut self, target: impl Into<String>) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::CurrentTimestamp,
        });
        self
    }

    /// Add UUID v4.
    pub fn uuid(mut self, target: impl Into<String>) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Uuid,
        });
        self
    }

    /// Extract value using JSON path.
    pub fn json_path(
        mut self,
        target: impl Into<String>,
        field: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::JsonPath(field.into(), path.into()),
        });
        self
    }

    /// Conditional value.
    pub fn conditional(
        mut self,
        target: impl Into<String>,
        condition: ComputeCondition,
        then_value: impl Into<Value>,
        else_value: impl Into<Value>,
    ) -> Self {
        self.computations.push(FieldComputation {
            target: target.into(),
            operation: ComputeOp::Conditional(condition, then_value.into(), else_value.into()),
        });
        self
    }

    /// Execute a computation.
    fn compute(&self, op: &ComputeOp, obj: &Map<String, Value>) -> Option<Value> {
        match op {
            ComputeOp::Concat(parts) => {
                let mut result = String::new();
                for part in parts {
                    match part {
                        ConcatPart::Literal(s) => result.push_str(s),
                        ConcatPart::Field(f) => {
                            if let Some(v) = obj.get(f) {
                                match v {
                                    Value::String(s) => result.push_str(s),
                                    Value::Number(n) => result.push_str(&n.to_string()),
                                    Value::Bool(b) => result.push_str(&b.to_string()),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Some(Value::String(result))
            }
            ComputeOp::Hash(fields) => {
                use ring::digest::{digest, SHA256};
                let mut data = Vec::new();
                for field in fields {
                    if let Some(v) = obj.get(field) {
                        data.extend_from_slice(
                            serde_json::to_string(v).unwrap_or_default().as_bytes(),
                        );
                    }
                }
                let hash = digest(&SHA256, &data);
                Some(Value::String(hex::encode(hash.as_ref())))
            }
            ComputeOp::Coalesce(fields) => {
                for field in fields {
                    if let Some(v) = obj.get(field) {
                        if !v.is_null() {
                            return Some(v.clone());
                        }
                    }
                }
                None
            }
            ComputeOp::Sum(fields) => {
                let mut sum = 0.0;
                for field in fields {
                    if let Some(v) = obj.get(field) {
                        if let Some(n) = v.as_f64() {
                            sum += n;
                        }
                    }
                }
                Some(Value::from(sum))
            }
            ComputeOp::Length(field) => obj
                .get(field)
                .and_then(|v| v.as_str())
                .map(|s| Value::from(s.len() as i64)),
            ComputeOp::Upper(field) => obj
                .get(field)
                .and_then(|v| v.as_str())
                .map(|s| Value::String(s.to_uppercase())),
            ComputeOp::Lower(field) => obj
                .get(field)
                .and_then(|v| v.as_str())
                .map(|s| Value::String(s.to_lowercase())),
            ComputeOp::Substring(field, start, len) => {
                obj.get(field).and_then(|v| v.as_str()).map(|s| {
                    let chars: Vec<char> = s.chars().collect();
                    let end = len
                        .map(|l| (*start + l).min(chars.len()))
                        .unwrap_or(chars.len());
                    let substr: String = chars[*start.min(&chars.len())..end].iter().collect();
                    Value::String(substr)
                })
            }
            ComputeOp::Replace(field, pattern, replacement) => {
                if let Ok(re) = Regex::new(pattern) {
                    obj.get(field)
                        .and_then(|v| v.as_str())
                        .map(|s| Value::String(re.replace_all(s, replacement.as_str()).to_string()))
                } else {
                    None
                }
            }
            ComputeOp::CurrentTimestamp => Some(Value::String(Utc::now().to_rfc3339())),
            ComputeOp::Uuid => {
                // Generate a simple UUID v4
                use ring::rand::{SecureRandom, SystemRandom};
                let rng = SystemRandom::new();
                let mut bytes = [0u8; 16];
                if rng.fill(&mut bytes).is_ok() {
                    // Set version and variant
                    bytes[6] = (bytes[6] & 0x0f) | 0x40; // Version 4
                    bytes[8] = (bytes[8] & 0x3f) | 0x80; // Variant
                    Some(Value::String(format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        bytes[0], bytes[1], bytes[2], bytes[3],
                        bytes[4], bytes[5],
                        bytes[6], bytes[7],
                        bytes[8], bytes[9],
                        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
                    )))
                } else {
                    None
                }
            }
            ComputeOp::JsonPath(field, path) => {
                // Simple JSON path: "nested.field.value"
                obj.get(field).and_then(|v| {
                    let parts: Vec<&str> = path.split('.').collect();
                    let mut current = v;
                    for part in parts {
                        current = current.get(part)?;
                    }
                    Some(current.clone())
                })
            }
            ComputeOp::Conditional(cond, then_val, else_val) => {
                let matches = match cond {
                    ComputeCondition::FieldEquals(f, v) => {
                        obj.get(f).map(|fv| fv == v).unwrap_or(false)
                    }
                    ComputeCondition::FieldIsNull(f) => {
                        obj.get(f).map(|v| v.is_null()).unwrap_or(true)
                    }
                    ComputeCondition::FieldMatches(f, pattern) => {
                        if let (Some(v), Ok(re)) = (obj.get(f), Regex::new(pattern)) {
                            v.as_str().map(|s| re.is_match(s)).unwrap_or(false)
                        } else {
                            false
                        }
                    }
                };
                Some(if matches {
                    then_val.clone()
                } else {
                    else_val.clone()
                })
            }
        }
    }
}

impl Smt for ComputeField {
    fn apply(&self, mut event: CdcEvent) -> Option<CdcEvent> {
        if let Some(after) = &mut event.after {
            if let Some(obj) = after.as_object_mut() {
                for comp in &self.computations {
                    // Clone the object for read-only access
                    let obj_clone = obj.clone();
                    if let Some(value) = self.compute(&comp.operation, &obj_clone) {
                        obj.insert(comp.target.clone(), value);
                    }
                }
            }
        }

        Some(event)
    }

    fn name(&self) -> &'static str {
        "ComputeField"
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(op: CdcOp, before: Option<Value>, after: Option<Value>) -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op,
            before,
            after,
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    // SMT Chain tests
    #[test]
    fn test_smt_chain_empty() {
        let chain = SmtChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
    }

    #[test]
    fn test_smt_chain_apply() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "email": "alice@test.com"})),
        );

        let chain = SmtChain::new()
            .add(MaskField::new(["email"]))
            .add(InsertField::new().static_field("version", json!(1)));

        let result = chain.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name"], "Alice");
        assert_ne!(after["email"], "alice@test.com"); // Masked
        assert_eq!(after["version"], 1);
    }

    #[test]
    fn test_smt_chain_names() {
        let chain = SmtChain::new()
            .add(MaskField::new(["email"]))
            .add(Flatten::new());

        let names = chain.names();
        assert_eq!(names, vec!["MaskField", "Flatten"]);
    }

    // ExtractNewRecordState tests
    #[test]
    fn test_extract_new_record_state() {
        let event = make_event(
            CdcOp::Update,
            Some(json!({"id": 1, "name": "Old"})),
            Some(json!({"id": 1, "name": "New"})),
        );

        let smt = ExtractNewRecordState::new()
            .add_op_field()
            .add_table_field();

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__op"], "u");
        assert_eq!(after["__table"], "users");
    }

    #[test]
    fn test_extract_drop_tombstones() {
        let event = make_event(CdcOp::Delete, Some(json!({"id": 1})), None);

        let smt = ExtractNewRecordState::new().drop_tombstones();
        assert!(smt.apply(event).is_none());
    }

    #[test]
    fn test_extract_rewrite_deletes() {
        let event = make_event(
            CdcOp::Delete,
            Some(json!({"id": 1, "name": "Deleted"})),
            None,
        );

        let smt = ExtractNewRecordState::new().delete_handling(DeleteHandling::Rewrite);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__deleted"], true);
        assert_eq!(after["id"], 1);
    }

    #[test]
    fn test_extract_custom_prefix() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));

        let smt = ExtractNewRecordState::new()
            .header_prefix("_cdc_")
            .add_op_field()
            .add_schema_field();

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["_cdc_op"], "c");
        assert_eq!(after["_cdc_schema"], "public");
    }

    // ValueToKey tests
    #[test]
    fn test_value_to_key() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"id": 42, "name": "Alice", "email": "alice@test.com"})),
        );

        let smt = ValueToKey::with_fields(["id"]);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__key"], json!({"id": 42}));
    }

    #[test]
    fn test_value_to_key_multiple_fields() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"org_id": 1, "user_id": 2, "name": "Alice"})),
        );

        let smt = ValueToKey::with_fields(["org_id", "user_id"]);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__key"], json!({"org_id": 1, "user_id": 2}));
    }

    // TimestampConverter tests
    #[test]
    fn test_timestamp_converter_to_iso() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": 1705320000000i64})), // 2024-01-15T10:00:00Z in millis
        );

        let smt = TimestampConverter::new(["created_at"], TimestampFormat::Iso8601);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after["created_at"].as_str().unwrap().contains("2024-01-15"));
    }

    #[test]
    fn test_timestamp_converter_to_epoch() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": "2024-01-15T10:00:00Z"})),
        );

        let smt = TimestampConverter::new(["created_at"], TimestampFormat::EpochMillis);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after["created_at"].is_number());
    }

    #[test]
    fn test_timestamp_converter_date_only() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": 1705320000000i64})),
        );

        let smt = TimestampConverter::new(["created_at"], TimestampFormat::DateOnly);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["created_at"], "2024-01-15");
    }

    // MaskField tests
    #[test]
    fn test_mask_field_asterisks() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "ssn": "123-45-6789"})),
        );

        let smt = MaskField::new(["ssn"]);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name"], "Alice");
        assert_eq!(after["ssn"], "***********");
    }

    #[test]
    fn test_mask_field_partial() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"card": "4111111111111111"})),
        );

        let smt = MaskField::new(["card"]).with_strategy(MaskStrategy::PartialMask {
            keep_first: 4,
            keep_last: 4,
        });

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["card"], "4111********1111");
    }

    #[test]
    fn test_mask_field_hash() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"email": "alice@test.com"})),
        );

        let smt = MaskField::new(["email"]).with_strategy(MaskStrategy::Hash);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // Should be a hex string
        assert!(after["email"]
            .as_str()
            .unwrap()
            .chars()
            .all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_mask_field_redact() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "password": "secret"})),
        );

        let smt = MaskField::new(["password"]).with_strategy(MaskStrategy::Redact);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(!after.as_object().unwrap().contains_key("password"));
    }

    #[test]
    fn test_mask_field_fixed() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"email": "alice@test.com"})),
        );

        let smt =
            MaskField::new(["email"]).with_strategy(MaskStrategy::Fixed("[REDACTED]".to_string()));
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["email"], "[REDACTED]");
    }

    // ReplaceField tests
    #[test]
    fn test_replace_field_rename() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"first_name": "Alice", "last_name": "Smith"})),
        );

        let smt = ReplaceField::new()
            .rename("first_name", "firstName")
            .rename("last_name", "lastName");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["firstName"], "Alice");
        assert_eq!(after["lastName"], "Smith");
        assert!(!after.as_object().unwrap().contains_key("first_name"));
    }

    #[test]
    fn test_replace_field_include() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"id": 1, "name": "Alice", "internal_field": "secret"})),
        );

        let smt = ReplaceField::new().include(["id", "name"]);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["id"], 1);
        assert_eq!(after["name"], "Alice");
        assert!(!after.as_object().unwrap().contains_key("internal_field"));
    }

    #[test]
    fn test_replace_field_exclude() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"id": 1, "name": "Alice", "password_hash": "xxx"})),
        );

        let smt = ReplaceField::new().exclude(["password_hash"]);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after.as_object().unwrap().contains_key("id"));
        assert!(!after.as_object().unwrap().contains_key("password_hash"));
    }

    // InsertField tests
    #[test]
    fn test_insert_field_static() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice"})));

        let smt = InsertField::new()
            .static_field("version", json!(1))
            .static_field("source", json!("cdc"));

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["version"], 1);
        assert_eq!(after["source"], "cdc");
    }

    #[test]
    fn test_insert_field_timestamp() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice"})));

        let smt = InsertField::new().timestamp_field("processed_at");
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after["processed_at"].as_str().is_some());
    }

    #[test]
    fn test_insert_field_copy() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"id": 42, "name": "Alice"})),
        );

        let smt = InsertField::new().copy_field("original_id", "id");
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["original_id"], 42);
    }

    // Filter tests
    #[test]
    fn test_filter_keep_equals() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "active", "name": "Alice"})),
        );

        let smt = Filter::keep(FilterCondition::Equals {
            field: "status".to_string(),
            value: json!("active"),
        });

        assert!(smt.apply(event).is_some());
    }

    #[test]
    fn test_filter_drop_equals() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "deleted", "name": "Alice"})),
        );

        let smt = Filter::drop(FilterCondition::Equals {
            field: "status".to_string(),
            value: json!("deleted"),
        });

        assert!(smt.apply(event).is_none());
    }

    #[test]
    fn test_filter_regex() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"email": "alice@example.com"})),
        );

        let smt = Filter::keep(FilterCondition::Matches {
            field: "email".to_string(),
            pattern: r".*@example\.com$".to_string(),
        });

        assert!(smt.apply(event).is_some());
    }

    #[test]
    fn test_filter_and() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "active", "role": "admin"})),
        );

        let smt = Filter::keep(FilterCondition::And(vec![
            FilterCondition::Equals {
                field: "status".to_string(),
                value: json!("active"),
            },
            FilterCondition::Equals {
                field: "role".to_string(),
                value: json!("admin"),
            },
        ]));

        assert!(smt.apply(event).is_some());
    }

    #[test]
    fn test_filter_or() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "deleted", "role": "admin"})),
        );

        let smt = Filter::keep(FilterCondition::Or(vec![
            FilterCondition::Equals {
                field: "status".to_string(),
                value: json!("active"),
            },
            FilterCondition::Equals {
                field: "role".to_string(),
                value: json!("admin"),
            },
        ]));

        assert!(smt.apply(event).is_some());
    }

    #[test]
    fn test_filter_not() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"status": "active"})));

        let smt = Filter::keep(FilterCondition::Not(Box::new(FilterCondition::Equals {
            field: "status".to_string(),
            value: json!("deleted"),
        })));

        assert!(smt.apply(event).is_some());
    }

    #[test]
    fn test_filter_is_null() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "email": null})),
        );

        let smt = Filter::keep(FilterCondition::IsNull {
            field: "email".to_string(),
        });

        assert!(smt.apply(event).is_some());
    }

    #[test]
    fn test_filter_in() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"status": "pending"})));

        let smt = Filter::keep(FilterCondition::In {
            field: "status".to_string(),
            values: vec![json!("pending"), json!("active"), json!("approved")],
        });

        assert!(smt.apply(event).is_some());
    }

    // Cast tests
    #[test]
    fn test_cast_to_string() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 42, "active": true})));

        let smt = Cast::new()
            .field("id", CastType::String)
            .field("active", CastType::String);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["id"], "42");
        assert_eq!(after["active"], "true");
    }

    #[test]
    fn test_cast_string_to_int() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"count": "42"})));

        let smt = Cast::new().field("count", CastType::Integer);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["count"], 42);
    }

    #[test]
    fn test_cast_to_boolean() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"flag1": "true", "flag2": 1, "flag3": "yes"})),
        );

        let smt = Cast::new()
            .field("flag1", CastType::Boolean)
            .field("flag2", CastType::Boolean)
            .field("flag3", CastType::Boolean);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["flag1"], true);
        assert_eq!(after["flag2"], true);
        assert_eq!(after["flag3"], true);
    }

    #[test]
    fn test_cast_json_string() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"config": "{\"key\":\"value\"}"})),
        );

        let smt = Cast::new().field("config", CastType::Json);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["config"]["key"], "value");
    }

    // Flatten tests
    #[test]
    fn test_flatten() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "user": {
                    "name": "Alice",
                    "address": {
                        "city": "NYC",
                        "zip": "10001"
                    }
                }
            })),
        );

        let smt = Flatten::new();
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["user.name"], "Alice");
        assert_eq!(after["user.address.city"], "NYC");
    }

    #[test]
    fn test_flatten_max_depth() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "level1": {
                    "level2": {
                        "level3": "deep"
                    }
                }
            })),
        );

        let smt = Flatten::new().max_depth(1);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // Should only flatten one level
        assert!(after.as_object().unwrap().contains_key("level1.level2"));
    }

    #[test]
    fn test_flatten_custom_delimiter() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "user": {
                    "name": "Alice"
                }
            })),
        );

        let smt = Flatten::new().delimiter("_");
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["user_name"], "Alice");
    }

    // RegexRouter tests
    #[test]
    fn test_regex_router() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));

        let smt = RegexRouter::new("cdc.default")
            .route(r"^public\.users$", "cdc.users")
            .route(r"^public\.orders.*", "cdc.orders");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();
        assert_eq!(after["__topic"], "cdc.users");
    }

    #[test]
    fn test_regex_router_default() {
        let mut event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));
        event.table = "unknown_table".to_string();

        let smt = RegexRouter::new("cdc.default").route(r"^public\.users$", "cdc.users");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();
        assert_eq!(after["__topic"], "cdc.default");
    }

    // Chain combination tests
    #[test]
    fn test_full_transform_chain() {
        let event = make_event(
            CdcOp::Update,
            Some(json!({"id": 1, "ssn": "123-45-6789", "name": "Old"})),
            Some(
                json!({"id": 1, "ssn": "123-45-6789", "name": "New", "created_at": 1705320000000i64}),
            ),
        );

        let chain = SmtChain::new()
            .add(
                ExtractNewRecordState::new()
                    .add_op_field()
                    .add_table_field(),
            )
            .add(MaskField::new(["ssn"]))
            .add(TimestampConverter::new(
                ["created_at"],
                TimestampFormat::Iso8601,
            ))
            .add(InsertField::new().static_field("_version", json!(1)));

        let result = chain.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__op"], "u");
        assert_eq!(after["__table"], "users");
        assert_eq!(after["ssn"], "***********");
        assert!(after["created_at"].as_str().unwrap().contains("2024"));
        assert_eq!(after["_version"], 1);
    }

    #[test]
    fn test_filter_drops_chain() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"status": "deleted"})));

        let chain = SmtChain::new()
            .add(Filter::drop(FilterCondition::Equals {
                field: "status".to_string(),
                value: json!("deleted"),
            }))
            .add(InsertField::new().static_field("processed", json!(true)));

        // Should be dropped by filter, so InsertField never runs
        assert!(chain.apply(event).is_none());
    }

    // Predicate tests
    #[test]
    fn test_predicate_table() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));

        let predicate = Predicate::table(r"^users$").unwrap();
        assert!(predicate.matches(&event));

        let predicate = Predicate::table(r"^orders$").unwrap();
        assert!(!predicate.matches(&event));
    }

    #[test]
    fn test_predicate_operation() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));

        let predicate = Predicate::operation(vec![CdcOp::Insert, CdcOp::Update]);
        assert!(predicate.matches(&event));

        let predicate = Predicate::operation(vec![CdcOp::Delete]);
        assert!(!predicate.matches(&event));
    }

    #[test]
    fn test_predicate_field_value() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "active", "id": 1})),
        );

        let predicate = Predicate::field_equals("status", json!("active"));
        assert!(predicate.matches(&event));

        let predicate = Predicate::field_equals("status", json!("deleted"));
        assert!(!predicate.matches(&event));
    }

    #[test]
    fn test_predicate_and() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"status": "active"})));

        let predicate = Predicate::And(vec![
            Predicate::operation(vec![CdcOp::Insert]),
            Predicate::field_equals("status", json!("active")),
        ]);
        assert!(predicate.matches(&event));
    }

    #[test]
    fn test_predicate_or() {
        let event = make_event(CdcOp::Delete, None, Some(json!({"id": 1})));

        let predicate = Predicate::Or(vec![
            Predicate::operation(vec![CdcOp::Insert]),
            Predicate::operation(vec![CdcOp::Delete]),
        ]);
        assert!(predicate.matches(&event));
    }

    #[test]
    fn test_predicate_not() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));

        let predicate = Predicate::Not(Box::new(Predicate::operation(vec![CdcOp::Delete])));
        assert!(predicate.matches(&event));
    }

    #[test]
    fn test_conditional_smt_when() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice"})));

        // Only mask when inserting
        let smt = ConditionalSmt::when(
            Predicate::operation(vec![CdcOp::Insert]),
            InsertField::new().static_field("_inserted", json!(true)),
        );

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();
        assert_eq!(after["_inserted"], true);
    }

    #[test]
    fn test_conditional_smt_unless() {
        let event = make_event(CdcOp::Delete, Some(json!({"id": 1})), None);

        // Don't add field on deletes
        let smt = ConditionalSmt::unless(
            Predicate::operation(vec![CdcOp::Delete]),
            InsertField::new().static_field("_processed", json!(true)),
        );

        let result = smt.apply(event).unwrap();
        // Delete event should pass through unchanged (no after to modify anyway)
        assert!(result.after.is_none());
    }

    #[test]
    fn test_conditional_smt_table_predicate() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"ssn": "123-45-6789"})));

        // Only mask SSN for users table
        let smt = ConditionalSmt::when(
            Predicate::table(r"^users$").unwrap(),
            MaskField::new(["ssn"]),
        );

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();
        assert_eq!(after["ssn"], "***********");
    }

    // HeaderToValue tests
    #[test]
    fn test_header_to_value() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"id": 1})));

        let smt = HeaderToValue::new()
            .field("_source", HeaderSource::SourceType)
            .field("_table", HeaderSource::Table)
            .field("_op", HeaderSource::Operation);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["_source"], "postgres");
        assert_eq!(after["_table"], "users");
        assert_eq!(after["_op"], "c");
    }

    #[test]
    fn test_header_to_value_all() {
        let event = make_event(CdcOp::Update, None, Some(json!({"id": 1})));

        let smt = HeaderToValue::new().all_headers("__");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__source_type"], "postgres");
        assert_eq!(after["__database"], "testdb");
        assert_eq!(after["__schema"], "public");
        assert_eq!(after["__table"], "users");
        assert_eq!(after["__op"], "u");
        assert!(after["__ts"].is_number());
    }

    // Unwrap tests
    #[test]
    fn test_unwrap_nested() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "payload": {
                    "data": {
                        "name": "Alice",
                        "email": "alice@test.com"
                    }
                }
            })),
        );

        let smt = Unwrap::new("payload.data");
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name"], "Alice");
        assert_eq!(after["email"], "alice@test.com");
        // Original payload should be gone (replaced)
        assert!(after.get("payload").is_none());
    }

    #[test]
    fn test_unwrap_merge() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "id": 1,
                "nested": {
                    "name": "Alice"
                }
            })),
        );

        let smt = Unwrap::new("nested").merge();
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["id"], 1);
        assert_eq!(after["name"], "Alice");
    }

    // SetNull tests
    #[test]
    fn test_set_null_always() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "password": "secret"})),
        );

        let smt = SetNull::new(["password"]);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name"], "Alice");
        assert!(after["password"].is_null());
    }

    #[test]
    fn test_set_null_if_empty() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "nickname": ""})),
        );

        let smt = SetNull::new(["nickname"]).when(NullCondition::IfEmpty);
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after["nickname"].is_null());
    }

    #[test]
    fn test_set_null_if_equals() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "N/A", "name": "Alice"})),
        );

        let smt = SetNull::new(["status"]).when(NullCondition::IfEquals(json!("N/A")));
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after["status"].is_null());
        assert_eq!(after["name"], "Alice");
    }

    #[test]
    fn test_set_null_if_matches() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"phone": "000-000-0000", "name": "Alice"})),
        );

        let smt = SetNull::new(["phone"]).when(NullCondition::IfMatches(r"^0+-0+-0+$".to_string()));
        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert!(after["phone"].is_null());
    }

    // Combined advanced tests
    #[test]
    fn test_advanced_chain_with_predicates() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "ssn": "123-45-6789", "status": "active"})),
        );

        let chain = SmtChain::new()
            // Only mask SSN for inserts
            .add(ConditionalSmt::when(
                Predicate::operation(vec![CdcOp::Insert]),
                MaskField::new(["ssn"]),
            ))
            // Add metadata
            .add(
                HeaderToValue::new()
                    .field("_table", HeaderSource::Table)
                    .field("_op", HeaderSource::Operation),
            )
            // Add version
            .add(InsertField::new().static_field("_version", json!(1)));

        let result = chain.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name"], "Alice");
        assert_eq!(after["ssn"], "***********"); // Masked
        assert_eq!(after["_table"], "users");
        assert_eq!(after["_op"], "c");
        assert_eq!(after["_version"], 1);
    }

    // =========================================================================
    // TimezoneConverter tests
    // =========================================================================

    #[test]
    fn test_timezone_converter_utc_to_est() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": "2024-01-15T15:00:00Z"})),
        );

        let smt = TimezoneConverter::new(["created_at"])
            .from("UTC")
            .to("America/New_York");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // UTC 15:00 should be EST 10:00 (EST is UTC-5 in January)
        let ts = after["created_at"].as_str().unwrap();
        assert!(ts.contains("10:00:00"));
        assert!(ts.contains("-05:00") || ts.contains("New_York"));
    }

    #[test]
    fn test_timezone_converter_date_only() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": "2024-01-15T23:00:00Z"})),
        );

        let smt = TimezoneConverter::new(["created_at"])
            .from("UTC")
            .to("America/Los_Angeles")
            .date_only();

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // UTC 23:00 is 15:00 PT (UTC-8), same day
        assert_eq!(after["created_at"], "2024-01-15");
    }

    #[test]
    fn test_timezone_converter_custom_format() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": "2024-01-15T12:30:00Z"})),
        );

        let smt = TimezoneConverter::new(["created_at"])
            .from("UTC")
            .to("Europe/London")
            .format("%Y-%m-%d %H:%M");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // London is UTC+0 in January
        assert_eq!(after["created_at"], "2024-01-15 12:30");
    }

    #[test]
    fn test_timezone_converter_epoch_input() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"created_at": 1705320000000i64})), // 2024-01-15T10:00:00Z
        );

        let smt = TimezoneConverter::new(["created_at"])
            .from("UTC")
            .to("Asia/Tokyo"); // JST is UTC+9

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // UTC 10:00 -> JST 19:00
        let ts = after["created_at"].as_str().unwrap();
        // The timestamp should contain Tokyo timezone indicator and correct time
        assert!(ts.contains("2024-01-15") && ts.contains("+09:00"));
    }

    #[test]
    fn test_timezone_converter_multiple_fields() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "created_at": "2024-01-15T10:00:00Z",
                "updated_at": "2024-01-15T15:30:00Z"
            })),
        );

        let smt = TimezoneConverter::new(["created_at", "updated_at"])
            .from("UTC")
            .to("Europe/Paris"); // CET is UTC+1 in January

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // UTC 10:00 -> CET 11:00
        assert!(after["created_at"].as_str().unwrap().contains("11:00:00"));
        // UTC 15:30 -> CET 16:30
        assert!(after["updated_at"].as_str().unwrap().contains("16:30:00"));
    }

    // =========================================================================
    // ContentRouter tests
    // =========================================================================

    #[test]
    fn test_content_router_exact_match() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"priority": "high", "message": "urgent"})),
        );

        let smt = ContentRouter::new()
            .route("priority", "high", "priority-events")
            .route("priority", "low", "batch-events")
            .default_topic("default-events");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__routing_topic"], "priority-events");
    }

    #[test]
    fn test_content_router_pattern_match() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"category": "urgent-alert-123", "data": "test"})),
        );

        let smt = ContentRouter::new()
            .route_pattern("category", r"^urgent", "urgent-events")
            .route_pattern("category", r"^normal", "normal-events");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__routing_topic"], "urgent-events");
    }

    #[test]
    fn test_content_router_in_set() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "active", "name": "Alice"})),
        );

        let smt = ContentRouter::new()
            .route_in("status", ["active", "pending"], "active-events")
            .route_in("status", ["archived", "deleted"], "archive-events");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__routing_topic"], "active-events");
    }

    #[test]
    fn test_content_router_predicate() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"amount": 1500, "currency": "USD"})),
        );

        let smt = ContentRouter::new()
            .route_if(
                "amount",
                |v| v.as_i64().map(|n| n > 1000).unwrap_or(false),
                "high-value",
            )
            .route_if(
                "amount",
                |v| v.as_i64().map(|n| n <= 1000).unwrap_or(false),
                "normal-value",
            );

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__routing_topic"], "high-value");
    }

    #[test]
    fn test_content_router_default_topic() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"type": "unknown", "data": "test"})),
        );

        let smt = ContentRouter::new()
            .route("type", "order", "order-events")
            .route("type", "user", "user-events")
            .default_topic("other-events");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["__routing_topic"], "other-events");
    }

    #[test]
    fn test_content_router_custom_field() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"priority": "high", "message": "test"})),
        );

        let smt = ContentRouter::new()
            .route("priority", "high", "priority-events")
            .topic_field("_target_topic");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["_target_topic"], "priority-events");
        assert!(after.get("__routing_topic").is_none());
    }

    // =========================================================================
    // ComputeField tests
    // =========================================================================

    #[test]
    fn test_compute_field_concat() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"first_name": "Alice", "last_name": "Smith"})),
        );

        let smt = ComputeField::new().concat("full_name", ["$first_name", " ", "$last_name"]);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["full_name"], "Alice Smith");
    }

    #[test]
    fn test_compute_field_hash() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"id": 123, "email": "alice@test.com"})),
        );

        let smt = ComputeField::new().hash("user_hash", ["id", "email"]);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // Should be a hex hash
        let hash = after["user_hash"].as_str().unwrap();
        assert_eq!(hash.len(), 64); // SHA-256 is 64 hex chars
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_compute_field_coalesce() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"nickname": null, "username": "alice123", "email": "alice@test.com"})),
        );

        let smt = ComputeField::new().coalesce("display_name", ["nickname", "username", "email"]);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["display_name"], "alice123");
    }

    #[test]
    fn test_compute_field_sum() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"price": 100.0, "tax": 10.0, "shipping": 5.0})),
        );

        let smt = ComputeField::new().sum("total", ["price", "tax", "shipping"]);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["total"].as_f64().unwrap(), 115.0);
    }

    #[test]
    fn test_compute_field_length() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice"})));

        let smt = ComputeField::new().length("name_length", "name");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name_length"], 5);
    }

    #[test]
    fn test_compute_field_upper_lower() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice Smith"})));

        let smt = ComputeField::new()
            .upper("name_upper", "name")
            .lower("name_lower", "name");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["name_upper"], "ALICE SMITH");
        assert_eq!(after["name_lower"], "alice smith");
    }

    #[test]
    fn test_compute_field_substring() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"phone": "+1-555-123-4567"})),
        );

        let smt = ComputeField::new().substring("area_code", "phone", 3, Some(3));

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["area_code"], "555");
    }

    #[test]
    fn test_compute_field_replace() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"phone": "555-123-4567"})));

        let smt = ComputeField::new().replace("phone_clean", "phone", r"-", "");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["phone_clean"], "5551234567");
    }

    #[test]
    fn test_compute_field_current_timestamp() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice"})));

        let smt = ComputeField::new().current_timestamp("processed_at");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // Should be ISO8601 timestamp
        assert!(after["processed_at"].as_str().unwrap().contains("20"));
    }

    #[test]
    fn test_compute_field_uuid() {
        let event = make_event(CdcOp::Insert, None, Some(json!({"name": "Alice"})));

        let smt = ComputeField::new().uuid("request_id");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        // Should be UUID format (8-4-4-4-12)
        let uuid = after["request_id"].as_str().unwrap();
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }

    #[test]
    fn test_compute_field_json_path() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "metadata": {
                    "user": {
                        "name": "Alice"
                    }
                }
            })),
        );

        let smt = ComputeField::new().json_path("user_name", "metadata", "user.name");

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["user_name"], "Alice");
    }

    #[test]
    fn test_compute_field_conditional() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"status": "active", "name": "Alice"})),
        );

        let smt = ComputeField::new().conditional(
            "is_active",
            ComputeCondition::FieldEquals("status".to_string(), json!("active")),
            json!(true),
            json!(false),
        );

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["is_active"], true);
    }

    #[test]
    fn test_compute_field_conditional_null_check() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"name": "Alice", "deleted_at": null})),
        );

        let smt = ComputeField::new().conditional(
            "status",
            ComputeCondition::FieldIsNull("deleted_at".to_string()),
            json!("active"),
            json!("deleted"),
        );

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["status"], "active");
    }

    #[test]
    fn test_compute_field_conditional_pattern() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({"email": "admin@company.com", "name": "Admin"})),
        );

        let smt = ComputeField::new().conditional(
            "is_admin",
            ComputeCondition::FieldMatches("email".to_string(), r"^admin@".to_string()),
            json!(true),
            json!(false),
        );

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["is_admin"], true);
    }

    #[test]
    fn test_compute_field_chain() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "first_name": "alice",
                "last_name": "smith",
                "amount": 100.0
            })),
        );

        let smt = ComputeField::new()
            .upper("first_upper", "first_name")
            .upper("last_upper", "last_name")
            .concat("full_name", ["$first_upper", " ", "$last_upper"])
            .sum("total_with_tax", ["amount"]);

        let result = smt.apply(event).unwrap();
        let after = result.after.unwrap();

        assert_eq!(after["first_upper"], "ALICE");
        assert_eq!(after["last_upper"], "SMITH");
        // Concat uses computed values since we clone obj after each computation
        assert_eq!(after["full_name"], "ALICE SMITH");
    }

    // =========================================================================
    // Integration tests - combining new SMTs
    // =========================================================================

    #[test]
    fn test_full_pipeline_with_new_smts() {
        let event = make_event(
            CdcOp::Insert,
            None,
            Some(json!({
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice@test.com",
                "priority": "high",
                "created_at": "2024-01-15T10:00:00Z"
            })),
        );

        let chain = SmtChain::new()
            // Compute full name
            .add(
                ComputeField::new()
                    .concat("full_name", ["$first_name", " ", "$last_name"])
                    .hash("email_hash", ["email"])
                    .uuid("event_id"),
            )
            // Mask email
            .add(MaskField::new(["email"]))
            // Route based on priority
            .add(
                ContentRouter::new()
                    .route("priority", "high", "priority-events")
                    .default_topic("normal-events"),
            )
            // Convert timezone
            .add(
                TimezoneConverter::new(["created_at"])
                    .from("UTC")
                    .to("America/New_York"),
            );

        let result = chain.apply(event).unwrap();
        let after = result.after.unwrap();

        // Check all transforms applied
        assert_eq!(after["full_name"], "Alice Smith");
        assert_eq!(after["email_hash"].as_str().unwrap().len(), 64);
        assert!(after["event_id"].as_str().unwrap().contains("-"));
        assert_ne!(after["email"], "alice@test.com"); // Masked
        assert_eq!(after["__routing_topic"], "priority-events");
        // Timezone should be converted (EST is UTC-5)
        let ts = after["created_at"].as_str().unwrap();
        assert!(ts.contains("05:00:00") || ts.contains("-05:00"));
    }
}

//! Common CDC utilities shared between PostgreSQL and MySQL connectors.
//!
//! This module provides shared functionality to eliminate code duplication
//! between CDC connectors while ensuring consistent behavior.
//!
//! ## Supported SMT Transforms
//!
//! All Debezium-compatible transforms are supported:
//!
//! | Transform | Description | Status |
//! |-----------|-------------|--------|
//! | `ExtractNewRecordState` | Flatten envelope, extract "after" state | ✅ |
//! | `ValueToKey` | Extract key fields from value | ✅ |
//! | `MaskField` | Mask sensitive fields (SSN, credit cards) | ✅ |
//! | `InsertField` | Add static or computed fields | ✅ |
//! | `ReplaceField` / `RenameField` | Rename, include, exclude fields | ✅ |
//! | `RegexRouter` | Route events based on regex patterns | ✅ |
//! | `TimestampConverter` | Convert timestamp formats | ✅ |
//! | `Filter` | Filter events based on conditions | ✅ |
//! | `Cast` | Cast field types | ✅ |
//! | `Flatten` | Flatten nested structures | ✅ |
//!
//! ## Example Usage
//!
//! ```yaml
//! transforms:
//!   - transform_type: mask_field
//!     config:
//!       fields: ["ssn", "credit_card"]
//!   - transform_type: timestamp_converter
//!     config:
//!       fields: ["created_at", "updated_at"]
//!       format: iso8601
//! ```

use super::cdc_config::{ColumnFilterConfig, SmtTransformConfig, SmtTransformType};
use rivven_cdc::common::{CdcEvent, CdcOp, FilterCondition, Smt, SmtChain};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// SMT (Single Message Transform) Builder
// ============================================================================

/// Build an SMT transform from YAML configuration.
///
/// Supports all Debezium-compatible transform types:
/// - `ExtractNewRecordState` - Flatten envelope, extract "after" state
/// - `ValueToKey` - Extract key fields from value
/// - `MaskField` - Mask sensitive fields (SSN, credit cards, etc.)
/// - `InsertField` - Add static or computed fields
/// - `ReplaceField` / `RenameField` - Rename, include, or exclude fields
/// - `RegexRouter` - Route events based on regex patterns
/// - `TimestampConverter` - Convert timestamp formats
/// - `Filter` - Filter events based on conditions
/// - `Cast` - Cast field types
/// - `Flatten` - Flatten nested structures
///
/// # Example YAML Configuration
///
/// ```yaml
/// transforms:
///   - transform_type: mask_field
///     config:
///       fields: ["ssn", "credit_card"]
///   - transform_type: extract_new_record_state
///     config:
///       drop_tombstones: false
///       add_table: true
/// ```
pub fn build_smt_transform(config: &SmtTransformConfig) -> Result<Arc<dyn Smt>, String> {
    use rivven_cdc::common::{
        Cast, CastType, ComputeField, ContentRouter, ExtractNewRecordState, Filter, Flatten,
        HeaderSource, HeaderToValue, InsertField, MaskField, NullCondition, RegexRouter,
        ReplaceField, SetNull, TimestampConverter, TimestampFormat, TimezoneConverter, Unwrap,
        ValueToKey,
    };

    match &config.transform_type {
        SmtTransformType::ExtractNewRecordState => {
            let mut transform = ExtractNewRecordState::new();

            if config
                .config
                .get("drop_tombstones")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.drop_tombstones();
            }
            if config
                .config
                .get("add_table")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.add_table_field();
            }
            if config
                .config
                .get("add_schema")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.add_schema_field();
            }
            if config
                .config
                .get("add_op")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.add_op_field();
            }
            if config
                .config
                .get("add_ts")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.add_ts_field();
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::ValueToKey => {
            let fields = get_string_array(&config.config, "fields")
                .ok_or("value_to_key requires 'fields' array")?;
            Ok(Arc::new(ValueToKey::new(fields)))
        }

        SmtTransformType::MaskField => {
            let fields = get_string_array(&config.config, "fields")
                .ok_or("mask_field requires 'fields' array")?;
            Ok(Arc::new(MaskField::new(fields)))
        }

        SmtTransformType::InsertField => {
            let mut transform = InsertField::new();

            if let Some(static_fields) = config
                .config
                .get("static_fields")
                .and_then(|v| v.as_object())
            {
                for (field, value) in static_fields {
                    transform = transform.static_field(field.clone(), value.clone());
                }
            }
            if let Some(ts_field) = config
                .config
                .get("timestamp_field")
                .and_then(|v| v.as_str())
            {
                transform = transform.timestamp_field(ts_field);
            }
            if let Some(date_field) = config.config.get("date_field").and_then(|v| v.as_str()) {
                transform = transform.date_field(date_field);
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::ReplaceField | SmtTransformType::RenameField => {
            let mut transform = ReplaceField::new();

            if let Some(renames) = config.config.get("renames").and_then(|v| v.as_object()) {
                for (from, to) in renames {
                    if let Some(to_str) = to.as_str() {
                        transform = transform.rename(from.clone(), to_str.to_string());
                    }
                }
            }
            if let Some(include) = get_string_array(&config.config, "include") {
                transform = transform.include(include);
            }
            if let Some(exclude) = get_string_array(&config.config, "exclude") {
                transform = transform.exclude(exclude);
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::RegexRouter => {
            let default_topic = config
                .config
                .get("default_topic")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            let mut router = RegexRouter::new(default_topic);

            if let Some(rules) = config.config.get("routes").and_then(|v| v.as_array()) {
                for rule in rules {
                    if let Some(obj) = rule.as_object() {
                        if let (Some(pattern), Some(topic)) = (
                            obj.get("pattern").and_then(|v| v.as_str()),
                            obj.get("topic").and_then(|v| v.as_str()),
                        ) {
                            router = router.route(pattern, topic);
                        }
                    }
                }
            }

            Ok(Arc::new(router))
        }

        SmtTransformType::TimestampConverter => {
            let fields = get_string_array(&config.config, "fields")
                .ok_or("timestamp_converter requires 'fields' array")?;

            let format = match config.config.get("format").and_then(|v| v.as_str()) {
                Some("iso8601") | None => TimestampFormat::Iso8601,
                Some("epoch_millis") => TimestampFormat::EpochMillis,
                Some("epoch_secs") | Some("epoch_seconds") => TimestampFormat::EpochSeconds,
                Some("epoch_micros") => TimestampFormat::EpochMicros,
                Some("date") | Some("date_only") => TimestampFormat::DateOnly,
                Some("time") | Some("time_only") => TimestampFormat::TimeOnly,
                Some(fmt) => return Err(format!("Unknown timestamp format: {}", fmt)),
            };

            Ok(Arc::new(TimestampConverter::new(fields, format)))
        }

        SmtTransformType::Filter => {
            let condition = config
                .config
                .get("condition")
                .and_then(|v| v.as_str())
                .ok_or("filter requires 'condition'")?;

            let filter_condition = parse_filter_condition(condition)?;

            // Check if we should drop or keep matching events
            let drop_matching = config
                .config
                .get("drop")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let filter = if drop_matching {
                Filter::drop(filter_condition)
            } else {
                Filter::keep(filter_condition)
            };

            Ok(Arc::new(filter))
        }

        SmtTransformType::Cast => {
            let mut transform = Cast::new();

            if let Some(specs) = config.config.get("specs").and_then(|v| v.as_object()) {
                for (field, type_val) in specs {
                    let cast_type = match type_val.as_str() {
                        Some("string") => CastType::String,
                        Some("int32") | Some("integer") | Some("int64") | Some("long") => {
                            CastType::Integer
                        }
                        Some("float32") | Some("float") | Some("float64") | Some("double") => {
                            CastType::Float
                        }
                        Some("boolean") | Some("bool") => CastType::Boolean,
                        Some("json") => CastType::Json,
                        Some(t) => return Err(format!("Unknown cast type: {}", t)),
                        None => continue,
                    };
                    transform = transform.field(field.clone(), cast_type);
                }
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::Flatten => {
            let mut transform = Flatten::new();

            if let Some(delimiter) = config.config.get("delimiter").and_then(|v| v.as_str()) {
                transform = transform.delimiter(delimiter);
            }

            if let Some(depth) = config.config.get("max_depth").and_then(|v| v.as_u64()) {
                transform = transform.max_depth(depth as usize);
            }

            Ok(Arc::new(transform))
        }

        // ====================================================================
        // Advanced Transforms
        // ====================================================================
        SmtTransformType::TimezoneConverter => {
            let fields = get_string_array(&config.config, "fields")
                .ok_or("timezone_converter requires 'fields' array")?;

            let mut transform = TimezoneConverter::new(fields);

            if let Some(from_tz) = config.config.get("from").and_then(|v| v.as_str()) {
                transform = transform.from(from_tz);
            }
            if let Some(to_tz) = config.config.get("to").and_then(|v| v.as_str()) {
                transform = transform.to(to_tz);
            }
            if config
                .config
                .get("date_only")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.date_only();
            }
            if let Some(fmt) = config.config.get("format").and_then(|v| v.as_str()) {
                transform = transform.format(fmt);
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::ContentRouter => {
            let mut router = ContentRouter::new();

            if let Some(default) = config.config.get("default_topic").and_then(|v| v.as_str()) {
                router = router.default_topic(default);
            }

            if let Some(routes) = config.config.get("routes").and_then(|v| v.as_array()) {
                for route in routes {
                    if let Some(obj) = route.as_object() {
                        let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let topic = obj.get("topic").and_then(|v| v.as_str()).unwrap_or("");

                        if let Some(value) = obj.get("value") {
                            router = router.route(field, value.clone(), topic);
                        } else if let Some(pattern) = obj.get("pattern").and_then(|v| v.as_str()) {
                            router = router.route_pattern(field, pattern, topic);
                        }
                    }
                }
            }

            Ok(Arc::new(router))
        }

        SmtTransformType::HeaderToValue => {
            let mut transform = HeaderToValue::new();

            // Support mapping envelope fields into record value
            // Example config: { "fields": { "source_type": "db_type", "table": "source_table" } }
            // Maps envelope.source_type -> value.db_type, envelope.table -> value.source_table
            if let Some(mappings) = config.config.get("fields").and_then(|v| v.as_object()) {
                for (source, target) in mappings {
                    if let Some(target_field) = target.as_str() {
                        let header_source = match source.as_str() {
                            "source_type" | "source" => HeaderSource::SourceType,
                            "database" | "db" => HeaderSource::Database,
                            "schema" => HeaderSource::Schema,
                            "table" => HeaderSource::Table,
                            "operation" | "op" => HeaderSource::Operation,
                            "timestamp" | "ts" => HeaderSource::Timestamp,
                            "transaction_id" | "txid" | "xid" => HeaderSource::TransactionId,
                            other => {
                                tracing::warn!("Unknown header source: {}", other);
                                continue;
                            }
                        };
                        transform = transform.field(target_field, header_source);
                    }
                }
            }

            // Support adding all standard headers with a prefix
            // Example: { "all_headers_prefix": "__" } -> adds __source_type, __database, etc.
            if let Some(prefix) = config
                .config
                .get("all_headers_prefix")
                .and_then(|v| v.as_str())
            {
                transform = transform.all_headers(prefix);
            }

            // Support move mode (remove from envelope) vs copy mode (keep in both)
            if config
                .config
                .get("move")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                transform = transform.move_mode();
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::Unwrap => {
            let field = config
                .config
                .get("field")
                .and_then(|v| v.as_str())
                .ok_or("unwrap requires 'field'")?;

            Ok(Arc::new(Unwrap::new(field)))
        }

        SmtTransformType::SetNull => {
            // SetNull takes fields in constructor
            // Example: { "fields": ["password", "secret"], "condition": "always" | "if_empty" | { "equals": "redacted" } }
            let fields = get_string_array(&config.config, "fields")
                .ok_or("set_null requires 'fields' array")?;

            let mut transform = SetNull::new(fields);

            // Parse optional condition
            if let Some(condition) = config.config.get("condition") {
                let null_cond = if let Some(cond_str) = condition.as_str() {
                    match cond_str {
                        "always" => NullCondition::Always,
                        "if_empty" | "empty" => NullCondition::IfEmpty,
                        _ => {
                            // Treat as pattern
                            NullCondition::IfMatches(cond_str.to_string())
                        }
                    }
                } else if let Some(obj) = condition.as_object() {
                    if let Some(equals) = obj.get("equals") {
                        NullCondition::IfEquals(equals.clone())
                    } else if let Some(pattern) = obj.get("matches").and_then(|v| v.as_str()) {
                        NullCondition::IfMatches(pattern.to_string())
                    } else {
                        NullCondition::Always
                    }
                } else {
                    NullCondition::Always
                };
                transform = transform.when(null_cond);
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::ComputeField => {
            let mut transform = ComputeField::new();

            if let Some(computations) = config.config.get("computations").and_then(|v| v.as_array())
            {
                for comp in computations {
                    if let Some(obj) = comp.as_object() {
                        let target = obj.get("target").and_then(|v| v.as_str()).unwrap_or("");
                        let op_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");

                        match op_type {
                            "concat" => {
                                if let Some(parts) = get_string_array_from_object(obj, "parts") {
                                    // Add $ prefix for field references
                                    let parts: Vec<String> = parts
                                        .into_iter()
                                        .map(|p| {
                                            if p.starts_with('$') {
                                                p
                                            } else {
                                                format!("${}", p)
                                            }
                                        })
                                        .collect();
                                    transform = transform.concat(target, parts);
                                }
                            }
                            "hash" => {
                                if let Some(fields) = get_string_array_from_object(obj, "fields") {
                                    transform = transform.hash(target, fields);
                                }
                            }
                            "upper" | "uppercase" => {
                                if let Some(source) = obj.get("source").and_then(|v| v.as_str()) {
                                    transform = transform.upper(target, source);
                                }
                            }
                            "lower" | "lowercase" => {
                                if let Some(source) = obj.get("source").and_then(|v| v.as_str()) {
                                    transform = transform.lower(target, source);
                                }
                            }
                            "coalesce" => {
                                if let Some(fields) = get_string_array_from_object(obj, "fields") {
                                    transform = transform.coalesce(target, fields);
                                }
                            }
                            "uuid" => {
                                transform = transform.uuid(target);
                            }
                            "timestamp" | "current_timestamp" => {
                                transform = transform.current_timestamp(target);
                            }
                            _ => {
                                tracing::warn!("Unknown compute operation type: {}", op_type);
                            }
                        }
                    }
                }
            }

            Ok(Arc::new(transform))
        }

        SmtTransformType::ConditionalSmt => {
            // ConditionalSmt requires nested SMT configs which is complex
            // For now, return an error suggesting alternative approaches
            Err(
                "conditional_smt requires nested transform configs - use filter transform instead"
                    .to_string(),
            )
        }

        SmtTransformType::InsertHeader | SmtTransformType::Custom => Err(format!(
            "SMT transform type {:?} not yet implemented",
            config.transform_type
        )),
    }
}

/// Parse a filter condition string into a FilterCondition
fn parse_filter_condition(condition: &str) -> Result<FilterCondition, String> {
    // Simple parser for conditions like "field = value" or "field > 10"
    let parts: Vec<&str> = condition.split_whitespace().collect();

    if parts.len() < 3 {
        return Err(format!("Invalid filter condition: {}", condition));
    }

    let field = parts[0].to_string();
    let op = parts[1];
    let value: serde_json::Value = serde_json::from_str(parts[2..].join(" ").as_str())
        .unwrap_or_else(|_| serde_json::Value::String(parts[2..].join(" ")));

    match op {
        "=" | "==" => Ok(FilterCondition::Equals { field, value }),
        "!=" | "<>" => Ok(FilterCondition::NotEquals { field, value }),
        "is_null" => Ok(FilterCondition::IsNull { field }),
        "is_not_null" => Ok(FilterCondition::IsNotNull { field }),
        "matches" => Ok(FilterCondition::Matches {
            field,
            pattern: value.as_str().unwrap_or("").to_string(),
        }),
        "in" => {
            let values = value.as_array().cloned().unwrap_or_default();
            Ok(FilterCondition::In { field, values })
        }
        _ => Err(format!("Unknown filter operator: {}", op)),
    }
}

/// Build an SMT chain from a list of transform configurations.
pub fn build_smt_chain(transforms: &[SmtTransformConfig]) -> Option<SmtChain> {
    if transforms.is_empty() {
        return None;
    }

    let mut chain = SmtChain::new();
    for transform_config in transforms {
        match build_smt_transform(transform_config) {
            Ok(arc_transform) => {
                chain = chain.add_boxed(arc_transform);
                tracing::debug!("Added SMT transform: {:?}", transform_config.transform_type);
            }
            Err(e) => {
                tracing::warn!("Failed to build SMT transform: {}", e);
            }
        }
    }
    Some(chain)
}

// ============================================================================
// Column Filtering
// ============================================================================

/// Apply column filter (include/exclude) to a JSON value.
///
/// This modifies the value in-place:
/// - If `include` is non-empty, only those columns are kept
/// - All columns in `exclude` are removed
///
/// # Example
///
/// ```yaml
/// column_filters:
///   public.users:
///     include: ["id", "email", "name"]
///     exclude: ["password_hash", "ssn"]
/// ```
pub fn apply_column_filter(value: &mut serde_json::Value, filter: &ColumnFilterConfig) {
    if let serde_json::Value::Object(ref mut map) = value {
        if !filter.include.is_empty() {
            // Include mode: keep only specified columns
            map.retain(|k, _| filter.include.contains(k));
        }
        // Exclude mode: remove specified columns
        for col in &filter.exclude {
            map.remove(col);
        }
    }
}

/// Apply column filters to a CDC event's before/after data.
pub fn apply_column_filters_to_event(
    event: &mut CdcEvent,
    filters: &HashMap<String, ColumnFilterConfig>,
) {
    let stream_name = format!("{}.{}", event.schema, event.table);

    if let Some(filter) = filters.get(&stream_name) {
        if let Some(ref mut after) = event.after {
            apply_column_filter(after, filter);
        }
        if let Some(ref mut before) = event.before {
            apply_column_filter(before, filter);
        }
    }
}

/// Apply a single column filter to a CDC event (for use by cdc_features module).
pub fn apply_column_filter_to_event(event: &mut CdcEvent, filter: &ColumnFilterConfig) {
    if let Some(ref mut after) = event.after {
        apply_column_filter(after, filter);
    }
    if let Some(ref mut before) = event.before {
        apply_column_filter(before, filter);
    }
}

// ============================================================================
// Event Conversion
// ============================================================================

use crate::traits::SourceEvent;

/// Convert a rivven-cdc CdcEvent to a rivven-connect SourceEvent.
///
/// Handles all CDC operation types:
/// - Insert/Snapshot → SourceEvent::insert
/// - Update → SourceEvent::update
/// - Delete → SourceEvent::delete
/// - Tombstone → SourceEvent::delete with null payload
/// - Truncate/Schema → None (filtered out)
pub fn cdc_event_to_source_event(event: &CdcEvent) -> Option<SourceEvent> {
    let stream_name = format!("{}.{}", event.schema, event.table);

    let source_event = match event.op {
        CdcOp::Insert | CdcOp::Snapshot => {
            SourceEvent::insert(&stream_name, event.after.clone().unwrap_or_default())
        }
        CdcOp::Update => SourceEvent::update(
            &stream_name,
            event.before.clone(),
            event.after.clone().unwrap_or_default(),
        ),
        CdcOp::Delete => {
            SourceEvent::delete(&stream_name, event.before.clone().unwrap_or_default())
        }
        CdcOp::Tombstone => SourceEvent::delete(&stream_name, serde_json::Value::Null),
        CdcOp::Truncate | CdcOp::Schema => return None,
    };

    Some(
        source_event
            .namespace(&event.schema)
            .position(format!("{}", event.timestamp)),
    )
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract a string array from a config map.
fn get_string_array(config: &HashMap<String, serde_json::Value>, key: &str) -> Option<Vec<String>> {
    config.get(key).and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    })
}

/// Extract a string array from a JSON object (serde_json::Map).
fn get_string_array_from_object(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<Vec<String>> {
    obj.get(key).and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_column_filter_include() {
        let filter = ColumnFilterConfig {
            include: vec!["id".to_string(), "name".to_string()],
            exclude: vec![],
        };

        let mut value = json!({"id": 1, "name": "Alice", "email": "alice@example.com"});
        apply_column_filter(&mut value, &filter);

        assert_eq!(value, json!({"id": 1, "name": "Alice"}));
    }

    #[test]
    fn test_column_filter_exclude() {
        let filter = ColumnFilterConfig {
            include: vec![],
            exclude: vec!["password".to_string()],
        };

        let mut value = json!({"id": 1, "name": "Alice", "password": "secret"});
        apply_column_filter(&mut value, &filter);

        assert_eq!(value, json!({"id": 1, "name": "Alice"}));
    }

    #[test]
    fn test_column_filter_combined() {
        let filter = ColumnFilterConfig {
            include: vec!["id".to_string(), "name".to_string(), "password".to_string()],
            exclude: vec!["password".to_string()],
        };

        let mut value = json!({"id": 1, "name": "Alice", "email": "a@b.com", "password": "secret"});
        apply_column_filter(&mut value, &filter);

        // Include filters first, then exclude
        assert_eq!(value, json!({"id": 1, "name": "Alice"}));
    }

    #[test]
    fn test_cdc_event_to_source_event_insert() {
        let event = CdcEvent::insert(
            "postgres",
            "testdb",
            "public",
            "users",
            json!({"id": 1, "name": "Alice"}),
            1234567890,
        );

        let source_event = cdc_event_to_source_event(&event).unwrap();
        assert_eq!(source_event.stream, "public.users");
    }

    #[test]
    fn test_cdc_event_to_source_event_truncate_filtered() {
        let event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op: CdcOp::Truncate,
            before: None,
            after: None,
            timestamp: 1234567890,
            transaction: None,
        };

        assert!(cdc_event_to_source_event(&event).is_none());
    }

    #[test]
    fn test_build_smt_chain_empty() {
        let chain = build_smt_chain(&[]);
        assert!(chain.is_none());
    }
}

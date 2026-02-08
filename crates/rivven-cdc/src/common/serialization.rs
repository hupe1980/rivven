//! Serialization format configuration and encoding for CDC events
//!
//! CDC events can be serialized in multiple formats:
//! - **JSON**: Human-readable, schema-optional (development/debugging)
//! - **Avro**: Binary with schema evolution (production recommended)
//!
//! # Best Practices
//!
//! In production Rivven deployments, **Avro is the recommended format**:
//! - Schema evolution with compatibility checking
//! - Compact binary encoding (50-70% smaller than JSON)
//! - Schema registry integration for centralized management
//! - Strong typing and validation
//!
//! However, JSON is useful for:
//! - Development and debugging (human-readable)
//! - Systems without Schema Registry
//! - Audit logs requiring human inspection
//! - Interoperability with legacy systems
//!
//! # Example
//!
//! ```rust
//! use rivven_cdc::common::{SerializationConfig, SerializationFormat};
//!
//! // JSON format (default for development)
//! let json_config = SerializationConfig::json();
//!
//! // Avro format with schema registry (production)
//! let avro_config = SerializationConfig::avro()
//!     .with_confluent_wire_format(true);
//! ```

use serde::{Deserialize, Serialize};

use super::CdcEvent;

/// Serialization format for CDC events
///
/// # Production Recommendations
///
/// | Format | Use Case                        | Schema Registry |
/// |--------|-------------------------------- |-----------------|
/// | JSON   | Development, debugging, logs    | Optional        |
/// | Avro   | Production (recommended)        | Required        |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SerializationFormat {
    /// JSON format - human-readable, no schema required
    ///
    /// Events are serialized as plain JSON objects.
    /// Best for: development, debugging, audit logs
    #[default]
    Json,

    /// Apache Avro - binary format with schema evolution
    ///
    /// Events are serialized using Avro binary encoding with
    /// optional Confluent wire format (5-byte schema ID header).
    /// Best for: production Rivven deployments
    /// Requires: Schema Registry
    Avro,
}

impl SerializationFormat {
    /// Check if this format requires a schema registry
    pub fn requires_schema(&self) -> bool {
        matches!(self, Self::Avro)
    }

    /// Check if this format produces binary output
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Avro)
    }

    /// Get the content type for this format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Avro => "application/avro",
        }
    }
}

impl std::fmt::Display for SerializationFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::Avro => write!(f, "avro"),
        }
    }
}

impl std::str::FromStr for SerializationFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "avro" => Ok(Self::Avro),
            _ => Err(format!("Unknown serialization format: {}", s)),
        }
    }
}

/// Configuration for CDC event serialization
///
/// Controls how CDC events are serialized before being sent to topics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializationConfig {
    /// Serialization format (json or avro)
    #[serde(default)]
    pub format: SerializationFormat,

    /// Use Confluent wire format for Avro (5-byte header with schema ID)
    ///
    /// Format: `[0x00][schema_id: 4 bytes big-endian][avro_payload]`
    ///
    /// This is required for compatibility with Confluent Schema Registry
    /// and ecosystem tools.
    #[serde(default = "default_true")]
    pub confluent_wire_format: bool,

    /// Auto-register schemas with the schema registry
    ///
    /// When enabled, new schemas are automatically registered.
    /// When disabled, schemas must be pre-registered.
    #[serde(default = "default_true")]
    pub auto_register_schemas: bool,

    /// Pretty-print JSON output (for debugging)
    #[serde(default)]
    pub pretty_json: bool,

    /// Include null fields in JSON output
    #[serde(default = "default_true")]
    pub include_nulls: bool,

    /// Include transaction metadata in events
    #[serde(default = "default_true")]
    pub include_transaction: bool,

    /// Include source metadata (database, schema, table) in events
    #[serde(default = "default_true")]
    pub include_source: bool,
}

fn default_true() -> bool {
    true
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            format: SerializationFormat::Json,
            confluent_wire_format: true,
            auto_register_schemas: true,
            pretty_json: false,
            include_nulls: true,
            include_transaction: true,
            include_source: true,
        }
    }
}

impl SerializationConfig {
    /// Create JSON serialization config (default for development)
    pub fn json() -> Self {
        Self {
            format: SerializationFormat::Json,
            ..Default::default()
        }
    }

    /// Create Avro serialization config (recommended for production)
    pub fn avro() -> Self {
        Self {
            format: SerializationFormat::Avro,
            confluent_wire_format: true,
            auto_register_schemas: true,
            ..Default::default()
        }
    }

    /// Set Confluent wire format
    pub fn with_confluent_wire_format(mut self, enabled: bool) -> Self {
        self.confluent_wire_format = enabled;
        self
    }

    /// Set auto-register schemas
    pub fn with_auto_register_schemas(mut self, enabled: bool) -> Self {
        self.auto_register_schemas = enabled;
        self
    }

    /// Set pretty JSON output
    pub fn with_pretty_json(mut self, enabled: bool) -> Self {
        self.pretty_json = enabled;
        self
    }

    /// Set include nulls in JSON
    pub fn with_include_nulls(mut self, enabled: bool) -> Self {
        self.include_nulls = enabled;
        self
    }

    /// Set include transaction metadata
    pub fn with_include_transaction(mut self, enabled: bool) -> Self {
        self.include_transaction = enabled;
        self
    }

    /// Set include source metadata
    pub fn with_include_source(mut self, enabled: bool) -> Self {
        self.include_source = enabled;
        self
    }
}

// =============================================================================
// CDC Event Serializer
// =============================================================================

/// The Avro schema for the CDC envelope in JSON form.
///
/// This schema follows the Debezium-compatible envelope format with:
/// - `source_type`, `database`, `schema`, `table` as source metadata
/// - `op` as string enum for operation type
/// - `before`/`after` as nullable JSON strings (flexible for any table schema)
/// - `timestamp` as long (Unix epoch seconds)
/// - `transaction` as nullable record for transaction metadata
const CDC_AVRO_SCHEMA: &str = r#"{
    "type": "record",
    "name": "CdcEvent",
    "namespace": "io.rivven.cdc",
    "fields": [
        {"name": "source_type", "type": "string"},
        {"name": "database", "type": "string"},
        {"name": "schema", "type": "string"},
        {"name": "table", "type": "string"},
        {"name": "op", "type": {"type": "enum", "name": "CdcOp", "symbols": ["Insert", "Update", "Delete", "Tombstone", "Truncate", "Snapshot", "Schema"]}},
        {"name": "before", "type": ["null", "string"], "default": null},
        {"name": "after", "type": ["null", "string"], "default": null},
        {"name": "timestamp", "type": "long"},
        {"name": "transaction", "type": ["null", {
            "type": "record",
            "name": "TransactionMetadata",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "lsn", "type": "string"},
                {"name": "sequence", "type": "long"},
                {"name": "total_events", "type": "long"},
                {"name": "commit_ts", "type": ["null", "long"], "default": null},
                {"name": "is_last", "type": "boolean"}
            ]
        }], "default": null}
    ]
}"#;

/// Serializes CDC events to JSON or Avro binary format.
///
/// Reuse a single `CdcEventSerializer` for all events to amortize schema parsing.
///
/// # Example
///
/// ```rust
/// use rivven_cdc::common::{SerializationConfig, CdcEventSerializer, CdcEvent, CdcOp};
///
/// let serializer = CdcEventSerializer::new(SerializationConfig::json());
/// let event = CdcEvent::insert("postgres", "mydb", "public", "users",
///     serde_json::json!({"id": 1, "name": "Alice"}), 1700000000);
/// let bytes = serializer.serialize(&event).unwrap();
/// ```
pub struct CdcEventSerializer {
    config: SerializationConfig,
    avro_schema: apache_avro::Schema,
}

impl CdcEventSerializer {
    /// Create a new serializer with the given configuration.
    ///
    /// Parses the Avro schema once at construction time.
    pub fn new(config: SerializationConfig) -> Self {
        let avro_schema = apache_avro::Schema::parse_str(CDC_AVRO_SCHEMA)
            .expect("built-in CDC Avro schema must be valid");
        Self {
            config,
            avro_schema,
        }
    }

    /// Serialize a CDC event to bytes according to the configured format.
    pub fn serialize(&self, event: &CdcEvent) -> Result<Vec<u8>, SerializationError> {
        match self.config.format {
            SerializationFormat::Json => self.serialize_json(event),
            SerializationFormat::Avro => self.serialize_avro(event),
        }
    }

    /// Serialize a CDC event with a Confluent-compatible schema ID prefix.
    ///
    /// Output: `[0x00][schema_id: 4 bytes big-endian][payload]`
    ///
    /// For JSON format, this prepends the header to the JSON bytes.
    /// For Avro format, this prepends the header to the Avro datum bytes.
    pub fn serialize_with_schema_id(
        &self,
        event: &CdcEvent,
        schema_id: u32,
    ) -> Result<Vec<u8>, SerializationError> {
        let payload = self.serialize(event)?;
        let mut result = Vec::with_capacity(5 + payload.len());
        result.push(0u8); // Confluent magic byte
        result.extend_from_slice(&schema_id.to_be_bytes());
        result.extend_from_slice(&payload);
        Ok(result)
    }

    /// Deserialize bytes back to a CDC event.
    pub fn deserialize(&self, data: &[u8]) -> Result<CdcEvent, SerializationError> {
        match self.config.format {
            SerializationFormat::Json => self.deserialize_json(data),
            SerializationFormat::Avro => self.deserialize_avro(data),
        }
    }

    /// Deserialize bytes with a Confluent schema ID prefix.
    ///
    /// Returns `(schema_id, event)`.
    pub fn deserialize_with_schema_id(
        &self,
        data: &[u8],
    ) -> Result<(u32, CdcEvent), SerializationError> {
        if data.len() < 5 {
            return Err(SerializationError::InvalidData(
                "data too short for Confluent wire format (need at least 5 bytes)".into(),
            ));
        }
        if data[0] != 0 {
            return Err(SerializationError::InvalidData(format!(
                "invalid Confluent magic byte: expected 0x00, got 0x{:02x}",
                data[0]
            )));
        }
        let schema_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let event = self.deserialize(&data[5..])?;
        Ok((schema_id, event))
    }

    /// Get a reference to the Avro schema used for CDC events.
    pub fn avro_schema(&self) -> &apache_avro::Schema {
        &self.avro_schema
    }

    /// Get the Avro schema as a JSON string (for registry registration).
    pub fn avro_schema_json(&self) -> String {
        self.avro_schema.canonical_form()
    }

    // -- Private implementation --

    fn serialize_json(&self, event: &CdcEvent) -> Result<Vec<u8>, SerializationError> {
        if self.config.pretty_json {
            serde_json::to_vec_pretty(event).map_err(|e| SerializationError::Json(e.to_string()))
        } else {
            serde_json::to_vec(event).map_err(|e| SerializationError::Json(e.to_string()))
        }
    }

    fn deserialize_json(&self, data: &[u8]) -> Result<CdcEvent, SerializationError> {
        serde_json::from_slice(data).map_err(|e| SerializationError::Json(e.to_string()))
    }

    fn serialize_avro(&self, event: &CdcEvent) -> Result<Vec<u8>, SerializationError> {
        let record = self.cdc_event_to_avro(event)?;
        apache_avro::to_avro_datum(&self.avro_schema, record)
            .map_err(|e| SerializationError::Avro(e.to_string()))
    }

    fn deserialize_avro(&self, data: &[u8]) -> Result<CdcEvent, SerializationError> {
        let mut cursor = std::io::Cursor::new(data);
        let value =
            apache_avro::from_avro_datum(&self.avro_schema, &mut cursor, Some(&self.avro_schema))
                .map_err(|e| SerializationError::Avro(e.to_string()))?;
        self.avro_to_cdc_event(value)
    }

    /// Convert a `CdcEvent` to an `apache_avro::types::Value::Record`.
    fn cdc_event_to_avro(
        &self,
        event: &CdcEvent,
    ) -> Result<apache_avro::types::Value, SerializationError> {
        use apache_avro::types::Value;

        let (op_idx, op_str) = match event.op {
            super::CdcOp::Insert => (0, "Insert"),
            super::CdcOp::Update => (1, "Update"),
            super::CdcOp::Delete => (2, "Delete"),
            super::CdcOp::Tombstone => (3, "Tombstone"),
            super::CdcOp::Truncate => (4, "Truncate"),
            super::CdcOp::Snapshot => (5, "Snapshot"),
            super::CdcOp::Schema => (6, "Schema"),
        };

        let before = match &event.before {
            Some(v) => Value::Union(1, Box::new(Value::String(v.to_string()))),
            None => Value::Union(0, Box::new(Value::Null)),
        };

        let after = match &event.after {
            Some(v) => Value::Union(1, Box::new(Value::String(v.to_string()))),
            None => Value::Union(0, Box::new(Value::Null)),
        };

        let transaction = match &event.transaction {
            Some(txn) => {
                let commit_ts = match txn.commit_ts {
                    Some(ts) => Value::Union(1, Box::new(Value::Long(ts))),
                    None => Value::Union(0, Box::new(Value::Null)),
                };
                let txn_record = Value::Record(vec![
                    ("id".to_string(), Value::String(txn.id.clone())),
                    ("lsn".to_string(), Value::String(txn.lsn.clone())),
                    ("sequence".to_string(), Value::Long(txn.sequence as i64)),
                    (
                        "total_events".to_string(),
                        Value::Long(txn.total_events as i64),
                    ),
                    ("commit_ts".to_string(), commit_ts),
                    ("is_last".to_string(), Value::Boolean(txn.is_last)),
                ]);
                Value::Union(1, Box::new(txn_record))
            }
            None => Value::Union(0, Box::new(Value::Null)),
        };

        Ok(Value::Record(vec![
            (
                "source_type".to_string(),
                Value::String(event.source_type.clone()),
            ),
            (
                "database".to_string(),
                Value::String(event.database.clone()),
            ),
            ("schema".to_string(), Value::String(event.schema.clone())),
            ("table".to_string(), Value::String(event.table.clone())),
            ("op".to_string(), Value::Enum(op_idx, op_str.to_string())),
            ("before".to_string(), before),
            ("after".to_string(), after),
            ("timestamp".to_string(), Value::Long(event.timestamp)),
            ("transaction".to_string(), transaction),
        ]))
    }

    /// Convert an `apache_avro::types::Value` back to a `CdcEvent`.
    fn avro_to_cdc_event(
        &self,
        value: apache_avro::types::Value,
    ) -> Result<CdcEvent, SerializationError> {
        use apache_avro::types::Value;

        let fields = match value {
            Value::Record(fields) => fields,
            _ => {
                return Err(SerializationError::Avro(
                    "expected Avro Record at top level".into(),
                ))
            }
        };

        let mut source_type = String::new();
        let mut database = String::new();
        let mut schema = String::new();
        let mut table = String::new();
        let mut op = super::CdcOp::Insert;
        let mut before = None;
        let mut after = None;
        let mut timestamp = 0i64;
        let mut transaction = None;

        for (name, val) in fields {
            match name.as_str() {
                "source_type" => {
                    if let Value::String(s) = val {
                        source_type = s;
                    }
                }
                "database" => {
                    if let Value::String(s) = val {
                        database = s;
                    }
                }
                "schema" => {
                    if let Value::String(s) = val {
                        schema = s;
                    }
                }
                "table" => {
                    if let Value::String(s) = val {
                        table = s;
                    }
                }
                "op" => {
                    if let Value::Enum(_, s) = val {
                        op = match s.as_str() {
                            "Insert" => super::CdcOp::Insert,
                            "Update" => super::CdcOp::Update,
                            "Delete" => super::CdcOp::Delete,
                            "Tombstone" => super::CdcOp::Tombstone,
                            "Truncate" => super::CdcOp::Truncate,
                            "Snapshot" => super::CdcOp::Snapshot,
                            "Schema" => super::CdcOp::Schema,
                            _ => super::CdcOp::Insert,
                        };
                    }
                }
                "before" => {
                    if let Value::Union(_, inner) = val {
                        if let Value::String(s) = *inner {
                            before = serde_json::from_str(&s).ok();
                        }
                    }
                }
                "after" => {
                    if let Value::Union(_, inner) = val {
                        if let Value::String(s) = *inner {
                            after = serde_json::from_str(&s).ok();
                        }
                    }
                }
                "timestamp" => {
                    if let Value::Long(ts) = val {
                        timestamp = ts;
                    }
                }
                "transaction" => {
                    if let Value::Union(_, inner) = val {
                        if let Value::Record(txn_fields) = *inner {
                            transaction = Some(Self::parse_txn_record(txn_fields));
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(CdcEvent {
            source_type,
            database,
            schema,
            table,
            op,
            before,
            after,
            timestamp,
            transaction,
        })
    }

    fn parse_txn_record(
        fields: Vec<(String, apache_avro::types::Value)>,
    ) -> super::TransactionMetadata {
        use apache_avro::types::Value;

        let mut id = String::new();
        let mut lsn = String::new();
        let mut sequence = 0u64;
        let mut total_events = 0u64;
        let mut commit_ts = None;
        let mut is_last = false;

        for (name, val) in fields {
            match name.as_str() {
                "id" => {
                    if let Value::String(s) = val {
                        id = s;
                    }
                }
                "lsn" => {
                    if let Value::String(s) = val {
                        lsn = s;
                    }
                }
                "sequence" => {
                    if let Value::Long(v) = val {
                        sequence = v as u64;
                    }
                }
                "total_events" => {
                    if let Value::Long(v) = val {
                        total_events = v as u64;
                    }
                }
                "commit_ts" => {
                    if let Value::Union(_, inner) = val {
                        if let Value::Long(ts) = *inner {
                            commit_ts = Some(ts);
                        }
                    }
                }
                "is_last" => {
                    if let Value::Boolean(b) = val {
                        is_last = b;
                    }
                }
                _ => {}
            }
        }

        super::TransactionMetadata {
            id,
            lsn,
            sequence,
            total_events,
            commit_ts,
            is_last,
        }
    }
}

/// Errors from serialization/deserialization.
#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("JSON serialization error: {0}")]
    Json(String),
    #[error("Avro serialization error: {0}")]
    Avro(String),
    #[error("Invalid data: {0}")]
    InvalidData(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{CdcOp, TransactionMetadata};

    #[test]
    fn test_format_parse() {
        assert_eq!(
            "json".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );
        assert_eq!(
            "avro".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Avro
        );
        assert_eq!(
            "JSON".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );
        assert_eq!(
            "AVRO".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Avro
        );
    }

    #[test]
    fn test_format_requires_schema() {
        assert!(!SerializationFormat::Json.requires_schema());
        assert!(SerializationFormat::Avro.requires_schema());
    }

    #[test]
    fn test_format_is_binary() {
        assert!(!SerializationFormat::Json.is_binary());
        assert!(SerializationFormat::Avro.is_binary());
    }

    #[test]
    fn test_config_builders() {
        let json = SerializationConfig::json();
        assert_eq!(json.format, SerializationFormat::Json);

        let avro = SerializationConfig::avro();
        assert_eq!(avro.format, SerializationFormat::Avro);
        assert!(avro.confluent_wire_format);
    }

    #[test]
    fn test_config_defaults() {
        let config = SerializationConfig::default();
        assert_eq!(config.format, SerializationFormat::Json);
        assert!(config.confluent_wire_format);
        assert!(config.auto_register_schemas);
        assert!(!config.pretty_json);
        assert!(config.include_nulls);
    }

    // =========================================================================
    // Serializer tests
    // =========================================================================

    fn sample_event() -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "mydb".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            timestamp: 1700000000,
            transaction: None,
        }
    }

    fn sample_event_with_transaction() -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "mydb".to_string(),
            schema: "public".to_string(),
            table: "orders".to_string(),
            op: CdcOp::Update,
            before: Some(serde_json::json!({"id": 42, "status": "pending"})),
            after: Some(serde_json::json!({"id": 42, "status": "shipped"})),
            timestamp: 1700000100,
            transaction: Some(TransactionMetadata {
                id: "txn-001".to_string(),
                lsn: "0/1234ABCD".to_string(),
                sequence: 3,
                total_events: 5,
                commit_ts: Some(1700000099),
                is_last: false,
            }),
        }
    }

    #[test]
    fn test_serializer_json_roundtrip() {
        let ser = CdcEventSerializer::new(SerializationConfig::json());
        let event = sample_event();
        let bytes = ser.serialize(&event).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.source_type, event.source_type);
        assert_eq!(decoded.database, event.database);
        assert_eq!(decoded.table, event.table);
        assert_eq!(decoded.op, event.op);
        assert_eq!(decoded.before, event.before);
        assert_eq!(decoded.after, event.after);
        assert_eq!(decoded.timestamp, event.timestamp);
        assert!(decoded.transaction.is_none());
    }

    #[test]
    fn test_serializer_json_with_transaction() {
        let ser = CdcEventSerializer::new(SerializationConfig::json());
        let event = sample_event_with_transaction();
        let bytes = ser.serialize(&event).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.op, CdcOp::Update);
        let txn = decoded.transaction.unwrap();
        assert_eq!(txn.id, "txn-001");
        assert_eq!(txn.lsn, "0/1234ABCD");
        assert_eq!(txn.sequence, 3);
        assert_eq!(txn.total_events, 5);
        assert_eq!(txn.commit_ts, Some(1700000099));
        assert!(!txn.is_last);
    }

    #[test]
    fn test_serializer_avro_roundtrip() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let event = sample_event();
        let bytes = ser.serialize(&event).unwrap();
        // Avro binary should be compact
        assert!(
            bytes.len() < 200,
            "Avro output too large: {} bytes",
            bytes.len()
        );
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.source_type, event.source_type);
        assert_eq!(decoded.database, event.database);
        assert_eq!(decoded.schema, event.schema);
        assert_eq!(decoded.table, event.table);
        assert_eq!(decoded.op, event.op);
        assert_eq!(decoded.before, event.before);
        assert_eq!(decoded.after, event.after);
        assert_eq!(decoded.timestamp, event.timestamp);
    }

    #[test]
    fn test_serializer_avro_with_transaction() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let event = sample_event_with_transaction();
        let bytes = ser.serialize(&event).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.op, CdcOp::Update);
        assert!(decoded.before.is_some());
        assert!(decoded.after.is_some());
        let txn = decoded.transaction.unwrap();
        assert_eq!(txn.id, "txn-001");
        assert_eq!(txn.lsn, "0/1234ABCD");
        assert_eq!(txn.sequence, 3);
        assert_eq!(txn.total_events, 5);
        assert_eq!(txn.commit_ts, Some(1700000099));
        assert!(!txn.is_last);
    }

    #[test]
    fn test_serializer_all_ops_roundtrip() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let ops = [
            CdcOp::Insert,
            CdcOp::Update,
            CdcOp::Delete,
            CdcOp::Tombstone,
            CdcOp::Truncate,
            CdcOp::Snapshot,
            CdcOp::Schema,
        ];
        for op in &ops {
            let mut event = sample_event();
            event.op = *op;
            let bytes = ser.serialize(&event).unwrap();
            let decoded = ser.deserialize(&bytes).unwrap();
            assert_eq!(decoded.op, *op, "Op roundtrip failed for {:?}", op);
        }
    }

    #[test]
    fn test_confluent_wire_format_json() {
        let ser = CdcEventSerializer::new(SerializationConfig::json());
        let event = sample_event();
        let schema_id = 42u32;
        let bytes = ser.serialize_with_schema_id(&event, schema_id).unwrap();
        // Check header
        assert_eq!(bytes[0], 0x00, "magic byte");
        assert_eq!(
            u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]),
            42
        );
        // Deserialize back
        let (id, decoded) = ser.deserialize_with_schema_id(&bytes).unwrap();
        assert_eq!(id, 42);
        assert_eq!(decoded.table, "users");
    }

    #[test]
    fn test_confluent_wire_format_avro() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let event = sample_event_with_transaction();
        let schema_id = 0xDEAD_BEEF;
        let bytes = ser.serialize_with_schema_id(&event, schema_id).unwrap();
        assert_eq!(bytes[0], 0x00);
        let (id, decoded) = ser.deserialize_with_schema_id(&bytes).unwrap();
        assert_eq!(id, schema_id);
        assert_eq!(decoded.table, "orders");
        assert!(decoded.transaction.is_some());
    }

    #[test]
    fn test_confluent_wire_format_too_short() {
        let ser = CdcEventSerializer::new(SerializationConfig::json());
        let result = ser.deserialize_with_schema_id(&[0, 1, 2]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_confluent_wire_format_bad_magic() {
        let ser = CdcEventSerializer::new(SerializationConfig::json());
        let result = ser.deserialize_with_schema_id(&[0xFF, 0, 0, 0, 1]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic byte"));
    }

    #[test]
    fn test_avro_schema_accessible() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let schema = ser.avro_schema();
        assert!(matches!(schema, apache_avro::Schema::Record(_)));
        let json = ser.avro_schema_json();
        assert!(json.contains("CdcEvent"));
        assert!(json.contains("io.rivven.cdc"));
    }

    #[test]
    fn test_avro_smaller_than_json() {
        let ser_json = CdcEventSerializer::new(SerializationConfig::json());
        let ser_avro = CdcEventSerializer::new(SerializationConfig::avro());
        let event = sample_event_with_transaction();
        let json_bytes = ser_json.serialize(&event).unwrap();
        let avro_bytes = ser_avro.serialize(&event).unwrap();
        assert!(
            avro_bytes.len() < json_bytes.len(),
            "Avro ({} bytes) should be smaller than JSON ({} bytes)",
            avro_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn test_delete_event_with_before_only() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "mydb".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op: CdcOp::Delete,
            before: Some(serde_json::json!({"id": 99})),
            after: None,
            timestamp: 1700000200,
            transaction: None,
        };
        let bytes = ser.serialize(&event).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.op, CdcOp::Delete);
        assert!(decoded.before.is_some());
        assert!(decoded.after.is_none());
    }

    #[test]
    fn test_transaction_without_commit_ts() {
        let ser = CdcEventSerializer::new(SerializationConfig::avro());
        let event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "db".to_string(),
            schema: "s".to_string(),
            table: "t".to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"x": 1})),
            timestamp: 1,
            transaction: Some(TransactionMetadata {
                id: "tx".to_string(),
                lsn: "0/FF".to_string(),
                sequence: 1,
                total_events: 1,
                commit_ts: None,
                is_last: true,
            }),
        };
        let bytes = ser.serialize(&event).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        let txn = decoded.transaction.unwrap();
        assert!(txn.commit_ts.is_none());
        assert!(txn.is_last);
    }
}

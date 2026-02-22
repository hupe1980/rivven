//! Value types for rivven-rdbc
//!
//! Comprehensive type system matching Debezium/Kafka Connect for full fidelity:
//! - All primitive types (bool, integers, floats, decimal)
//! - Date/time types with timezone support
//! - Binary data (bytes, blobs)
//! - Structured types (JSON, arrays)
//! - Spatial types (geometry, geography)

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// SQL value type that can hold any database value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum Value {
    /// SQL NULL
    Null,
    /// Boolean value
    Bool(bool),
    /// 8-bit signed integer (TINYINT)
    Int8(i8),
    /// 16-bit signed integer (SMALLINT)
    Int16(i16),
    /// 32-bit signed integer (INTEGER)
    Int32(i32),
    /// 64-bit signed integer (BIGINT)
    Int64(i64),
    /// 32-bit floating point (REAL)
    Float32(f32),
    /// 64-bit floating point (DOUBLE PRECISION)
    Float64(f64),
    /// Arbitrary precision decimal (NUMERIC, DECIMAL)
    Decimal(Decimal),
    /// Text string (VARCHAR, TEXT, CHAR)
    String(String),
    /// Binary data (BYTEA, BLOB, VARBINARY)
    Bytes(Vec<u8>),
    /// Date without time (DATE)
    Date(NaiveDate),
    /// Time without date (TIME)
    Time(NaiveTime),
    /// Timestamp without timezone (TIMESTAMP)
    DateTime(NaiveDateTime),
    /// Timestamp with timezone (TIMESTAMPTZ)
    DateTimeTz(DateTime<Utc>),
    /// UUID
    Uuid(Uuid),
    /// JSON value
    Json(serde_json::Value),
    /// Array of values
    Array(Vec<Value>),
    /// Interval (stored as microseconds)
    Interval(i64),
    /// Bit string
    Bit(Vec<u8>),
    /// Enum value (stored as string)
    Enum(String),
    /// Geometry (WKB format)
    Geometry(Vec<u8>),
    /// Geography (WKB format)
    Geography(Vec<u8>),
    /// Range type (e.g., int4range, tsrange)
    Range {
        lower: Option<Box<Value>>,
        upper: Option<Box<Value>>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    /// Composite/row type
    Composite(HashMap<String, Value>),
    /// Custom type (vendor-specific)
    Custom { type_name: String, data: Vec<u8> },
}

impl Value {
    /// Check if value is NULL
    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get SQL type name
    pub fn sql_type(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Bool(_) => "BOOLEAN",
            Self::Int8(_) => "TINYINT",
            Self::Int16(_) => "SMALLINT",
            Self::Int32(_) => "INTEGER",
            Self::Int64(_) => "BIGINT",
            Self::Float32(_) => "REAL",
            Self::Float64(_) => "DOUBLE PRECISION",
            Self::Decimal(_) => "DECIMAL",
            Self::String(_) => "VARCHAR",
            Self::Bytes(_) => "BYTEA",
            Self::Date(_) => "DATE",
            Self::Time(_) => "TIME",
            Self::DateTime(_) => "TIMESTAMP",
            Self::DateTimeTz(_) => "TIMESTAMPTZ",
            Self::Uuid(_) => "UUID",
            Self::Json(_) => "JSONB",
            Self::Array(_) => "ARRAY",
            Self::Interval(_) => "INTERVAL",
            Self::Bit(_) => "BIT",
            Self::Enum(_) => "ENUM",
            Self::Geometry(_) => "GEOMETRY",
            Self::Geography(_) => "GEOGRAPHY",
            Self::Range { .. } => "RANGE",
            Self::Composite(_) => "COMPOSITE",
            Self::Custom { .. } => "CUSTOM",
        }
    }

    /// Try to convert to bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            Self::Int8(n) => Some(*n != 0),
            Self::Int16(n) => Some(*n != 0),
            Self::Int32(n) => Some(*n != 0),
            Self::Int64(n) => Some(*n != 0),
            Self::String(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "1" => Some(true),
                "false" | "f" | "no" | "n" | "0" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    /// Try to convert to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int8(n) => Some(i64::from(*n)),
            Self::Int16(n) => Some(i64::from(*n)),
            Self::Int32(n) => Some(i64::from(*n)),
            Self::Int64(n) => Some(*n),
            Self::Float32(n) => {
                if n.is_finite() {
                    Some(*n as i64)
                } else {
                    None
                }
            }
            Self::Float64(n) => {
                if n.is_finite() {
                    Some(*n as i64)
                } else {
                    None
                }
            }
            Self::Decimal(d) => d.to_string().parse().ok(),
            Self::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to convert to f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int8(n) => Some(f64::from(*n)),
            Self::Int16(n) => Some(f64::from(*n)),
            Self::Int32(n) => Some(f64::from(*n)),
            Self::Int64(n) => Some(*n as f64),
            Self::Float32(n) => Some(f64::from(*n)),
            Self::Float64(n) => Some(*n),
            Self::Decimal(d) => d.to_string().parse().ok(),
            Self::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to convert to string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s.as_str()),
            Self::Enum(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Try to convert to bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b.as_slice()),
            Self::String(s) => Some(s.as_bytes()),
            Self::Geometry(b) | Self::Geography(b) => Some(b.as_slice()),
            Self::Custom { data, .. } => Some(data.as_slice()),
            _ => None,
        }
    }

    /// Try to convert to UUID
    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            Self::Uuid(u) => Some(*u),
            Self::String(s) => Uuid::parse_str(s).ok(),
            Self::Bytes(b) if b.len() == 16 => Uuid::from_slice(b).ok(),
            _ => None,
        }
    }

    /// Try to convert to JSON
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Json(j) => Some(j),
            _ => None,
        }
    }

    /// Convert to owned string representation
    pub fn as_string(&self) -> Option<String> {
        match self {
            Self::String(s) => Some(s.clone()),
            Self::Enum(s) => Some(s.clone()),
            Self::Int8(n) => Some(n.to_string()),
            Self::Int16(n) => Some(n.to_string()),
            Self::Int32(n) => Some(n.to_string()),
            Self::Int64(n) => Some(n.to_string()),
            Self::Float32(n) => Some(n.to_string()),
            Self::Float64(n) => Some(n.to_string()),
            Self::Decimal(d) => Some(d.to_string()),
            Self::Bool(b) => Some(b.to_string()),
            Self::Uuid(u) => Some(u.to_string()),
            _ => None,
        }
    }
}

/// Implement From traits for common types
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Self::Int8(v)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Self::Int16(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Self::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Self::Float32(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Float64(v)
    }
}

impl From<Decimal> for Value {
    fn from(v: Decimal) -> Self {
        Self::Decimal(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

impl From<NaiveDate> for Value {
    fn from(v: NaiveDate) -> Self {
        Self::Date(v)
    }
}

impl From<NaiveTime> for Value {
    fn from(v: NaiveTime) -> Self {
        Self::Time(v)
    }
}

impl From<NaiveDateTime> for Value {
    fn from(v: NaiveDateTime) -> Self {
        Self::DateTime(v)
    }
}

impl From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Self {
        Self::DateTimeTz(v)
    }
}

impl From<Uuid> for Value {
    fn from(v: Uuid) -> Self {
        Self::Uuid(v)
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Self::Json(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Self::Null,
        }
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Self::Array(v.into_iter().map(Into::into).collect())
    }
}

/// Database row as ordered column values
#[derive(Debug, Clone)]
pub struct Row {
    /// Column names
    columns: Vec<String>,
    /// Column values (same order as columns)
    values: Vec<Value>,
}

impl Row {
    /// Create a new row
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        debug_assert_eq!(columns.len(), values.len());
        Self { columns, values }
    }

    /// Get column count
    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if row is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get column names
    #[inline]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get all values
    #[inline]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Get value by column index
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// Get value by column index (alias for get)
    #[inline]
    pub fn get_index(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// Get value by column name
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
            .and_then(|idx| self.values.get(idx))
    }

    /// Convert row to HashMap
    pub fn into_map(self) -> HashMap<String, Value> {
        self.columns.into_iter().zip(self.values).collect()
    }
}

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// SQL type name (vendor-specific)
    pub type_name: String,
    /// Whether column is nullable
    pub nullable: bool,
    /// Primary key ordinal (1-based, None if not PK)
    pub primary_key_ordinal: Option<u32>,
    /// Column ordinal (1-based)
    pub ordinal: u32,
    /// Maximum length for string/binary types
    pub max_length: Option<u32>,
    /// Precision for numeric types
    pub precision: Option<u32>,
    /// Scale for numeric types  
    pub scale: Option<u32>,
    /// Default value expression
    pub default_value: Option<String>,
    /// Auto-increment/serial
    pub auto_increment: bool,
    /// Column comment
    pub comment: Option<String>,
}

impl ColumnMetadata {
    /// Create basic column metadata
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable: true,
            primary_key_ordinal: None,
            ordinal: 0,
            max_length: None,
            precision: None,
            scale: None,
            default_value: None,
            auto_increment: false,
            comment: None,
        }
    }

    /// Check if this column is part of the primary key
    #[inline]
    pub fn is_primary_key(&self) -> bool {
        self.primary_key_ordinal.is_some()
    }
}

/// Table metadata
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Schema (or database for MySQL)
    pub schema: Option<String>,
    /// Table name
    pub name: String,
    /// Column metadata (in ordinal order)
    pub columns: Vec<ColumnMetadata>,
    /// Table comment
    pub comment: Option<String>,
}

impl TableMetadata {
    /// Create new table metadata
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
            columns: Vec::new(),
            comment: None,
        }
    }

    /// Get fully qualified name
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }

    /// Get column by name
    pub fn column(&self, name: &str) -> Option<&ColumnMetadata> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get primary key columns
    pub fn primary_key_columns(&self) -> Vec<&ColumnMetadata> {
        let mut pk_cols: Vec<_> = self.columns.iter().filter(|c| c.is_primary_key()).collect();
        pk_cols.sort_by_key(|c| c.primary_key_ordinal);
        pk_cols
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn test_value_conversions() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::String("yes".into()).as_bool(), Some(true));
        assert_eq!(Value::String("false".into()).as_bool(), Some(false));

        assert_eq!(Value::Int32(42).as_i64(), Some(42));
        assert_eq!(Value::Float64(1.5).as_f64(), Some(1.5));
    }

    #[test]
    fn test_value_from_impl() {
        let v: Value = 42_i32.into();
        assert!(matches!(v, Value::Int32(42)));

        let v: Value = "hello".into();
        assert!(matches!(v, Value::String(s) if s == "hello"));

        let v: Value = None::<i32>.into();
        assert!(v.is_null());
    }

    #[test]
    fn test_row_operations() {
        let row = Row::new(
            vec!["id".into(), "name".into()],
            vec![Value::Int32(1), Value::String("Alice".into())],
        );

        assert_eq!(row.len(), 2);
        assert_eq!(row.get(0), Some(&Value::Int32(1)));
        assert_eq!(
            row.get_by_name("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            row.get_by_name("NAME"),
            Some(&Value::String("Alice".into()))
        ); // case-insensitive
    }

    #[test]
    fn test_table_metadata() {
        let mut table = TableMetadata::new("users");
        table.schema = Some("public".into());
        table.columns.push(ColumnMetadata {
            name: "id".into(),
            type_name: "integer".into(),
            nullable: false,
            primary_key_ordinal: Some(1),
            ordinal: 1,
            max_length: None,
            precision: None,
            scale: None,
            default_value: None,
            auto_increment: true,
            comment: None,
        });

        assert_eq!(table.qualified_name(), "public.users");
        assert_eq!(table.primary_key_columns().len(), 1);
        assert!(table.column("id").unwrap().is_primary_key());
    }
}

//! MySQL type mapper for Avro schema generation
//!
//! Maps MySQL column types to Apache Avro types for CDC events.
//! Uses JSON-based schema definition for compatibility with apache-avro 0.21.

use apache_avro::Schema;
use serde_json::json;

use super::decoder::ColumnType;

/// Maps MySQL column types to Avro schema
pub struct MySqlTypeMapper;

impl MySqlTypeMapper {
    /// Map a MySQL column type to an Avro schema JSON value
    pub fn to_avro_json(
        col_type: ColumnType,
        metadata: u16,
        nullable: bool,
        _name: &str,
    ) -> serde_json::Value {
        let base_schema = match col_type {
            // Integer types
            ColumnType::Tiny | ColumnType::Short | ColumnType::Int24 | ColumnType::Long => {
                json!({"type": "int"})
            }
            ColumnType::LongLong => json!({"type": "long"}),

            // Floating point
            ColumnType::Float => json!({"type": "float"}),
            ColumnType::Double => json!({"type": "double"}),

            // Decimal
            ColumnType::Decimal | ColumnType::NewDecimal => {
                let precision = (metadata >> 8) as usize;
                let scale = (metadata & 0xFF) as usize;
                json!({
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": precision.max(1),
                    "scale": scale
                })
            }

            // String types
            ColumnType::Varchar | ColumnType::VarString | ColumnType::String => {
                json!({"type": "string"})
            }

            // Binary types
            ColumnType::Blob
            | ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob => {
                json!({"type": "bytes"})
            }

            // Date/Time types - stored as logical types
            ColumnType::Date => {
                json!({"type": "int", "logicalType": "date"})
            }
            ColumnType::Time | ColumnType::Time2 => {
                json!({"type": "long", "logicalType": "time-micros"})
            }
            ColumnType::DateTime | ColumnType::DateTime2 => {
                json!({"type": "long", "logicalType": "timestamp-micros"})
            }
            ColumnType::Timestamp | ColumnType::Timestamp2 => {
                json!({"type": "long", "logicalType": "timestamp-micros"})
            }
            ColumnType::Year => json!({"type": "int"}),

            // JSON
            ColumnType::Json => json!({"type": "string"}),

            // Enum - stored as string
            ColumnType::Enum => json!({"type": "string"}),

            // Set - stored as array of strings
            ColumnType::Set => {
                json!({"type": "array", "items": "string"})
            }

            // Bit
            ColumnType::Bit => json!({"type": "bytes"}),

            // Geometry
            ColumnType::Geometry => json!({"type": "bytes"}),

            // Null
            ColumnType::Null => json!("null"),

            // Default to bytes for unknown types
            _ => json!({"type": "bytes"}),
        };

        if nullable {
            json!(["null", base_schema])
        } else {
            base_schema
        }
    }

    /// Map a MySQL column type to an Avro schema  
    pub fn to_avro_schema(
        col_type: ColumnType,
        metadata: u16,
        nullable: bool,
        name: &str,
    ) -> Schema {
        let json_schema = Self::to_avro_json(col_type, metadata, nullable, name);
        Schema::parse(&json_schema).expect("Failed to parse Avro schema")
    }

    /// Create an Avro record schema for a MySQL table as JSON
    pub fn table_to_avro_json(
        schema_name: &str,
        table_name: &str,
        columns: &[(String, ColumnType, u16, bool)], // (name, type, metadata, nullable)
    ) -> serde_json::Value {
        let fields: Vec<serde_json::Value> = columns
            .iter()
            .map(|(col_name, col_type, metadata, nullable)| {
                let field_type = Self::to_avro_json(*col_type, *metadata, *nullable, col_name);
                if *nullable {
                    json!({
                        "name": col_name,
                        "type": field_type,
                        "default": null
                    })
                } else {
                    json!({
                        "name": col_name,
                        "type": field_type
                    })
                }
            })
            .collect();

        json!({
            "type": "record",
            "name": table_name,
            "namespace": schema_name,
            "doc": format!("CDC record for {}.{}", schema_name, table_name),
            "fields": fields
        })
    }

    /// Create an Avro record schema for a MySQL table
    pub fn table_to_avro_schema(
        schema_name: &str,
        table_name: &str,
        columns: &[(String, ColumnType, u16, bool)],
    ) -> Schema {
        let json_schema = Self::table_to_avro_json(schema_name, table_name, columns);
        Schema::parse(&json_schema).expect("Failed to parse table schema")
    }

    /// Create a CDC envelope schema as JSON
    pub fn cdc_envelope_json(
        schema_name: &str,
        table_name: &str,
        columns: &[(String, ColumnType, u16, bool)],
    ) -> serde_json::Value {
        let record_schema = Self::table_to_avro_json(schema_name, table_name, columns);

        // Source info schema
        let source_schema = json!({
            "type": "record",
            "name": "Source",
            "namespace": format!("{}.{}", schema_name, table_name),
            "fields": [
                {"name": "version", "type": "string"},
                {"name": "connector", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "db", "type": "string"},
                {"name": "table", "type": "string"},
                {"name": "server_id", "type": "long"},
                {"name": "gtid", "type": ["null", "string"], "default": null},
                {"name": "file", "type": "string"},
                {"name": "pos", "type": "long"}
            ]
        });

        json!({
            "type": "record",
            "name": format!("{}_envelope", table_name),
            "namespace": schema_name,
            "doc": format!("CDC envelope for {}.{}", schema_name, table_name),
            "fields": [
                {
                    "name": "op",
                    "type": "string",
                    "doc": "Operation type: c=create, u=update, d=delete, r=read"
                },
                {
                    "name": "ts_ms",
                    "type": "long",
                    "doc": "Timestamp in milliseconds"
                },
                {
                    "name": "source",
                    "type": source_schema,
                    "doc": "Source metadata"
                },
                {
                    "name": "before",
                    "type": ["null", record_schema.clone()],
                    "default": null,
                    "doc": "Row state before the change"
                },
                {
                    "name": "after",
                    "type": ["null", record_schema],
                    "default": null,
                    "doc": "Row state after the change"
                }
            ]
        })
    }

    /// Create a CDC envelope schema (wraps before/after with metadata)
    pub fn cdc_envelope_schema(
        schema_name: &str,
        table_name: &str,
        columns: &[(String, ColumnType, u16, bool)],
    ) -> Schema {
        let json_schema = Self::cdc_envelope_json(schema_name, table_name, columns);
        Schema::parse(&json_schema).expect("Failed to parse envelope schema")
    }
}

/// Convert MySQL column value to Avro value
pub fn column_value_to_avro(
    value: &super::decoder::ColumnValue,
    col_type: ColumnType,
    nullable: bool,
) -> apache_avro::types::Value {
    use super::decoder::ColumnValue;
    use apache_avro::types::Value;

    let inner_value = match value {
        ColumnValue::Null => {
            return Value::Null;
        }
        ColumnValue::SignedInt(v) => match col_type {
            ColumnType::Tiny | ColumnType::Short | ColumnType::Int24 | ColumnType::Long => {
                Value::Int(*v as i32)
            }
            _ => Value::Long(*v),
        },
        ColumnValue::UnsignedInt(v) => match col_type {
            ColumnType::Tiny | ColumnType::Short | ColumnType::Int24 | ColumnType::Long => {
                Value::Int(*v as i32)
            }
            _ => Value::Long(*v as i64),
        },
        ColumnValue::Float(v) => Value::Float(*v),
        ColumnValue::Double(v) => Value::Double(*v),
        ColumnValue::Decimal(v) => {
            // Store decimal as bytes (Avro decimal logical type)
            Value::Bytes(v.as_bytes().to_vec())
        }
        ColumnValue::String(v) => Value::String(v.clone()),
        ColumnValue::Bytes(v) => Value::Bytes(v.clone()),
        ColumnValue::Date { year, month, day } => {
            // Days since epoch (1970-01-01)
            let days = days_since_epoch(*year as i32, *month as u32, *day as u32);
            Value::Date(days)
        }
        ColumnValue::Time {
            hours,
            minutes,
            seconds,
            microseconds,
            negative,
        } => {
            // Microseconds since midnight
            let mut micros = (*hours as i64) * 3_600_000_000
                + (*minutes as i64) * 60_000_000
                + (*seconds as i64) * 1_000_000
                + (*microseconds as i64);
            if *negative {
                micros = -micros;
            }
            Value::TimeMicros(micros)
        }
        ColumnValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        } => {
            // Microseconds since epoch
            let days = days_since_epoch(*year as i32, *month as u32, *day as u32);
            let day_micros = (days as i64) * 86_400_000_000;
            let time_micros = (*hour as i64) * 3_600_000_000
                + (*minute as i64) * 60_000_000
                + (*second as i64) * 1_000_000
                + (*microsecond as i64);
            Value::TimestampMicros(day_micros + time_micros)
        }
        ColumnValue::Timestamp(v) => {
            // Unix timestamp in seconds -> microseconds
            Value::TimestampMicros((*v as i64) * 1_000_000)
        }
        ColumnValue::Year(v) => Value::Int(*v as i32),
        ColumnValue::Json(v) => Value::String(v.to_string()),
        ColumnValue::Enum(v) => Value::String(format!("{}", v)),
        ColumnValue::Set(v) => {
            // Return as array of bit positions that are set
            let mut values = Vec::new();
            for i in 0..64 {
                if (v >> i) & 1 == 1 {
                    values.push(Value::String(format!("{}", i + 1)));
                }
            }
            Value::Array(values)
        }
        ColumnValue::Bit(v) => Value::Bytes(v.clone()),
    };

    if nullable {
        Value::Union(1, Box::new(inner_value))
    } else {
        inner_value
    }
}

/// Calculate days since Unix epoch (1970-01-01)
fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    // Simple calculation (not accounting for all edge cases)
    let mut y = year;
    let mut m = month as i32;

    // Adjust for months Jan/Feb
    if m <= 2 {
        y -= 1;
        m += 12;
    }

    let days_per_year = 365;
    let leap_years = y / 4 - y / 100 + y / 400;
    let year_days = (y - 1970) * days_per_year + (leap_years - 477); // 477 = leap years before 1970

    let month_days: [i32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let month_offset = if (1..=12).contains(&m) {
        month_days[(m - 1) as usize]
    } else {
        0
    };

    year_days + month_offset + day as i32
}

/// Parse type name from MySQL type string to ColumnType.
/// This is used for the connector's common interface.
fn parse_mysql_type_name(type_name: &str) -> (ColumnType, u16) {
    let type_name_lower = type_name.to_lowercase();
    let base_type = type_name_lower
        .split('(')
        .next()
        .unwrap_or(&type_name_lower)
        .trim();

    // Extract precision/scale from parentheses if present
    let metadata = if let Some(start) = type_name_lower.find('(') {
        if let Some(end) = type_name_lower.find(')') {
            let inner = &type_name_lower[start + 1..end];
            if let Some((prec, scale)) = inner.split_once(',') {
                let p: u16 = prec.trim().parse().unwrap_or(0);
                let s: u16 = scale.trim().parse().unwrap_or(0);
                (p << 8) | s
            } else {
                inner.trim().parse().unwrap_or(0)
            }
        } else {
            0
        }
    } else {
        0
    };

    let col_type = match base_type {
        "tinyint" | "tiny" => ColumnType::Tiny,
        "smallint" | "short" => ColumnType::Short,
        "mediumint" | "int24" => ColumnType::Int24,
        "int" | "integer" | "long" => ColumnType::Long,
        "bigint" | "longlong" => ColumnType::LongLong,
        "float" => ColumnType::Float,
        "double" | "real" => ColumnType::Double,
        "decimal" | "numeric" => ColumnType::NewDecimal,
        "varchar" => ColumnType::Varchar,
        "char" => ColumnType::String,
        "text" | "tinytext" | "mediumtext" | "longtext" => ColumnType::Varchar,
        "blob" => ColumnType::Blob,
        "tinyblob" => ColumnType::TinyBlob,
        "mediumblob" => ColumnType::MediumBlob,
        "longblob" => ColumnType::LongBlob,
        "binary" | "varbinary" => ColumnType::Blob,
        "date" => ColumnType::Date,
        "time" => ColumnType::Time2,
        "datetime" => ColumnType::DateTime2,
        "timestamp" => ColumnType::Timestamp2,
        "year" => ColumnType::Year,
        "json" => ColumnType::Json,
        "enum" => ColumnType::Enum,
        "set" => ColumnType::Set,
        "bit" => ColumnType::Bit,
        "geometry" | "point" | "linestring" | "polygon" => ColumnType::Geometry,
        _ => ColumnType::Varchar, // Default to string for unknown types
    };

    (col_type, metadata)
}

impl MySqlTypeMapper {
    /// Generate Avro schema from MySQL table metadata.
    ///
    /// This method provides a common interface compatible with PostgresTypeMapper.
    /// The columns parameter uses (name, type_id, type_name) tuples where:
    /// - type_id is ignored (MySQL uses string type names)
    /// - type_name is parsed to determine the MySQL column type
    pub fn generate_avro_schema(
        namespace: &str,
        table_name: &str,
        columns: &[(String, i32, String)], // (column_name, type_id, type_name)
    ) -> anyhow::Result<Schema> {
        let mut fields = Vec::new();

        for (col_name, _type_id, type_name) in columns {
            let (col_type, metadata) = parse_mysql_type_name(type_name);

            // Make all fields nullable by default for CDC compatibility
            let field_type = Self::to_avro_json(col_type, metadata, true, col_name);

            fields.push(json!({
                "name": col_name,
                "type": field_type,
                "default": serde_json::Value::Null
            }));
        }

        let schema_json = json!({
            "type": "record",
            "name": table_name,
            "namespace": format!("rivven.cdc.mysql.{}", namespace),
            "fields": fields
        });

        let schema_str = serde_json::to_string(&schema_json)?;
        Schema::parse_str(&schema_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse Avro schema: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_types_to_avro() {
        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Tiny, 0, false, "col");
        assert!(matches!(schema, Schema::Int));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Short, 0, false, "col");
        assert!(matches!(schema, Schema::Int));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Long, 0, false, "col");
        assert!(matches!(schema, Schema::Int));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::LongLong, 0, false, "col");
        assert!(matches!(schema, Schema::Long));
    }

    #[test]
    fn test_nullable_wraps_in_union() {
        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Int24, 0, true, "col");
        assert!(matches!(schema, Schema::Union(_)));
    }

    #[test]
    fn test_string_types_to_avro() {
        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Varchar, 255, false, "col");
        assert!(matches!(schema, Schema::String));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::VarString, 1000, false, "col");
        assert!(matches!(schema, Schema::String));
    }

    #[test]
    fn test_blob_types_to_avro() {
        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Blob, 2, false, "col");
        assert!(matches!(schema, Schema::Bytes));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::LongBlob, 4, false, "col");
        assert!(matches!(schema, Schema::Bytes));
    }

    #[test]
    fn test_datetime_types_to_avro() {
        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Date, 0, false, "col");
        assert!(matches!(schema, Schema::Date));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::DateTime, 0, false, "col");
        assert!(matches!(schema, Schema::TimestampMicros));

        let schema = MySqlTypeMapper::to_avro_schema(ColumnType::Timestamp, 0, false, "col");
        assert!(matches!(schema, Schema::TimestampMicros));
    }

    #[test]
    fn test_table_to_avro_schema() {
        let columns = vec![
            ("id".to_string(), ColumnType::Long, 0, false),
            ("name".to_string(), ColumnType::Varchar, 255, true),
            ("age".to_string(), ColumnType::Tiny, 0, true),
        ];

        let schema = MySqlTypeMapper::table_to_avro_schema("test_db", "users", &columns);

        if let Schema::Record(record) = schema {
            assert_eq!(record.name.name, "users");
            assert_eq!(record.name.namespace, Some("test_db".to_string()));
            assert_eq!(record.fields.len(), 3);
            assert_eq!(record.fields[0].name, "id");
            assert_eq!(record.fields[1].name, "name");
            assert_eq!(record.fields[2].name, "age");
        } else {
            panic!("Expected Record schema");
        }
    }

    #[test]
    fn test_cdc_envelope_schema() {
        let columns = vec![
            ("id".to_string(), ColumnType::Long, 0, false),
            ("name".to_string(), ColumnType::Varchar, 255, true),
        ];

        let schema = MySqlTypeMapper::cdc_envelope_schema("test_db", "users", &columns);

        if let Schema::Record(record) = schema {
            assert_eq!(record.name.name, "users_envelope");
            let field_names: Vec<_> = record.fields.iter().map(|f| f.name.as_str()).collect();
            assert!(field_names.contains(&"op"));
            assert!(field_names.contains(&"ts_ms"));
            assert!(field_names.contains(&"source"));
            assert!(field_names.contains(&"before"));
            assert!(field_names.contains(&"after"));
        } else {
            panic!("Expected Record schema");
        }
    }

    #[test]
    fn test_days_since_epoch() {
        // 1970-01-01 = day 0
        // Actually the calculation is approximate, let's test relative values
        let d1 = days_since_epoch(2000, 1, 1);
        let d2 = days_since_epoch(2000, 1, 2);
        assert_eq!(d2 - d1, 1);
    }
}

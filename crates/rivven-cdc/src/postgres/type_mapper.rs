//! PostgreSQL type mapper
//!
//! Maps PostgreSQL OID types to Avro schema types.

use anyhow::{Context, Result};
use apache_avro::Schema as AvroSchema;
use serde_json::json;

/// Maps PostgreSQL OID types to Avro schema types
///
/// Reference: <https://www.postgresql.org/docs/current/datatype.html>
pub struct PostgresTypeMapper;

impl PostgresTypeMapper {
    /// Convert PostgreSQL type OID to Avro type JSON
    pub fn pg_type_to_avro(type_oid: i32, type_name: &str) -> serde_json::Value {
        match type_oid {
            // Boolean
            16 => json!({"type": "boolean"}),

            // Integer types
            20 => json!({"type": "long"}), // bigint/int8
            21 => json!({"type": "int"}),  // smallint/int2
            23 => json!({"type": "int"}),  // integer/int4
            26 => json!({"type": "long"}), // oid
            28 => json!({"type": "long"}), // xid

            // Floating point
            700 => json!({"type": "float"}),  // real/float4
            701 => json!({"type": "double"}), // double precision/float8

            // Numeric/Decimal (string to preserve precision)
            1700 => json!({"type": "string"}), // numeric/decimal

            // String types
            18 => json!({"type": "string"}),   // char
            19 => json!({"type": "string"}),   // name
            25 => json!({"type": "string"}),   // text
            1042 => json!({"type": "string"}), // char(n)
            1043 => json!({"type": "string"}), // varchar(n)

            // Binary
            17 => json!({"type": "bytes"}), // bytea

            // Date/Time types
            1082 => json!({"type": "int", "logicalType": "date"}), // date
            1083 => json!({"type": "int", "logicalType": "time-millis"}), // time
            1114 => json!({"type": "long", "logicalType": "timestamp-millis"}), // timestamp
            1184 => json!({"type": "long", "logicalType": "timestamp-millis"}), // timestamptz
            1186 => json!({"type": "string"}),                     // interval

            // JSON types
            114 => json!({"type": "string"}),  // json
            3802 => json!({"type": "string"}), // jsonb

            // UUID
            2950 => json!({"type": "string", "logicalType": "uuid"}),

            // Network types
            869 => json!({"type": "string"}), // inet
            650 => json!({"type": "string"}), // cidr
            829 => json!({"type": "string"}), // macaddr

            // Geometric types
            600 => json!({"type": "string"}), // point
            601 => json!({"type": "string"}), // lseg
            602 => json!({"type": "string"}), // path
            603 => json!({"type": "string"}), // box
            604 => json!({"type": "string"}), // polygon
            628 => json!({"type": "string"}), // line
            718 => json!({"type": "string"}), // circle

            // Arrays
            _ if type_name.starts_with('_') => {
                json!({"type": "array", "items": "string"})
            }

            // Default: nullable string
            _ => json!(["null", "string"]),
        }
    }

    /// Generate Avro schema from PostgreSQL table metadata
    pub fn generate_avro_schema(
        namespace: &str,
        table_name: &str,
        columns: &[(String, i32, String)], // (column_name, type_oid, type_name)
    ) -> Result<AvroSchema> {
        let mut fields = Vec::new();

        for (col_name, type_oid, type_name) in columns {
            let field_type = Self::pg_type_to_avro(*type_oid, type_name);

            // Make all fields nullable - wrap complex types (arrays, objects with type field) in union
            let is_complex = field_type
                .get("type")
                .is_some_and(|t| t.as_str() == Some("array") || field_type.is_object());
            let nullable_type = if is_complex {
                json!(["null", field_type])
            } else {
                field_type
            };

            fields.push(json!({
                "name": col_name,
                "type": nullable_type,
                "default": null
            }));
        }

        let schema_json = json!({
            "type": "record",
            "name": table_name,
            "namespace": format!("rivven.cdc.postgres.{}", namespace),
            "fields": fields
        });

        let schema_str = serde_json::to_string(&schema_json)?;
        AvroSchema::parse_str(&schema_str).context("Failed to parse generated Avro schema")
    }

    /// Generate CDC envelope schema (with before/after)
    pub fn generate_envelope_schema(
        namespace: &str,
        table_name: &str,
        columns: &[(String, i32, String)],
    ) -> Result<AvroSchema> {
        let value_schema = Self::generate_avro_schema(namespace, table_name, columns)?;
        let value_json: serde_json::Value = serde_json::from_str(&value_schema.canonical_form())?;

        let envelope_json = json!({
            "type": "record",
            "name": format!("{}_envelope", table_name),
            "namespace": format!("rivven.cdc.postgres.{}", namespace),
            "fields": [
                {"name": "op", "type": "string"},
                {"name": "before", "type": ["null", value_json], "default": null},
                {"name": "after", "type": ["null", value_json], "default": null},
                {"name": "ts_ms", "type": "long"}
            ]
        });

        let schema_str = serde_json::to_string(&envelope_json)?;
        AvroSchema::parse_str(&schema_str).context("Failed to parse envelope schema")
    }
}

/// Fetches table schema metadata from PostgreSQL
pub struct SchemaInference;

impl SchemaInference {
    /// SQL query to fetch table metadata from pg_catalog
    pub fn get_table_metadata_query(schema: &str, table: &str) -> String {
        format!(
            r#"
            SELECT 
                a.attname AS column_name,
                a.atttypid AS type_oid,
                t.typname AS type_name,
                a.attnotnull AS not_null,
                a.attnum AS ordinal_position
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
            WHERE 
                n.nspname = '{}'
                AND c.relname = '{}'
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY a.attnum;
            "#,
            schema, table
        )
    }

    /// Generate Avro schema from query results
    pub fn schema_from_metadata(
        schema: &str,
        table: &str,
        rows: Vec<(String, i32, String)>,
    ) -> Result<AvroSchema> {
        PostgresTypeMapper::generate_avro_schema(schema, table, &rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_type_mapping() {
        // Integer
        let int_type = PostgresTypeMapper::pg_type_to_avro(23, "int4");
        assert_eq!(int_type, json!({"type": "int"}));

        // Text
        let text_type = PostgresTypeMapper::pg_type_to_avro(25, "text");
        assert_eq!(text_type, json!({"type": "string"}));

        // Boolean
        let bool_type = PostgresTypeMapper::pg_type_to_avro(16, "bool");
        assert_eq!(bool_type, json!({"type": "boolean"}));

        // UUID
        let uuid_type = PostgresTypeMapper::pg_type_to_avro(2950, "uuid");
        assert!(uuid_type.get("logicalType").is_some());
    }

    #[test]
    fn test_schema_generation() {
        let columns = vec![
            ("id".to_string(), 23, "int4".to_string()),
            ("name".to_string(), 25, "text".to_string()),
            ("email".to_string(), 1043, "varchar".to_string()),
            ("created_at".to_string(), 1114, "timestamp".to_string()),
        ];

        let schema = PostgresTypeMapper::generate_avro_schema("public", "users", &columns);
        assert!(schema.is_ok());

        let schema = schema.unwrap();
        let canonical = schema.canonical_form();
        assert!(canonical.contains("\"type\":\"record\""));
        assert!(canonical.contains("rivven.cdc.postgres.public.users"));
    }

    #[test]
    fn test_envelope_schema() {
        let columns = vec![
            ("id".to_string(), 23, "int4".to_string()),
            ("name".to_string(), 25, "text".to_string()),
        ];

        let schema = PostgresTypeMapper::generate_envelope_schema("public", "users", &columns);
        assert!(schema.is_ok());
    }
}

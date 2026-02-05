//! Unit tests for rivven-rdbc types module

use rivven_rdbc::types::{ColumnMetadata, Row, TableMetadata, Value};

#[test]
fn test_value_null() {
    let v = Value::Null;
    assert!(v.is_null());
    assert_eq!(v.as_str(), None);
    assert_eq!(v.as_i64(), None);
    assert_eq!(v.as_f64(), None);
    assert_eq!(v.as_bool(), None);
}

#[test]
fn test_value_boolean() {
    let v = Value::Bool(true);
    assert!(!v.is_null());
    assert_eq!(v.as_bool(), Some(true));

    let v = Value::Bool(false);
    assert_eq!(v.as_bool(), Some(false));
}

#[test]
fn test_value_integer_types() {
    let v = Value::Int8(42);
    assert_eq!(v.as_i64(), Some(42));

    let v = Value::Int16(-100);
    assert_eq!(v.as_i64(), Some(-100));

    let v = Value::Int32(1_000_000);
    assert_eq!(v.as_i64(), Some(1_000_000));

    let v = Value::Int64(9_000_000_000_000);
    assert_eq!(v.as_i64(), Some(9_000_000_000_000));
}

#[test]
fn test_value_float_types() {
    let v = Value::Float32(1.5);
    assert!((v.as_f64().unwrap() - 1.5).abs() < 0.01);

    let v = Value::Float64(2.5);
    assert!((v.as_f64().unwrap() - 2.5).abs() < 0.00001);
}

#[test]
fn test_value_string() {
    let v = Value::String("hello world".to_string());
    assert_eq!(v.as_str(), Some("hello world"));
    assert_eq!(v.as_string(), Some("hello world".to_string()));
}

#[test]
fn test_value_bytes() {
    let data = vec![0u8, 1, 2, 3, 4, 5];
    let v = Value::Bytes(data.clone());
    assert_eq!(v.as_bytes(), Some(&data[..]));
}

#[test]
fn test_value_date_time() {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    let date = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
    let v = Value::Date(date);
    // Value::Date doesn't have as_date accessor in this impl
    assert!(!v.is_null());

    let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
    let v = Value::Time(time);
    assert!(!v.is_null());

    let datetime = NaiveDateTime::new(date, time);
    let v = Value::DateTime(datetime);
    assert!(!v.is_null());
}

#[test]
fn test_value_uuid() {
    let uuid = uuid::Uuid::new_v4();
    let v = Value::Uuid(uuid);
    // Check it's stored correctly
    if let Value::Uuid(stored) = v {
        assert_eq!(stored, uuid);
    } else {
        panic!("Expected Uuid value");
    }
}

#[test]
fn test_value_json() {
    let json_val = serde_json::json!({"name": "test", "count": 42});
    let v = Value::Json(json_val.clone());
    assert_eq!(v.as_json(), Some(&json_val));
}

#[test]
fn test_row_get_by_index() {
    let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    let values = vec![
        Value::Int32(1),
        Value::String("Alice".to_string()),
        Value::Int32(30),
    ];

    let row = Row::new(columns, values);

    assert_eq!(row.len(), 3);
    assert!(!row.is_empty());

    assert_eq!(row.get_index(0).and_then(|v| v.as_i64()), Some(1));
    assert_eq!(row.get_index(1).and_then(|v| v.as_str()), Some("Alice"));
    assert_eq!(row.get_index(2).and_then(|v| v.as_i64()), Some(30));
    assert!(row.get_index(3).is_none());
}

#[test]
fn test_row_get_by_name() {
    let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    let values = vec![
        Value::Int32(1),
        Value::String("Alice".to_string()),
        Value::Int32(30),
    ];

    let row = Row::new(columns, values);

    assert_eq!(row.get_by_name("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        row.get_by_name("name").and_then(|v| v.as_str()),
        Some("Alice")
    );
    assert_eq!(row.get_by_name("age").and_then(|v| v.as_i64()), Some(30));
    assert!(row.get_by_name("unknown").is_none());
}

#[test]
fn test_row_columns() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![Value::Int32(1), Value::String("Test".to_string())];

    let row = Row::new(columns, values);

    assert_eq!(row.columns(), &["id", "name"]);
}

#[test]
fn test_column_metadata_new() {
    let col = ColumnMetadata::new("user_id", "BIGINT");

    assert_eq!(col.name, "user_id");
    assert_eq!(col.type_name, "BIGINT");
    assert!(col.nullable); // default
    assert_eq!(col.ordinal, 0);
    assert!(col.primary_key_ordinal.is_none());
    assert!(col.max_length.is_none());
    assert!(col.precision.is_none());
    assert!(col.scale.is_none());
    assert!(col.default_value.is_none());
}

#[test]
fn test_column_metadata_is_primary_key() {
    let mut col = ColumnMetadata::new("id", "BIGINT");
    assert!(!col.is_primary_key());

    col.primary_key_ordinal = Some(1);
    assert!(col.is_primary_key());
}

#[test]
fn test_table_metadata_new() {
    let table = TableMetadata::new("users");

    assert_eq!(table.name, "users");
    assert!(table.schema.is_none());
    assert!(table.columns.is_empty());
}

#[test]
fn test_table_metadata_primary_key_columns() {
    let mut table = TableMetadata::new("orders");

    let mut id_col = ColumnMetadata::new("id", "BIGINT");
    id_col.primary_key_ordinal = Some(1);

    let mut tenant_col = ColumnMetadata::new("tenant_id", "BIGINT");
    tenant_col.primary_key_ordinal = Some(2);

    let name_col = ColumnMetadata::new("name", "VARCHAR");

    table.columns = vec![id_col, tenant_col, name_col];

    let pk_cols = table.primary_key_columns();
    assert_eq!(pk_cols.len(), 2);
    assert_eq!(pk_cols[0].name, "id");
    assert_eq!(pk_cols[1].name, "tenant_id");
}

#[test]
fn test_table_metadata_get_column() {
    let mut table = TableMetadata::new("products");
    table.columns = vec![
        ColumnMetadata::new("id", "BIGINT"),
        ColumnMetadata::new("name", "VARCHAR"),
        ColumnMetadata::new("price", "DECIMAL"),
    ];

    assert!(table.column("id").is_some());
    assert!(table.column("name").is_some());
    assert!(table.column("price").is_some());
    assert!(table.column("unknown").is_none());
}

#[test]
fn test_table_qualified_name() {
    let mut table = TableMetadata::new("users");
    assert_eq!(table.qualified_name(), "users");

    table.schema = Some("public".to_string());
    assert_eq!(table.qualified_name(), "public.users");
}

#[test]
fn test_table_column_names() {
    let mut table = TableMetadata::new("items");
    table.columns = vec![
        ColumnMetadata::new("id", "INT"),
        ColumnMetadata::new("name", "VARCHAR"),
    ];

    let names = table.column_names();
    assert_eq!(names, vec!["id", "name"]);
}

#[test]
fn test_value_from_primitives() {
    let v: Value = 42i32.into();
    assert_eq!(v.as_i64(), Some(42));

    let v: Value = "hello".into();
    assert_eq!(v.as_str(), Some("hello"));

    let v: Value = true.into();
    assert_eq!(v.as_bool(), Some(true));

    let none_val: Option<i32> = None;
    let v: Value = none_val.into();
    assert!(v.is_null());
}

#[test]
fn test_value_serialization() {
    let v = Value::String("test".to_string());
    let json = serde_json::to_string(&v).unwrap();
    assert!(json.contains("test"));

    let v2: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v, v2);
}

#[test]
fn test_row_into_map() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![Value::Int32(1), Value::String("Alice".to_string())];

    let row = Row::new(columns, values);
    let map = row.into_map();

    assert_eq!(map.get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(map.get("name").and_then(|v| v.as_str()), Some("Alice"));
}

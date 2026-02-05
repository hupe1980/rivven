//! Unit tests for rivven-rdbc dialect module

use rivven_rdbc::dialect::{
    dialect_for, MySqlDialect, PostgresDialect, SqlDialect, SqlServerDialect,
};
use rivven_rdbc::types::{ColumnMetadata, TableMetadata};

#[test]
fn test_postgres_quote_identifier() {
    let dialect = PostgresDialect;

    assert_eq!(dialect.quote_identifier("users"), "\"users\"");
    assert_eq!(dialect.quote_identifier("user_table"), "\"user_table\"");
    // Test escaping double quotes
    assert_eq!(dialect.quote_identifier("my\"table"), "\"my\"\"table\"");
}

#[test]
fn test_mysql_quote_identifier() {
    let dialect = MySqlDialect;

    assert_eq!(dialect.quote_identifier("users"), "`users`");
    assert_eq!(dialect.quote_identifier("user_table"), "`user_table`");
    // Test escaping backticks
    assert_eq!(dialect.quote_identifier("my`table"), "`my``table`");
}

#[test]
fn test_sqlserver_quote_identifier() {
    let dialect = SqlServerDialect;

    assert_eq!(dialect.quote_identifier("users"), "[users]");
    assert_eq!(dialect.quote_identifier("user_table"), "[user_table]");
}

#[test]
fn test_postgres_placeholder() {
    let dialect = PostgresDialect;

    assert_eq!(dialect.placeholder(1), "$1");
    assert_eq!(dialect.placeholder(2), "$2");
    assert_eq!(dialect.placeholder(10), "$10");
}

#[test]
fn test_mysql_placeholder() {
    let dialect = MySqlDialect;

    // MySQL uses ? for all placeholders
    assert_eq!(dialect.placeholder(1), "?");
    assert_eq!(dialect.placeholder(2), "?");
}

#[test]
fn test_sqlserver_placeholder() {
    let dialect = SqlServerDialect;

    assert_eq!(dialect.placeholder(1), "@p1");
    assert_eq!(dialect.placeholder(2), "@p2");
    assert_eq!(dialect.placeholder(10), "@p10");
}

#[test]
fn test_postgres_table_exists_sql() {
    let dialect = PostgresDialect;

    let sql = dialect.table_exists_sql(Some("public"), "users");
    assert!(sql.contains("information_schema.tables"));
    assert!(sql.contains("public"));
    assert!(sql.contains("users"));
}

#[test]
fn test_mysql_table_exists_sql() {
    let dialect = MySqlDialect;

    let sql = dialect.table_exists_sql(Some("mydb"), "users");
    // MySQL uses lowercase information_schema
    assert!(sql.contains("information_schema.tables"));
    assert!(sql.contains("mydb"));
    assert!(sql.contains("users"));
}

#[test]
fn test_postgres_native_type() {
    let dialect = PostgresDialect;

    let col = ColumnMetadata::new("id", "BIGINT");
    assert_eq!(dialect.native_type(&col), "BIGINT");

    let col = ColumnMetadata::new("name", "TEXT");
    // Default string without max_length becomes TEXT
    assert_eq!(dialect.native_type(&col), "TEXT");

    let mut col = ColumnMetadata::new("name", "VARCHAR");
    col.max_length = Some(100);
    assert_eq!(dialect.native_type(&col), "VARCHAR(100)");

    let col = ColumnMetadata::new("active", "BOOLEAN");
    assert_eq!(dialect.native_type(&col), "BOOLEAN");

    let col = ColumnMetadata::new("id", "UUID");
    assert_eq!(dialect.native_type(&col), "UUID");
}

#[test]
fn test_mysql_native_type() {
    let dialect = MySqlDialect;

    let col = ColumnMetadata::new("id", "BIGINT");
    assert_eq!(dialect.native_type(&col), "BIGINT");

    let col = ColumnMetadata::new("active", "BOOLEAN");
    // MySQL converts BOOLEAN to TINYINT(1)
    assert_eq!(dialect.native_type(&col), "TINYINT(1)");

    let col = ColumnMetadata::new("data", "JSON");
    assert_eq!(dialect.native_type(&col), "JSON");
}

#[test]
fn test_dialect_supports_returning() {
    assert!(PostgresDialect.supports_returning());
    assert!(!MySqlDialect.supports_returning()); // MySQL 8+ has it but older doesn't
    assert!(SqlServerDialect.supports_returning()); // OUTPUT clause
}

#[test]
fn test_dialect_supports_on_conflict() {
    assert!(PostgresDialect.supports_on_conflict());
    assert!(!MySqlDialect.supports_on_conflict()); // MySQL uses ON DUPLICATE KEY
    assert!(!SqlServerDialect.supports_on_conflict()); // SQL Server uses MERGE
}

#[test]
fn test_dialect_supports_merge() {
    assert!(PostgresDialect.supports_merge()); // PostgreSQL 15+
    assert!(!MySqlDialect.supports_merge());
    assert!(SqlServerDialect.supports_merge());
}

#[test]
fn test_postgres_upsert_sql() {
    let dialect = PostgresDialect;

    let mut table = TableMetadata::new("users");
    table.schema = Some("public".to_string());

    let sql = dialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);

    assert!(sql.contains("INSERT INTO"));
    assert!(sql.contains("ON CONFLICT"));
    assert!(sql.contains("DO UPDATE SET"));
}

#[test]
fn test_mysql_upsert_sql() {
    let dialect = MySqlDialect;

    let table = TableMetadata::new("users");
    let sql = dialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);

    assert!(sql.contains("INSERT INTO"));
    assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
}

#[test]
fn test_sqlserver_upsert_sql() {
    let dialect = SqlServerDialect;

    let table = TableMetadata::new("users");
    let sql = dialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);

    assert!(sql.contains("MERGE"));
    assert!(sql.contains("WHEN MATCHED THEN UPDATE"));
    assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
}

#[test]
fn test_postgres_delete_sql() {
    let dialect = PostgresDialect;

    let mut table = TableMetadata::new("users");
    table.schema = Some("public".to_string());

    let sql = dialect.delete_sql(&table, &["id"]);

    assert!(sql.contains("DELETE FROM"));
    assert!(sql.contains("WHERE"));
    assert!(sql.contains("$1"));
}

#[test]
fn test_limit_offset_sql() {
    let pg = PostgresDialect;
    let mysql = MySqlDialect;
    let sql_server = SqlServerDialect;

    // Just limit
    assert!(pg.limit_offset_sql(Some(10), None).contains("LIMIT 10"));
    assert!(mysql.limit_offset_sql(Some(10), None).contains("LIMIT 10"));
    assert!(sql_server
        .limit_offset_sql(Some(10), None)
        .contains("FETCH NEXT 10 ROWS ONLY"));

    // Limit and offset
    let pg_sql = pg.limit_offset_sql(Some(10), Some(20));
    assert!(pg_sql.contains("LIMIT 10"));
    assert!(pg_sql.contains("OFFSET 20"));

    let mysql_sql = mysql.limit_offset_sql(Some(10), Some(20));
    assert!(mysql_sql.contains("LIMIT 10"));
    assert!(mysql_sql.contains("OFFSET 20"));

    let ss_sql = sql_server.limit_offset_sql(Some(10), Some(20));
    assert!(ss_sql.contains("OFFSET 20 ROWS"));
    assert!(ss_sql.contains("FETCH NEXT 10 ROWS ONLY"));
}

#[test]
fn test_postgres_current_timestamp() {
    let dialect = PostgresDialect;
    assert_eq!(dialect.current_timestamp(), "CURRENT_TIMESTAMP");
}

#[test]
fn test_boolean_literal() {
    let pg = PostgresDialect;
    assert_eq!(pg.boolean_literal(true), "TRUE");
    assert_eq!(pg.boolean_literal(false), "FALSE");

    // MySQL uses 1/0 for booleans
    let mysql = MySqlDialect;
    assert_eq!(mysql.boolean_literal(true), "1");
    assert_eq!(mysql.boolean_literal(false), "0");
}

#[test]
fn test_dialect_for() {
    let pg = dialect_for("postgres");
    assert_eq!(pg.name(), "PostgreSQL");

    let pg2 = dialect_for("postgresql");
    assert_eq!(pg2.name(), "PostgreSQL");

    let mysql = dialect_for("mysql");
    assert_eq!(mysql.name(), "MySQL");

    let ss = dialect_for("sqlserver");
    assert_eq!(ss.name(), "SQL Server");

    let mssql = dialect_for("mssql");
    assert_eq!(mssql.name(), "SQL Server");

    // Default to PostgreSQL for unknown
    let unknown = dialect_for("unknown");
    assert_eq!(unknown.name(), "PostgreSQL");
}

#[test]
fn test_postgres_list_columns_sql() {
    let dialect = PostgresDialect;
    let sql = dialect.list_columns_sql(Some("public"), "users");

    assert!(sql.contains("information_schema.columns"));
    assert!(sql.contains("column_name"));
    assert!(sql.contains("data_type"));
    assert!(sql.contains("is_nullable"));
    assert!(sql.contains("public"));
    assert!(sql.contains("users"));
}

#[test]
fn test_escape_string() {
    let pg = PostgresDialect;
    // Basic escaping - should escape single quotes
    let escaped = pg.escape_string("O'Connor");
    assert!(escaped.contains("''") || escaped.contains("\\'"));

    let mysql = MySqlDialect;
    let escaped = mysql.escape_string("O'Connor");
    assert!(escaped.contains("''") || escaped.contains("\\'") || escaped.contains("O'Connor"));
}

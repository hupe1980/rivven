//! SQL dialect abstraction for rivven-rdbc
//!
//! Provides vendor-agnostic SQL generation using sea-query for type-safe query
//! building on PostgreSQL and MySQL, with manual SQL for SQL Server (no sea-query backend).
//!
//! - SqlDialect: Trait for database-specific SQL generation
//! - Identifier quoting (handled by sea-query where possible)
//! - Type mapping
//! - Upsert strategies (ON CONFLICT, ON DUPLICATE KEY, MERGE)

use crate::security::escape_string_literal;
use crate::types::{ColumnMetadata, TableMetadata};
use sea_query::{
    Alias, Asterisk, Expr, IntoIden, MysqlQueryBuilder, OnConflict, Order, PostgresQueryBuilder,
    Query, TableRef,
};

// ---------------------------------------------------------------------------
// Helper: build a sea-query TableRef from optional schema + table name
// ---------------------------------------------------------------------------

fn sea_table_ref(schema: Option<&str>, table: &str) -> TableRef {
    match schema {
        Some(s) => TableRef::SchemaTable(Alias::new(s).into_iden(), Alias::new(table).into_iden()),
        None => TableRef::Table(Alias::new(table).into_iden()),
    }
}

/// SQL dialect for vendor-specific SQL generation
pub trait SqlDialect: Send + Sync {
    /// Get the dialect name
    fn name(&self) -> &'static str;

    /// Quote an identifier (table, column name)
    fn quote_identifier(&self, name: &str) -> String;

    /// Get the placeholder for a parameter (e.g., $1, ?, @p1)
    fn placeholder(&self, index: usize) -> String;

    /// Get the SQL for checking table existence
    fn table_exists_sql(&self, schema: Option<&str>, table: &str) -> String;

    /// Get the SQL for listing columns
    fn list_columns_sql(&self, schema: Option<&str>, table: &str) -> String;

    /// Get the native type name for a rivven Value type
    fn native_type(&self, column: &ColumnMetadata) -> String;

    /// Generate an upsert statement
    fn upsert_sql(&self, table: &TableMetadata, pk_columns: &[&str], columns: &[&str]) -> String;

    /// Generate a delete statement with primary key conditions
    fn delete_sql(&self, table: &TableMetadata, pk_columns: &[&str]) -> String;

    /// Whether the dialect supports RETURNING clause
    fn supports_returning(&self) -> bool;

    /// Whether the dialect supports MERGE statement
    fn supports_merge(&self) -> bool;

    /// Whether the dialect supports ON CONFLICT (PostgreSQL)
    fn supports_on_conflict(&self) -> bool;

    /// Get the LIMIT/OFFSET syntax
    fn limit_offset_sql(&self, limit: Option<u64>, offset: Option<u64>) -> String;

    /// Escape a string literal
    fn escape_string(&self, value: &str) -> String;

    /// Get current timestamp expression
    fn current_timestamp(&self) -> &'static str;

    /// Get the boolean literal
    fn boolean_literal(&self, value: bool) -> &'static str;

    /// Build a SELECT statement
    #[allow(clippy::too_many_arguments)]
    fn build_select(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[&str],
        where_clause: Option<&str>,
        order_by: Option<&[(&str, bool)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String;
}

// ===========================================================================
// PostgreSQL — uses sea-query for upsert / delete / select
// ===========================================================================

/// PostgreSQL dialect
#[derive(Debug, Clone, Default)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }

    fn table_exists_sql(&self, schema: Option<&str>, table: &str) -> String {
        let schema = escape_string_literal(schema.unwrap_or("public"));
        let table = escape_string_literal(table);
        format!(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}')",
            schema, table
        )
    }

    fn list_columns_sql(&self, schema: Option<&str>, table: &str) -> String {
        let schema = escape_string_literal(schema.unwrap_or("public"));
        let table = escape_string_literal(table);
        format!(
            r#"SELECT
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES' as nullable,
                c.ordinal_position,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                CASE WHEN pk.column_name IS NOT NULL THEN pk.ordinal_position END as pk_ordinal
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name, ku.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                    AND tc.table_schema = ku.table_schema
                    AND tc.table_name = ku.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = '{}'
                    AND tc.table_name = '{}'
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_schema = '{}' AND c.table_name = '{}'
            ORDER BY c.ordinal_position"#,
            schema, table, schema, table
        )
    }

    fn native_type(&self, column: &ColumnMetadata) -> String {
        match column.type_name.to_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => "BOOLEAN".to_string(),
            "TINYINT" | "INT8" => "SMALLINT".to_string(),
            "SMALLINT" | "INT16" => "SMALLINT".to_string(),
            "INTEGER" | "INT" | "INT32" => "INTEGER".to_string(),
            "BIGINT" | "INT64" => "BIGINT".to_string(),
            "REAL" | "FLOAT32" => "REAL".to_string(),
            "DOUBLE PRECISION" | "FLOAT64" | "DOUBLE" => "DOUBLE PRECISION".to_string(),
            "DECIMAL" | "NUMERIC" => match (column.precision, column.scale) {
                (Some(p), Some(s)) => format!("NUMERIC({},{})", p, s),
                (Some(p), None) => format!("NUMERIC({})", p),
                _ => "NUMERIC".to_string(),
            },
            "VARCHAR" | "STRING" => match column.max_length {
                Some(len) => format!("VARCHAR({})", len),
                None => "TEXT".to_string(),
            },
            "TEXT" => "TEXT".to_string(),
            "BYTEA" | "BYTES" | "BLOB" => "BYTEA".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "TIMESTAMP" | "DATETIME" => "TIMESTAMP".to_string(),
            "TIMESTAMPTZ" => "TIMESTAMPTZ".to_string(),
            "UUID" => "UUID".to_string(),
            "JSON" => "JSON".to_string(),
            "JSONB" => "JSONB".to_string(),
            other => other.to_string(),
        }
    }

    fn upsert_sql(&self, table: &TableMetadata, pk_columns: &[&str], columns: &[&str]) -> String {
        let tbl = sea_table_ref(table.schema.as_deref(), &table.name);

        let col_idens: Vec<_> = columns.iter().map(|c| Alias::new(*c).into_iden()).collect();
        let update_cols: Vec<_> = columns
            .iter()
            .filter(|c| !pk_columns.contains(c))
            .map(|c| Alias::new(*c).into_iden())
            .collect();
        let pk_idens: Vec<_> = pk_columns
            .iter()
            .map(|c| Alias::new(*c).into_iden())
            .collect();

        let values: Vec<_> = (1..=columns.len())
            .map(|i| Expr::cust(format!("${}", i)))
            .collect();

        let mut on_conflict = OnConflict::columns(pk_idens);
        on_conflict.update_columns(update_cols);

        let mut stmt = Query::insert();
        stmt.into_table(tbl)
            .columns(col_idens)
            .values_panic(values)
            .on_conflict(on_conflict.to_owned());

        stmt.to_string(PostgresQueryBuilder)
    }

    fn delete_sql(&self, table: &TableMetadata, pk_columns: &[&str]) -> String {
        let tbl = sea_table_ref(table.schema.as_deref(), &table.name);

        let mut stmt = Query::delete();
        stmt.from_table(tbl);
        for (i, col) in pk_columns.iter().enumerate() {
            stmt.and_where(Expr::col(Alias::new(*col)).eq(Expr::cust(format!("${}", i + 1))));
        }

        stmt.to_string(PostgresQueryBuilder)
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_merge(&self) -> bool {
        true // PostgreSQL 15+
    }

    fn supports_on_conflict(&self) -> bool {
        true
    }

    fn limit_offset_sql(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        let mut sql = String::new();
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }
        if let Some(o) = offset {
            sql.push_str(&format!(" OFFSET {}", o));
        }
        sql
    }

    fn escape_string(&self, value: &str) -> String {
        value.replace('\'', "''")
    }

    fn current_timestamp(&self) -> &'static str {
        "CURRENT_TIMESTAMP"
    }

    fn boolean_literal(&self, value: bool) -> &'static str {
        if value {
            "TRUE"
        } else {
            "FALSE"
        }
    }

    fn build_select(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[&str],
        where_clause: Option<&str>,
        order_by: Option<&[(&str, bool)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let tbl = sea_table_ref(schema, table);

        let mut stmt = Query::select();
        stmt.from(tbl);

        if columns.is_empty() {
            stmt.column(Asterisk);
        } else {
            for col in columns {
                stmt.column(Alias::new(*col));
            }
        }

        if let Some(w) = where_clause {
            stmt.and_where(Expr::cust(w));
        }

        if let Some(orders) = order_by {
            for (col, asc) in orders {
                stmt.order_by(
                    Alias::new(*col),
                    if *asc { Order::Asc } else { Order::Desc },
                );
            }
        }

        if let Some(l) = limit {
            stmt.limit(l);
        }
        if let Some(o) = offset {
            stmt.offset(o);
        }

        stmt.to_string(PostgresQueryBuilder)
    }
}

// ===========================================================================
// MySQL — uses sea-query for upsert / delete / select
// ===========================================================================

/// MySQL dialect
#[derive(Debug, Clone, Default)]
pub struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    fn name(&self) -> &'static str {
        "MySQL"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn table_exists_sql(&self, schema: Option<&str>, table: &str) -> String {
        if let Some(db) = schema {
            format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}')",
                escape_string_literal(db), escape_string_literal(table)
            )
        } else {
            format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{}' AND table_schema = DATABASE())",
                escape_string_literal(table)
            )
        }
    }

    fn list_columns_sql(&self, schema: Option<&str>, table: &str) -> String {
        let db_filter = schema
            .map(|s| format!("table_schema = '{}'", escape_string_literal(s)))
            .unwrap_or_else(|| "table_schema = DATABASE()".to_string());

        format!(
            r#"SELECT
                column_name,
                data_type,
                is_nullable = 'YES' as nullable,
                ordinal_position,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                column_key = 'PRI' as is_primary_key,
                extra LIKE '%auto_increment%' as auto_increment
            FROM information_schema.columns
            WHERE {} AND table_name = '{}'
            ORDER BY ordinal_position"#,
            db_filter, escape_string_literal(table)
        )
    }

    fn native_type(&self, column: &ColumnMetadata) -> String {
        match column.type_name.to_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => "TINYINT(1)".to_string(),
            "TINYINT" | "INT8" => "TINYINT".to_string(),
            "SMALLINT" | "INT16" => "SMALLINT".to_string(),
            "INTEGER" | "INT" | "INT32" => "INT".to_string(),
            "BIGINT" | "INT64" => "BIGINT".to_string(),
            "REAL" | "FLOAT32" => "FLOAT".to_string(),
            "DOUBLE PRECISION" | "FLOAT64" | "DOUBLE" => "DOUBLE".to_string(),
            "DECIMAL" | "NUMERIC" => match (column.precision, column.scale) {
                (Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
                (Some(p), None) => format!("DECIMAL({})", p),
                _ => "DECIMAL(65,30)".to_string(),
            },
            "VARCHAR" | "STRING" => match column.max_length {
                Some(len) if len <= 65535 => format!("VARCHAR({})", len),
                _ => "TEXT".to_string(),
            },
            "TEXT" => "TEXT".to_string(),
            "BYTEA" | "BYTES" | "BLOB" => "BLOB".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => "DATETIME".to_string(),
            "UUID" => "CHAR(36)".to_string(),
            "JSON" | "JSONB" => "JSON".to_string(),
            other => other.to_string(),
        }
    }

    fn upsert_sql(&self, table: &TableMetadata, _pk_columns: &[&str], columns: &[&str]) -> String {
        let tbl = sea_table_ref(table.schema.as_deref(), &table.name);

        let col_idens: Vec<_> = columns.iter().map(|c| Alias::new(*c).into_iden()).collect();
        let update_cols: Vec<_> = columns.iter().map(|c| Alias::new(*c).into_iden()).collect();

        let values: Vec<_> = columns.iter().map(|_| Expr::cust("?")).collect();

        let mut on_conflict = OnConflict::new();
        on_conflict.update_columns(update_cols);

        let mut stmt = Query::insert();
        stmt.into_table(tbl)
            .columns(col_idens)
            .values_panic(values)
            .on_conflict(on_conflict.to_owned());

        stmt.to_string(MysqlQueryBuilder)
    }

    fn delete_sql(&self, table: &TableMetadata, pk_columns: &[&str]) -> String {
        let tbl = sea_table_ref(table.schema.as_deref(), &table.name);

        let mut stmt = Query::delete();
        stmt.from_table(tbl);
        for col in pk_columns {
            stmt.and_where(Expr::col(Alias::new(*col)).eq(Expr::cust("?")));
        }

        stmt.to_string(MysqlQueryBuilder)
    }

    fn supports_returning(&self) -> bool {
        false
    }

    fn supports_merge(&self) -> bool {
        false
    }

    fn supports_on_conflict(&self) -> bool {
        false // Uses ON DUPLICATE KEY instead
    }

    fn limit_offset_sql(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (Some(l), Some(o)) => format!(" LIMIT {} OFFSET {}", l, o),
            (Some(l), None) => format!(" LIMIT {}", l),
            (None, Some(o)) => format!(" LIMIT 18446744073709551615 OFFSET {}", o),
            (None, None) => String::new(),
        }
    }

    fn escape_string(&self, value: &str) -> String {
        value
            .replace('\\', "\\\\")
            .replace('\'', "\\'")
            .replace('"', "\\\"")
    }

    fn current_timestamp(&self) -> &'static str {
        "NOW()"
    }

    fn boolean_literal(&self, value: bool) -> &'static str {
        if value {
            "1"
        } else {
            "0"
        }
    }

    fn build_select(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[&str],
        where_clause: Option<&str>,
        order_by: Option<&[(&str, bool)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let tbl = sea_table_ref(schema, table);

        let mut stmt = Query::select();
        stmt.from(tbl);

        if columns.is_empty() {
            stmt.column(Asterisk);
        } else {
            for col in columns {
                stmt.column(Alias::new(*col));
            }
        }

        if let Some(w) = where_clause {
            stmt.and_where(Expr::cust(w));
        }

        if let Some(orders) = order_by {
            for (col, asc) in orders {
                stmt.order_by(
                    Alias::new(*col),
                    if *asc { Order::Asc } else { Order::Desc },
                );
            }
        }

        if let Some(l) = limit {
            stmt.limit(l);
        }
        if let Some(o) = offset {
            stmt.offset(o);
        }

        stmt.to_string(MysqlQueryBuilder)
    }
}

// ===========================================================================
// SQL Server — manual SQL (sea-query has no MSSQL backend)
// ===========================================================================

/// SQL Server dialect
#[derive(Debug, Clone, Default)]
pub struct SqlServerDialect;

impl SqlDialect for SqlServerDialect {
    fn name(&self) -> &'static str {
        "SQL Server"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    fn placeholder(&self, index: usize) -> String {
        format!("@p{}", index)
    }

    fn table_exists_sql(&self, schema: Option<&str>, table: &str) -> String {
        let schema = escape_string_literal(schema.unwrap_or("dbo"));
        let table = escape_string_literal(table);
        format!(
            "SELECT CASE WHEN EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') THEN 1 ELSE 0 END",
            schema, table
        )
    }

    fn list_columns_sql(&self, schema: Option<&str>, table: &str) -> String {
        let schema = escape_string_literal(schema.unwrap_or("dbo"));
        let table = escape_string_literal(table);
        format!(
            r#"SELECT
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                CASE c.IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END as nullable,
                c.ORDINAL_POSITION as ordinal_position,
                c.COLUMN_DEFAULT as column_default,
                c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                c.NUMERIC_PRECISION as numeric_precision,
                c.NUMERIC_SCALE as numeric_scale,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_primary_key,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as auto_increment
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_SCHEMA = '{}'
                    AND tc.TABLE_NAME = '{}'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = '{}' AND c.TABLE_NAME = '{}'
            ORDER BY c.ORDINAL_POSITION"#,
            schema, table, schema, table
        )
    }

    fn native_type(&self, column: &ColumnMetadata) -> String {
        match column.type_name.to_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => "BIT".to_string(),
            "TINYINT" | "INT8" => "TINYINT".to_string(),
            "SMALLINT" | "INT16" => "SMALLINT".to_string(),
            "INTEGER" | "INT" | "INT32" => "INT".to_string(),
            "BIGINT" | "INT64" => "BIGINT".to_string(),
            "REAL" | "FLOAT32" => "REAL".to_string(),
            "DOUBLE PRECISION" | "FLOAT64" | "DOUBLE" => "FLOAT".to_string(),
            "DECIMAL" | "NUMERIC" => match (column.precision, column.scale) {
                (Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
                (Some(p), None) => format!("DECIMAL({})", p),
                _ => "DECIMAL(38,10)".to_string(),
            },
            "VARCHAR" | "STRING" => match column.max_length {
                Some(len) if len <= 8000 => format!("NVARCHAR({})", len),
                _ => "NVARCHAR(MAX)".to_string(),
            },
            "TEXT" => "NVARCHAR(MAX)".to_string(),
            "BYTEA" | "BYTES" | "BLOB" => "VARBINARY(MAX)".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => "DATETIME2".to_string(),
            "UUID" => "UNIQUEIDENTIFIER".to_string(),
            "JSON" | "JSONB" => "NVARCHAR(MAX)".to_string(),
            other => other.to_string(),
        }
    }

    fn upsert_sql(&self, table: &TableMetadata, pk_columns: &[&str], columns: &[&str]) -> String {
        // SQL Server uses MERGE — no sea-query support
        let table_name = self.quote_identifier(&table.name);
        let schema_prefix = table
            .schema
            .as_ref()
            .map(|s| format!("{}.", self.quote_identifier(s)))
            .unwrap_or_default();

        let cols: Vec<_> = columns.iter().map(|c| self.quote_identifier(c)).collect();
        let source_cols: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                format!(
                    "{} as {}",
                    self.placeholder(i + 1),
                    self.quote_identifier(c)
                )
            })
            .collect();

        let join_conditions: Vec<_> = pk_columns
            .iter()
            .map(|c| {
                format!(
                    "target.{} = source.{}",
                    self.quote_identifier(c),
                    self.quote_identifier(c)
                )
            })
            .collect();

        let update_cols: Vec<_> = columns
            .iter()
            .filter(|c| !pk_columns.contains(c))
            .map(|c| {
                format!(
                    "target.{} = source.{}",
                    self.quote_identifier(c),
                    self.quote_identifier(c)
                )
            })
            .collect();

        let insert_cols: Vec<_> = columns
            .iter()
            .map(|c| format!("source.{}", self.quote_identifier(c)))
            .collect();

        format!(
            r#"MERGE {}{} AS target
            USING (SELECT {}) AS source
            ON ({})
            WHEN MATCHED THEN UPDATE SET {}
            WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
            schema_prefix,
            table_name,
            source_cols.join(", "),
            join_conditions.join(" AND "),
            update_cols.join(", "),
            cols.join(", "),
            insert_cols.join(", ")
        )
    }

    fn delete_sql(&self, table: &TableMetadata, pk_columns: &[&str]) -> String {
        let table_name = self.quote_identifier(&table.name);
        let schema_prefix = table
            .schema
            .as_ref()
            .map(|s| format!("{}.", self.quote_identifier(s)))
            .unwrap_or_default();

        let conditions: Vec<_> = pk_columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{} = {}", self.quote_identifier(c), self.placeholder(i + 1)))
            .collect();

        format!(
            "DELETE FROM {}{} WHERE {}",
            schema_prefix,
            table_name,
            conditions.join(" AND ")
        )
    }

    fn supports_returning(&self) -> bool {
        true // OUTPUT clause
    }

    fn supports_merge(&self) -> bool {
        true
    }

    fn supports_on_conflict(&self) -> bool {
        false
    }

    fn limit_offset_sql(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        // SQL Server uses OFFSET-FETCH (requires ORDER BY)
        match (limit, offset) {
            (Some(l), Some(o)) => format!(" OFFSET {} ROWS FETCH NEXT {} ROWS ONLY", o, l),
            (Some(l), None) => format!(" OFFSET 0 ROWS FETCH NEXT {} ROWS ONLY", l),
            (None, Some(o)) => format!(" OFFSET {} ROWS", o),
            (None, None) => String::new(),
        }
    }

    fn escape_string(&self, value: &str) -> String {
        value.replace('\'', "''")
    }

    fn current_timestamp(&self) -> &'static str {
        "GETDATE()"
    }

    fn boolean_literal(&self, value: bool) -> &'static str {
        if value {
            "1"
        } else {
            "0"
        }
    }

    fn build_select(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[&str],
        where_clause: Option<&str>,
        order_by: Option<&[(&str, bool)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        let table_name = match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        };

        let cols = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| self.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!("SELECT {} FROM {}", cols, table_name);

        if let Some(w) = where_clause {
            sql.push_str(&format!(" WHERE {}", w));
        }

        // SQL Server requires ORDER BY for OFFSET-FETCH
        if order_by.is_some() || limit.is_some() || offset.is_some() {
            if let Some(orders) = order_by {
                if !orders.is_empty() {
                    let order_parts: Vec<_> = orders
                        .iter()
                        .map(|(col, asc)| {
                            format!(
                                "{} {}",
                                self.quote_identifier(col),
                                if *asc { "ASC" } else { "DESC" }
                            )
                        })
                        .collect();
                    sql.push_str(&format!(" ORDER BY {}", order_parts.join(", ")));
                }
            } else if limit.is_some() || offset.is_some() {
                // Default ordering needed for OFFSET-FETCH
                sql.push_str(" ORDER BY (SELECT NULL)");
            }
        }

        sql.push_str(&self.limit_offset_sql(limit, offset));
        sql
    }
}

// ===========================================================================
// MariaDB — delegates to MySQL (sea-query), overrides where behaviour differs
// ===========================================================================

/// MariaDB dialect (inherits from MySQL but adds RETURNING support since 10.5)
#[derive(Debug, Clone, Default)]
pub struct MariaDbDialect;

impl SqlDialect for MariaDbDialect {
    fn name(&self) -> &'static str {
        "MariaDB"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn table_exists_sql(&self, schema: Option<&str>, table: &str) -> String {
        MySqlDialect.table_exists_sql(schema, table)
    }

    fn list_columns_sql(&self, schema: Option<&str>, table: &str) -> String {
        MySqlDialect.list_columns_sql(schema, table)
    }

    fn native_type(&self, column: &ColumnMetadata) -> String {
        match column.type_name.to_uppercase().as_str() {
            // MariaDB has native UUID since 10.7
            "UUID" => "UUID".to_string(),
            _ => MySqlDialect.native_type(column),
        }
    }

    fn upsert_sql(&self, table: &TableMetadata, pk_columns: &[&str], columns: &[&str]) -> String {
        MySqlDialect.upsert_sql(table, pk_columns, columns)
    }

    fn delete_sql(&self, table: &TableMetadata, pk_columns: &[&str]) -> String {
        MySqlDialect.delete_sql(table, pk_columns)
    }

    fn supports_returning(&self) -> bool {
        true // MariaDB 10.5+
    }

    fn supports_merge(&self) -> bool {
        false
    }

    fn supports_on_conflict(&self) -> bool {
        false
    }

    fn limit_offset_sql(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        MySqlDialect.limit_offset_sql(limit, offset)
    }

    fn escape_string(&self, value: &str) -> String {
        MySqlDialect.escape_string(value)
    }

    fn current_timestamp(&self) -> &'static str {
        "NOW()"
    }

    fn boolean_literal(&self, value: bool) -> &'static str {
        if value {
            "TRUE"
        } else {
            "FALSE"
        }
    }

    fn build_select(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[&str],
        where_clause: Option<&str>,
        order_by: Option<&[(&str, bool)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> String {
        MySqlDialect.build_select(
            schema,
            table,
            columns,
            where_clause,
            order_by,
            limit,
            offset,
        )
    }
}

/// Get a dialect instance by database type name
pub fn dialect_for(name: &str) -> Box<dyn SqlDialect> {
    match name.to_lowercase().as_str() {
        "postgres" | "postgresql" => Box::new(PostgresDialect),
        "mysql" => Box::new(MySqlDialect),
        "mariadb" => Box::new(MariaDbDialect),
        "sqlserver" | "mssql" => Box::new(SqlServerDialect),
        _ => Box::new(PostgresDialect), // Default to PostgreSQL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_dialect() {
        let dialect = PostgresDialect;
        assert_eq!(dialect.quote_identifier("users"), "\"users\"");
        assert_eq!(dialect.placeholder(1), "$1");
        assert!(dialect.supports_returning());
        assert!(dialect.supports_on_conflict());
    }

    #[test]
    fn test_mysql_dialect() {
        let dialect = MySqlDialect;
        assert_eq!(dialect.quote_identifier("users"), "`users`");
        assert_eq!(dialect.placeholder(1), "?");
        assert!(!dialect.supports_returning());
        assert!(!dialect.supports_on_conflict());
    }

    #[test]
    fn test_mariadb_dialect() {
        let dialect = MariaDbDialect;
        assert_eq!(dialect.quote_identifier("users"), "`users`");
        assert_eq!(dialect.placeholder(1), "?");
        assert!(dialect.supports_returning());
        assert!(!dialect.supports_on_conflict());
        assert_eq!(dialect.boolean_literal(true), "TRUE");
        assert_eq!(dialect.boolean_literal(false), "FALSE");
    }

    #[test]
    fn test_mariadb_native_uuid() {
        let dialect = MariaDbDialect;
        let col = ColumnMetadata::new("id", "UUID");
        assert_eq!(dialect.native_type(&col), "UUID");
    }

    #[test]
    fn test_sqlserver_dialect() {
        let dialect = SqlServerDialect;
        assert_eq!(dialect.quote_identifier("users"), "[users]");
        assert_eq!(dialect.placeholder(1), "@p1");
        assert!(dialect.supports_returning());
        assert!(dialect.supports_merge());
    }

    #[test]
    fn test_build_select() {
        let dialect = PostgresDialect;
        let sql = dialect.build_select(
            Some("public"),
            "users",
            &["id", "name"],
            Some("id > 0"),
            Some(&[("id", true)]),
            Some(10),
            Some(20),
        );
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("\"public\".\"users\""));
        assert!(sql.contains("WHERE id > 0"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
    }

    #[test]
    fn test_upsert_postgres() {
        let mut table = TableMetadata::new("users");
        table.schema = Some("public".into());
        let dialect = PostgresDialect;
        let sql = dialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);
        assert!(sql.contains("INSERT INTO"));
        assert!(sql.contains("ON CONFLICT"));
        assert!(sql.contains("DO UPDATE SET"));
    }

    #[test]
    fn test_upsert_mysql() {
        let table = TableMetadata::new("users");
        let dialect = MySqlDialect;
        let sql = dialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);
        assert!(sql.contains("INSERT INTO"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn test_upsert_sqlserver() {
        let mut table = TableMetadata::new("users");
        table.schema = Some("dbo".into());
        let dialect = SqlServerDialect;
        let sql = dialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);
        assert!(sql.contains("MERGE"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_postgres_delete_sql() {
        let mut table = TableMetadata::new("users");
        table.schema = Some("public".into());
        let dialect = PostgresDialect;
        let sql = dialect.delete_sql(&table, &["id"]);
        assert!(sql.contains("DELETE FROM"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("$1"));
    }

    #[test]
    fn test_native_types() {
        let col = ColumnMetadata::new("amount", "DECIMAL");
        assert_eq!(PostgresDialect.native_type(&col), "NUMERIC");
        assert!(MySqlDialect.native_type(&col).starts_with("DECIMAL"));
        assert!(SqlServerDialect.native_type(&col).starts_with("DECIMAL"));
    }

    #[test]
    fn test_dialect_for() {
        assert_eq!(dialect_for("postgres").name(), "PostgreSQL");
        assert_eq!(dialect_for("mysql").name(), "MySQL");
        assert_eq!(dialect_for("mariadb").name(), "MariaDB");
        assert_eq!(dialect_for("sqlserver").name(), "SQL Server");
    }

    // sea-query specific: verify generated SQL is well-formed
    #[test]
    fn test_sea_query_postgres_upsert_output() {
        let mut table = TableMetadata::new("users");
        table.schema = Some("public".into());
        let sql = PostgresDialect.upsert_sql(&table, &["id"], &["id", "name", "email"]);
        assert!(sql.starts_with("INSERT INTO"));
        assert!(sql.contains("\"public\".\"users\""));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
        assert!(sql.contains("$3"));
        assert!(sql.contains("\"name\" = \"excluded\".\"name\""));
        assert!(sql.contains("\"email\" = \"excluded\".\"email\""));
    }

    #[test]
    fn test_sea_query_mysql_upsert_output() {
        let table = TableMetadata::new("orders");
        let sql = MySqlDialect.upsert_sql(&table, &["id"], &["id", "total", "status"]);
        assert!(sql.starts_with("INSERT INTO"));
        assert!(sql.contains("`orders`"));
        assert!(sql.contains("VALUES (?, ?, ?)"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn test_sea_query_postgres_select_wildcard() {
        let sql = PostgresDialect.build_select(None, "events", &[], None, None, Some(100), None);
        assert!(sql.contains("SELECT *"));
        assert!(sql.contains("\"events\""));
        assert!(sql.contains("LIMIT 100"));
    }

    #[test]
    fn test_sea_query_mysql_select_with_order() {
        let sql = MySqlDialect.build_select(
            Some("mydb"),
            "logs",
            &["ts", "msg"],
            Some("level = 'ERROR'"),
            Some(&[("ts", false)]),
            Some(50),
            Some(10),
        );
        assert!(sql.contains("`mydb`.`logs`"));
        assert!(sql.contains("level = 'ERROR'"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("DESC"));
        assert!(sql.contains("LIMIT 50"));
        assert!(sql.contains("OFFSET 10"));
    }
}

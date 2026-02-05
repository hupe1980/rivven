//! Schema discovery and management for rivven-rdbc
//!
//! Provides:
//! - SchemaProvider: Read-only schema discovery
//! - SchemaManager: Schema management (DDL operations)
//! - Auto-DDL modes for table creation/evolution

use async_trait::async_trait;
use std::collections::HashMap;

use crate::connection::Connection;
use crate::error::Result;
use crate::types::{ColumnMetadata, TableMetadata};

/// Schema provider for read-only schema discovery
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// List all schemas/databases
    async fn list_schemas(&self) -> Result<Vec<String>>;

    /// List all tables in a schema
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>>;

    /// Get table metadata
    async fn get_table(&self, schema: Option<&str>, table: &str) -> Result<Option<TableMetadata>>;

    /// Check if a table exists
    async fn table_exists(&self, schema: Option<&str>, table: &str) -> Result<bool> {
        Ok(self.get_table(schema, table).await?.is_some())
    }

    /// Get primary key columns for a table
    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ColumnMetadata>> {
        let meta = self.get_table(schema, table).await?;
        Ok(meta
            .map(|t| t.primary_key_columns().into_iter().cloned().collect())
            .unwrap_or_default())
    }

    /// Get all columns for a table
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnMetadata>> {
        let meta = self.get_table(schema, table).await?;
        Ok(meta.map(|t| t.columns).unwrap_or_default())
    }

    /// Get column by name
    async fn get_column(
        &self,
        schema: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<Option<ColumnMetadata>> {
        let meta = self.get_table(schema, table).await?;
        Ok(meta.and_then(|t| t.column(column).cloned()))
    }

    /// List all indexes on a table
    async fn list_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexMetadata>>;

    /// List foreign keys referencing a table
    async fn list_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyMetadata>>;
}

/// Schema manager for DDL operations
#[async_trait]
pub trait SchemaManager: SchemaProvider {
    /// Create a new table
    async fn create_table(&self, table: &TableMetadata) -> Result<()>;

    /// Create a table if it doesn't exist
    async fn create_table_if_not_exists(&self, table: &TableMetadata) -> Result<bool> {
        if self
            .table_exists(table.schema.as_deref(), &table.name)
            .await?
        {
            return Ok(false);
        }
        self.create_table(table).await?;
        Ok(true)
    }

    /// Drop a table
    async fn drop_table(&self, schema: Option<&str>, table: &str) -> Result<()>;

    /// Drop a table if it exists
    async fn drop_table_if_exists(&self, schema: Option<&str>, table: &str) -> Result<bool> {
        if !self.table_exists(schema, table).await? {
            return Ok(false);
        }
        self.drop_table(schema, table).await?;
        Ok(true)
    }

    /// Add a column to a table
    async fn add_column(
        &self,
        schema: Option<&str>,
        table: &str,
        column: &ColumnMetadata,
    ) -> Result<()>;

    /// Drop a column from a table
    async fn drop_column(&self, schema: Option<&str>, table: &str, column: &str) -> Result<()>;

    /// Alter a column
    async fn alter_column(
        &self,
        schema: Option<&str>,
        table: &str,
        column: &ColumnMetadata,
    ) -> Result<()>;

    /// Rename a table
    async fn rename_table(
        &self,
        schema: Option<&str>,
        old_name: &str,
        new_name: &str,
    ) -> Result<()>;

    /// Rename a column
    async fn rename_column(
        &self,
        schema: Option<&str>,
        table: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()>;

    /// Create an index
    async fn create_index(&self, index: &IndexMetadata) -> Result<()>;

    /// Drop an index
    async fn drop_index(&self, schema: Option<&str>, index_name: &str) -> Result<()>;

    /// Apply schema evolution (add missing columns, etc.)
    async fn evolve_schema(
        &self,
        target: &TableMetadata,
        mode: SchemaEvolutionMode,
    ) -> Result<SchemaEvolutionResult>;
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Index name
    pub name: String,
    /// Column names (in order)
    pub columns: Vec<String>,
    /// Whether the index is unique
    pub unique: bool,
    /// Whether this is the primary key index
    pub primary: bool,
    /// Index type (B-tree, hash, GIN, etc.)
    pub index_type: Option<String>,
    /// Partial index predicate (WHERE clause)
    pub predicate: Option<String>,
}

impl IndexMetadata {
    /// Create a new index metadata
    pub fn new(table: impl Into<String>, name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            name: name.into(),
            columns,
            unique: false,
            primary: false,
            index_type: None,
            predicate: None,
        }
    }

    /// Set as unique index
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set as primary key index
    pub fn primary(mut self) -> Self {
        self.primary = true;
        self.unique = true;
        self
    }
}

/// Foreign key metadata
#[derive(Debug, Clone)]
pub struct ForeignKeyMetadata {
    /// Constraint name
    pub name: String,
    /// Source schema
    pub source_schema: Option<String>,
    /// Source table
    pub source_table: String,
    /// Source columns
    pub source_columns: Vec<String>,
    /// Target schema
    pub target_schema: Option<String>,
    /// Target table
    pub target_table: String,
    /// Target columns
    pub target_columns: Vec<String>,
    /// ON DELETE action
    pub on_delete: ForeignKeyAction,
    /// ON UPDATE action
    pub on_update: ForeignKeyAction,
}

/// Foreign key referential action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForeignKeyAction {
    /// No action (default)
    #[default]
    NoAction,
    /// Restrict deletion
    Restrict,
    /// Cascade delete/update
    Cascade,
    /// Set to NULL
    SetNull,
    /// Set to default value
    SetDefault,
}

impl ForeignKeyAction {
    /// Convert to SQL string
    pub fn to_sql(&self) -> &'static str {
        match self {
            Self::NoAction => "NO ACTION",
            Self::Restrict => "RESTRICT",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
        }
    }
}

/// Schema evolution mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaEvolutionMode {
    /// No changes allowed
    None,
    /// Only add new columns (safest)
    #[default]
    AddColumnsOnly,
    /// Add columns and alter types (widen only)
    AddAndWiden,
    /// Full evolution including drops (dangerous)
    Full,
}

/// Result of schema evolution
#[derive(Debug, Clone, Default)]
pub struct SchemaEvolutionResult {
    /// Columns added
    pub columns_added: Vec<String>,
    /// Columns altered
    pub columns_altered: Vec<String>,
    /// Columns dropped (only in Full mode)
    pub columns_dropped: Vec<String>,
    /// Indexes added
    pub indexes_added: Vec<String>,
    /// Indexes dropped
    pub indexes_dropped: Vec<String>,
    /// Whether the table was created
    pub table_created: bool,
    /// Whether any changes were made
    pub changed: bool,
}

impl SchemaEvolutionResult {
    /// Create a result for a newly created table
    pub fn table_created() -> Self {
        Self {
            table_created: true,
            changed: true,
            ..Default::default()
        }
    }

    /// Check if any changes were made
    pub fn has_changes(&self) -> bool {
        self.changed
            || self.table_created
            || !self.columns_added.is_empty()
            || !self.columns_altered.is_empty()
            || !self.columns_dropped.is_empty()
            || !self.indexes_added.is_empty()
            || !self.indexes_dropped.is_empty()
    }
}

/// Auto-DDL mode for sink/source operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoDdlMode {
    /// No auto-DDL (error if table doesn't exist)
    #[default]
    None,
    /// Create table if not exists
    Create,
    /// Create and evolve schema
    CreateAndEvolve,
}

/// Table naming strategy for target table names
pub trait TableNamingStrategy: Send + Sync {
    /// Transform source table name to target name
    fn transform(&self, schema: Option<&str>, table: &str) -> (Option<String>, String);
}

/// Default identity naming (no transformation)
#[derive(Debug, Clone, Default)]
pub struct IdentityNaming;

impl TableNamingStrategy for IdentityNaming {
    fn transform(&self, schema: Option<&str>, table: &str) -> (Option<String>, String) {
        (schema.map(String::from), table.to_string())
    }
}

/// Prefix naming strategy
#[derive(Debug, Clone)]
pub struct PrefixNaming {
    prefix: String,
}

impl PrefixNaming {
    /// Create a new prefix naming strategy
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl TableNamingStrategy for PrefixNaming {
    fn transform(&self, schema: Option<&str>, table: &str) -> (Option<String>, String) {
        (
            schema.map(String::from),
            format!("{}{}", self.prefix, table),
        )
    }
}

/// Suffix naming strategy
#[derive(Debug, Clone)]
pub struct SuffixNaming {
    suffix: String,
}

impl SuffixNaming {
    /// Create a new suffix naming strategy
    pub fn new(suffix: impl Into<String>) -> Self {
        Self {
            suffix: suffix.into(),
        }
    }
}

impl TableNamingStrategy for SuffixNaming {
    fn transform(&self, schema: Option<&str>, table: &str) -> (Option<String>, String) {
        (
            schema.map(String::from),
            format!("{}{}", table, self.suffix),
        )
    }
}

/// Schema mapping naming strategy
#[derive(Debug, Clone)]
pub struct SchemaMapping {
    mappings: HashMap<String, String>,
    default_schema: Option<String>,
}

impl SchemaMapping {
    /// Create a new schema mapping
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            default_schema: None,
        }
    }

    /// Add a schema mapping
    pub fn map(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.mappings.insert(from.into(), to.into());
        self
    }

    /// Set default target schema
    pub fn default_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = Some(schema.into());
        self
    }
}

impl Default for SchemaMapping {
    fn default() -> Self {
        Self::new()
    }
}

impl TableNamingStrategy for SchemaMapping {
    fn transform(&self, schema: Option<&str>, table: &str) -> (Option<String>, String) {
        let target_schema = schema
            .and_then(|s| self.mappings.get(s).cloned())
            .or_else(|| self.default_schema.clone())
            .or_else(|| schema.map(String::from));
        (target_schema, table.to_string())
    }
}

/// Create a schema provider from a connection
pub fn schema_provider(_conn: &dyn Connection) -> Box<dyn SchemaProvider> {
    // This would be implemented per-backend
    // For now, return a placeholder that panics
    unimplemented!("schema provider implementation is per-backend")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_naming() {
        let naming = IdentityNaming;
        let (schema, table) = naming.transform(Some("public"), "users");
        assert_eq!(schema, Some("public".to_string()));
        assert_eq!(table, "users");
    }

    #[test]
    fn test_prefix_naming() {
        let naming = PrefixNaming::new("cdc_");
        let (_, table) = naming.transform(None, "users");
        assert_eq!(table, "cdc_users");
    }

    #[test]
    fn test_suffix_naming() {
        let naming = SuffixNaming::new("_replica");
        let (_, table) = naming.transform(None, "orders");
        assert_eq!(table, "orders_replica");
    }

    #[test]
    fn test_schema_mapping() {
        let naming = SchemaMapping::new()
            .map("source_db", "target_db")
            .default_schema("default_target");

        let (schema, _) = naming.transform(Some("source_db"), "users");
        assert_eq!(schema, Some("target_db".into()));

        let (schema, _) = naming.transform(Some("unknown"), "users");
        assert_eq!(schema, Some("default_target".into()));

        let (schema, _) = naming.transform(None, "users");
        assert_eq!(schema, Some("default_target".into()));
    }

    #[test]
    fn test_schema_evolution_result() {
        let result = SchemaEvolutionResult::table_created();
        assert!(result.has_changes());
        assert!(result.table_created);

        let mut result = SchemaEvolutionResult::default();
        assert!(!result.has_changes());

        result.columns_added.push("new_col".into());
        assert!(result.has_changes());
    }

    #[test]
    fn test_foreign_key_action_sql() {
        assert_eq!(ForeignKeyAction::Cascade.to_sql(), "CASCADE");
        assert_eq!(ForeignKeyAction::SetNull.to_sql(), "SET NULL");
    }

    #[test]
    fn test_index_metadata() {
        let idx = IndexMetadata::new("users", "idx_users_email", vec!["email".into()]).unique();

        assert_eq!(idx.table, "users");
        assert_eq!(idx.name, "idx_users_email");
        assert!(idx.unique);
        assert!(!idx.primary);
    }
}

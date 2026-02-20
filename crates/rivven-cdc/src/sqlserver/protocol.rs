//! SQL Server TDS protocol client for CDC operations
//!
//! Uses Tiberius for TDS communication with SQL Server.

use super::error::SqlServerError;
use super::source::{CaptureInstance, Lsn, SqlServerCdcConfig};
use crate::common::Validator;
use crate::common::{CdcError, CdcOp, Result};
use serde_json::{Map, Value};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, trace, warn};

/// A change record from the CDC tables
#[derive(Debug, Clone)]
pub struct CdcChangeRecord {
    /// Commit LSN (transaction commit position)
    pub commit_lsn: Lsn,
    /// Change LSN (sequence within transaction)
    pub change_lsn: Lsn,
    /// Operation type (1=Delete, 2=Insert, 3=Update before, 4=Update after)
    pub operation: CdcOp,
    /// Update mask bitmap (which columns changed)
    pub update_mask: Vec<u8>,
    /// Before image (for deletes and updates)
    pub before: Option<Map<String, Value>>,
    /// After image (for inserts and updates)
    pub after: Option<Map<String, Value>>,
    /// Commit timestamp (if available)
    pub commit_time: Option<i64>,
}

/// SQL Server client for CDC operations
pub struct SqlServerClient {
    client: Client<Compat<TcpStream>>,
    database: String,
}

impl SqlServerClient {
    /// Connect to SQL Server
    pub async fn connect(config: &SqlServerCdcConfig) -> Result<Self> {
        debug!(
            "Connecting to SQL Server {}:{}/{}",
            config.host, config.port, config.database
        );

        let mut tiberius_config = Config::new();
        tiberius_config.host(&config.host);
        tiberius_config.port(config.port);
        tiberius_config.database(&config.database);
        tiberius_config.application_name(&config.application_name);

        // Authentication
        if let Some(ref password) = config.password {
            tiberius_config.authentication(AuthMethod::sql_server(&config.username, password));
        } else {
            // Windows authentication (NTLM) - requires additional setup
            return Err(CdcError::config(
                "Password is required for SQL Server authentication",
            ));
        }

        // TLS settings
        if config.encrypt {
            if config.trust_server_certificate {
                tiberius_config.encryption(EncryptionLevel::Required);
                tiberius_config.trust_cert();
            } else {
                tiberius_config.encryption(EncryptionLevel::Required);
            }
        } else {
            tiberius_config.encryption(EncryptionLevel::NotSupported);
        }

        // Connect
        let tcp = TcpStream::connect(tiberius_config.get_addr())
            .await
            .map_err(|e| SqlServerError::Connection(e.to_string()))?;

        tcp.set_nodelay(true)
            .map_err(|e| SqlServerError::Connection(e.to_string()))?;

        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .map_err(|e| SqlServerError::Tds(e.to_string()))?;

        info!(
            "Connected to SQL Server {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self {
            client,
            database: config.database.clone(),
        })
    }

    /// Verify that CDC is enabled on the database
    pub async fn verify_cdc_enabled(&mut self) -> Result<()> {
        let query = "SELECT is_cdc_enabled FROM sys.databases WHERE name = @P1";
        let result = self
            .client
            .query(query, &[&self.database.as_str()])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        if result.is_empty() {
            return Err(SqlServerError::CdcNotEnabled(self.database.clone()).into());
        }

        let is_enabled: bool = result[0]
            .get::<bool, _>(0)
            .ok_or_else(|| SqlServerError::CdcNotEnabled(self.database.clone()))?;

        if !is_enabled {
            return Err(SqlServerError::CdcNotEnabled(self.database.clone()).into());
        }

        debug!("CDC is enabled on database '{}'", self.database);
        Ok(())
    }

    /// Discover all capture instances (CDC-enabled tables)
    pub async fn discover_capture_instances(&mut self) -> Result<Vec<CaptureInstance>> {
        let query = r#"
            SELECT 
                source_schema,
                source_table,
                capture_instance,
                object_id,
                supports_net_changes
            FROM cdc.change_tables
            ORDER BY source_schema, source_table
        "#;

        let result = self
            .client
            .query(query, &[])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        let mut instances = Vec::new();

        for row in result {
            let source_schema: &str = row.get(0).unwrap_or_default();
            let source_table: &str = row.get(1).unwrap_or_default();
            let capture_instance: &str = row.get(2).unwrap_or_default();
            let object_id: i32 = row.get(3).unwrap_or_default();
            let supports_net_changes: bool = row.get(4).unwrap_or_default();

            // Get columns for this capture instance
            let columns = self.get_capture_columns(capture_instance).await?;
            let pk_columns = self.get_primary_key_columns(object_id).await?;

            instances.push(CaptureInstance {
                source_schema: source_schema.to_string(),
                source_table: source_table.to_string(),
                capture_instance: capture_instance.to_string(),
                object_id,
                columns,
                primary_key_columns: pk_columns,
                supports_net_changes,
            });
        }

        debug!("Discovered {} capture instances", instances.len());
        Ok(instances)
    }

    /// Get columns for a capture instance
    async fn get_capture_columns(&mut self, capture_instance: &str) -> Result<Vec<String>> {
        let query = r#"
            SELECT column_name
            FROM cdc.captured_columns cc
            JOIN cdc.change_tables ct ON cc.object_id = ct.object_id
            WHERE ct.capture_instance = @P1
            ORDER BY cc.column_ordinal
        "#;

        let result = self
            .client
            .query(query, &[&capture_instance])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        let columns: Vec<String> = result
            .iter()
            .filter_map(|row| row.get::<&str, _>(0).map(|s| s.to_string()))
            .collect();

        Ok(columns)
    }

    /// Get primary key columns for a table
    async fn get_primary_key_columns(&mut self, object_id: i32) -> Result<Vec<String>> {
        let query = r#"
            SELECT c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE i.is_primary_key = 1 AND ic.object_id = @P1
            ORDER BY ic.key_ordinal
        "#;

        let result = self
            .client
            .query(query, &[&object_id])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        let pk_columns: Vec<String> = result
            .iter()
            .filter_map(|row| row.get::<&str, _>(0).map(|s| s.to_string()))
            .collect();

        Ok(pk_columns)
    }

    /// Get the current maximum LSN
    pub async fn get_max_lsn(&mut self) -> Result<Lsn> {
        let query = "SELECT sys.fn_cdc_get_max_lsn()";

        let result = self
            .client
            .query(query, &[])
            .await
            .map_err(|e| SqlServerError::MaxLsnFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::MaxLsnFailed(e.to_string()))?;

        if result.is_empty() {
            return Err(SqlServerError::MaxLsnFailed("No result returned".to_string()).into());
        }

        let lsn_bytes: &[u8] = result[0]
            .get(0)
            .ok_or_else(|| SqlServerError::MaxLsnFailed("NULL LSN returned".to_string()))?;

        if lsn_bytes.len() != 10 {
            return Err(SqlServerError::InvalidLsn(format!(
                "Expected 10 bytes, got {}",
                lsn_bytes.len()
            ))
            .into());
        }

        let mut arr = [0u8; 10];
        arr.copy_from_slice(lsn_bytes);
        Ok(Lsn::new(arr))
    }

    /// Get the minimum LSN for a capture instance
    pub async fn get_min_lsn(&mut self, capture_instance: &str) -> Result<Lsn> {
        // Validate capture_instance to prevent SQL injection — the value is
        // interpolated as an unquoted identifier in the function argument.
        Validator::validate_identifier(capture_instance)
            .map_err(|e| CdcError::Config(e.to_string()))?;

        let query = format!(
            "SELECT sys.fn_cdc_get_min_lsn('{}')",
            capture_instance.replace('\'', "''")
        );

        let result = self
            .client
            .query(&query, &[])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        if result.is_empty() {
            return Err(
                SqlServerError::QueryFailed("No min LSN result returned".to_string()).into(),
            );
        }

        let lsn_bytes: &[u8] = result[0]
            .get(0)
            .ok_or_else(|| SqlServerError::InvalidLsn("NULL min LSN returned".to_string()))?;

        if lsn_bytes.len() != 10 {
            return Err(SqlServerError::InvalidLsn(format!(
                "Expected 10 bytes, got {}",
                lsn_bytes.len()
            ))
            .into());
        }

        let mut arr = [0u8; 10];
        arr.copy_from_slice(lsn_bytes);
        Ok(Lsn::new(arr))
    }

    /// Get changes from a capture instance
    pub async fn get_changes(
        &mut self,
        capture_instance: &str,
        from_lsn: &Lsn,
        to_lsn: &Lsn,
        batch_size: u32,
    ) -> Result<Vec<CdcChangeRecord>> {
        // Validate capture_instance to prevent SQL injection — the value is
        // interpolated directly into the CDC function name, which cannot be
        // parameterized in T-SQL.
        Validator::validate_identifier(capture_instance)
            .map_err(|e| CdcError::Config(e.to_string()))?;

        // Use fn_cdc_get_all_changes_ for full change data
        // 'all update old' returns both before and after images for updates
        let query = format!(
            r#"
            SELECT TOP (@P1)
                __$start_lsn,
                __$seqval,
                __$operation,
                __$update_mask,
                sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __commit_time,
                *
            FROM cdc.fn_cdc_get_all_changes_{capture_instance}(@P2, @P3, N'all update old')
            WHERE __$start_lsn > @P2
            ORDER BY __$start_lsn, __$seqval
            "#,
            capture_instance = capture_instance.replace('\'', "''")
        );

        let result = self
            .client
            .query(
                &query,
                &[
                    &(batch_size as i32),
                    &from_lsn.bytes.as_slice(),
                    &to_lsn.bytes.as_slice(),
                ],
            )
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        // Get column metadata for this capture instance
        let columns = self.get_capture_columns(capture_instance).await?;

        let mut changes = Vec::with_capacity(result.len());

        for row in result {
            // Parse CDC metadata columns
            let commit_lsn = parse_lsn_from_row(&row, 0)?;
            let change_lsn = parse_lsn_from_row(&row, 1)?;
            let operation: i32 = row.get(2).unwrap_or(0);
            let update_mask: Vec<u8> = row
                .get::<&[u8], _>(3)
                .map(|b| b.to_vec())
                .unwrap_or_default();

            // Parse commit timestamp from sys.fn_cdc_map_lsn_to_time (column 4)
            let commit_time: Option<i64> = row
                .try_get::<chrono::NaiveDateTime, _>(4)
                .ok()
                .flatten()
                .map(|dt| dt.and_utc().timestamp());

            // Convert operation code to CdcOp
            // 1=Delete, 2=Insert, 3=Update (before), 4=Update (after)
            let op = match operation {
                1 => CdcOp::Delete,
                2 => CdcOp::Insert,
                3 => CdcOp::Update, // Before image - we'll pair with after
                4 => CdcOp::Update, // After image
                _ => {
                    warn!("Unknown CDC operation code: {}", operation);
                    continue;
                }
            };

            // Parse data columns (starting from column 5, after __commit_time)
            let data = parse_row_data(&row, &columns, 5);

            // Build change record
            let (before, after) = match operation {
                1 => (Some(data), None), // Delete: data is before image
                2 => (None, Some(data)), // Insert: data is after image
                3 => (Some(data), None), // Update before: data is before image
                4 => (None, Some(data)), // Update after: data is after image
                _ => (None, None),
            };

            changes.push(CdcChangeRecord {
                commit_lsn,
                change_lsn,
                operation: op,
                update_mask,
                before,
                after,
                commit_time,
            });
        }

        // Merge update pairs (operation 3 followed by 4)
        let changes = merge_update_pairs(changes);

        trace!("Got {} changes from {}", changes.len(), capture_instance);
        Ok(changes)
    }

    /// Execute a snapshot query for initial data load
    pub async fn snapshot_table(
        &mut self,
        schema: &str,
        table: &str,
        columns: &[String],
        batch_size: u32,
        offset: u64,
    ) -> Result<Vec<Map<String, Value>>> {
        let column_list = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| format!("[{}]", c.replace(']', "]]")))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Use OFFSET/FETCH for pagination (SQL Server 2012+)
        let query = format!(
            r#"
            SELECT {columns}
            FROM [{schema}].[{table}]
            ORDER BY (SELECT NULL)
            OFFSET @P1 ROWS FETCH NEXT @P2 ROWS ONLY
            "#,
            columns = column_list,
            schema = schema.replace(']', "]]"),
            table = table.replace(']', "]]")
        );

        let result = self
            .client
            .query(&query, &[&(offset as i64), &(batch_size as i64)])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        // Get column names from query result
        let col_names: Vec<String> = if columns.is_empty() && !result.is_empty() {
            // Extract from first row metadata
            result[0]
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect()
        } else {
            columns.to_vec()
        };

        let mut rows = Vec::with_capacity(result.len());
        for row in result {
            let data = parse_row_data(&row, &col_names, 0);
            rows.push(data);
        }

        Ok(rows)
    }

    /// Get row count for a table (for snapshot progress)
    pub async fn get_table_row_count(&mut self, schema: &str, table: &str) -> Result<u64> {
        let query = format!(
            "SELECT COUNT(*) FROM [{schema}].[{table}]",
            schema = schema.replace(']', "]]"),
            table = table.replace(']', "]]")
        );

        let result = self
            .client
            .query(&query, &[])
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| SqlServerError::QueryFailed(e.to_string()))?;

        if result.is_empty() {
            return Ok(0);
        }

        let count: i64 = result[0].get(0).unwrap_or(0);
        Ok(count as u64)
    }
}

/// Parse LSN from a row at given index
fn parse_lsn_from_row(row: &Row, index: usize) -> Result<Lsn> {
    let bytes: &[u8] = row
        .get(index)
        .ok_or_else(|| SqlServerError::InvalidLsn("Missing LSN column".to_string()))?;

    if bytes.len() != 10 {
        return Err(
            SqlServerError::InvalidLsn(format!("Expected 10 bytes, got {}", bytes.len())).into(),
        );
    }

    let mut arr = [0u8; 10];
    arr.copy_from_slice(bytes);
    Ok(Lsn::new(arr))
}

/// Parse row data into JSON map
fn parse_row_data(row: &Row, columns: &[String], start_index: usize) -> Map<String, Value> {
    let mut data = Map::new();

    for (i, col_name) in columns.iter().enumerate() {
        let col_idx = start_index + i;
        if col_idx >= row.len() {
            break;
        }

        // Try different types in order of likelihood
        let value = if let Some(v) = row.try_get::<&str, _>(col_idx).ok().flatten() {
            Value::String(v.to_string())
        } else if let Some(v) = row.try_get::<i64, _>(col_idx).ok().flatten() {
            Value::Number(v.into())
        } else if let Some(v) = row.try_get::<i32, _>(col_idx).ok().flatten() {
            Value::Number(v.into())
        } else if let Some(v) = row.try_get::<i16, _>(col_idx).ok().flatten() {
            Value::Number(v.into())
        } else if let Some(v) = row.try_get::<f64, _>(col_idx).ok().flatten() {
            // Handle NaN/Infinity
            if v.is_nan() || v.is_infinite() {
                Value::Null
            } else {
                serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
        } else if let Some(v) = row.try_get::<bool, _>(col_idx).ok().flatten() {
            Value::Bool(v)
        } else if let Some(v) = row.try_get::<&[u8], _>(col_idx).ok().flatten() {
            // Binary data as base64
            Value::String(base64_encode(v))
        } else if let Some(v) = row
            .try_get::<chrono::NaiveDateTime, _>(col_idx)
            .ok()
            .flatten()
        {
            Value::String(v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        } else if let Some(v) = row.try_get::<chrono::NaiveDate, _>(col_idx).ok().flatten() {
            Value::String(v.format("%Y-%m-%d").to_string())
        } else if let Some(v) = row.try_get::<chrono::NaiveTime, _>(col_idx).ok().flatten() {
            Value::String(v.format("%H:%M:%S%.f").to_string())
        } else if let Some(v) = row.try_get::<uuid::Uuid, _>(col_idx).ok().flatten() {
            Value::String(v.to_string())
        } else {
            // Null or unsupported type
            Value::Null
        };

        data.insert(col_name.clone(), value);
    }

    data
}

/// Base64 encode binary data
fn base64_encode(data: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(data)
}

/// Merge update pairs (operation 3 followed by 4) into single change records
fn merge_update_pairs(changes: Vec<CdcChangeRecord>) -> Vec<CdcChangeRecord> {
    let mut merged = Vec::with_capacity(changes.len());
    let mut iter = changes.into_iter().peekable();

    while let Some(current) = iter.next() {
        // Check if this is an update before image (op 3) followed by after image (op 4)
        if current.operation == CdcOp::Update && current.before.is_some() && current.after.is_none()
        {
            // Peek at the next item to see if it's the matching after image
            if let Some(next) = iter.peek() {
                if next.operation == CdcOp::Update
                    && next.before.is_none()
                    && next.after.is_some()
                    && next.commit_lsn == current.commit_lsn
                {
                    // Take the next item and merge (safe: peek() confirmed it exists)
                    let Some(next) = iter.next() else {
                        unreachable!("peek() succeeded but next() returned None");
                    };
                    merged.push(CdcChangeRecord {
                        commit_lsn: current.commit_lsn,
                        change_lsn: next.change_lsn,
                        operation: CdcOp::Update,
                        update_mask: next.update_mask,
                        before: current.before,
                        after: next.after,
                        commit_time: next.commit_time,
                    });
                    continue;
                }
            }
        }

        merged.push(current);
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_parsing() {
        let hex = "00000001000000010001";
        let lsn = Lsn::from_hex(hex).unwrap();
        assert_eq!(lsn.to_hex(), hex);
    }

    #[test]
    fn test_lsn_comparison() {
        let lsn1 = Lsn::from_hex("00000001000000010001").unwrap();
        let lsn2 = Lsn::from_hex("00000001000000010002").unwrap();
        assert!(lsn1 < lsn2);
    }

    #[test]
    fn test_base64_encode() {
        let data = b"hello world";
        let encoded = base64_encode(data);
        assert_eq!(encoded, "aGVsbG8gd29ybGQ=");
    }
}

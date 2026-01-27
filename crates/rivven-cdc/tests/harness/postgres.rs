//! PostgreSQL testcontainer setup for CDC testing

use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info};

/// PostgreSQL container with logical replication enabled
pub struct PostgresTestContainer {
    #[allow(dead_code)]
    container: ContainerAsync<Postgres>,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub replication_user: String,
    pub replication_password: String,
}

#[allow(dead_code)]
impl PostgresTestContainer {
    /// Start a new PostgreSQL container with logical replication enabled
    pub async fn start() -> Result<Self> {
        info!("🐘 Starting PostgreSQL testcontainer with logical replication...");

        let container = Postgres::default()
            .with_cmd(vec![
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=50",
                "-c",
                "max_wal_senders=50",
                "-c",
                "wal_sender_timeout=0",
            ])
            .start()
            .await
            .context("Failed to start PostgreSQL container")?;

        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(5432).await?;

        info!("✅ PostgreSQL container started on {}:{}", host, port);

        let instance = Self {
            container,
            host,
            port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            replication_user: "rivven_cdc".to_string(),
            replication_password: "rivven_password".to_string(),
        };

        instance.wait_for_ready().await?;
        instance.setup_replication().await?;

        Ok(instance)
    }

    async fn wait_for_ready(&self) -> Result<()> {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        );

        for attempt in 1..=60 {
            match tokio_postgres::connect(&conn_str, NoTls).await {
                Ok(_) => {
                    debug!("PostgreSQL ready after {} attempts", attempt);
                    return Ok(());
                }
                Err(e) => {
                    if attempt % 10 == 0 {
                        info!("Waiting for PostgreSQL (attempt {}/60): {}", attempt, e);
                    }
                    sleep(Duration::from_millis(1000)).await;
                }
            }
        }

        anyhow::bail!("PostgreSQL did not become ready in time")
    }

    /// Create a new PostgreSQL client connection
    pub async fn new_client(&self) -> Result<Client> {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Client connection error: {}", e);
            }
        });

        Ok(client)
    }

    async fn setup_replication(&self) -> Result<()> {
        let client = self.new_client().await?;

        info!("🔧 Setting up logical replication...");

        client
            .execute(
                &format!(
                    "CREATE USER {} WITH REPLICATION PASSWORD '{}'",
                    self.replication_user, self.replication_password
                ),
                &[],
            )
            .await
            .context("Failed to create replication user")?;

        client
            .execute(
                &format!("GRANT USAGE ON SCHEMA public TO {}", self.replication_user),
                &[],
            )
            .await?;

        client
            .execute(
                &format!(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}",
                    self.replication_user
                ),
                &[],
            )
            .await?;

        info!("✅ Replication setup complete");
        Ok(())
    }

    pub async fn create_publication(&self, name: &str) -> Result<()> {
        let client = self.new_client().await?;
        client
            .execute(&format!("CREATE PUBLICATION {} FOR ALL TABLES", name), &[])
            .await?;
        info!("📢 Created publication: {}", name);
        Ok(())
    }

    pub async fn create_replication_slot(&self, name: &str) -> Result<()> {
        let client = self.new_client().await?;
        client
            .execute(
                &format!(
                    "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                    name
                ),
                &[],
            )
            .await?;
        info!("🎰 Created replication slot: {}", name);
        Ok(())
    }

    pub async fn drop_replication_slot(&self, name: &str) -> Result<()> {
        let client = self.new_client().await?;
        client
            .execute(&format!("SELECT pg_drop_replication_slot('{}')", name), &[])
            .await
            .ok();
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<u64> {
        let client = self.new_client().await?;
        let rows = client.execute(sql, &[]).await?;
        debug!("Executed SQL: {} (affected {} rows)", sql, rows);
        Ok(rows)
    }

    pub async fn execute_batch(&self, statements: &[&str]) -> Result<()> {
        let client = self.new_client().await?;
        client.execute("BEGIN", &[]).await?;
        for sql in statements {
            client.execute(*sql, &[]).await?;
        }
        client.execute("COMMIT", &[]).await?;
        Ok(())
    }

    pub async fn create_test_table(&self, table_name: &str) -> Result<()> {
        self.execute(&format!(
            "CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email VARCHAR(255),
                age INTEGER,
                balance NUMERIC(12, 2),
                is_active BOOLEAN DEFAULT true,
                metadata JSONB,
                tags TEXT[],
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )",
            table_name
        ))
        .await?;
        info!("📋 Created test table: {}", table_name);
        Ok(())
    }

    /// Create table with all PostgreSQL data types for type testing
    pub async fn create_all_types_table(&self, table_name: &str) -> Result<()> {
        self.execute(&format!(
            "CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                col_smallint SMALLINT,
                col_integer INTEGER,
                col_bigint BIGINT,
                col_real REAL,
                col_double DOUBLE PRECISION,
                col_numeric NUMERIC(15, 6),
                col_text TEXT,
                col_varchar VARCHAR(255),
                col_char CHAR(10),
                col_bytea BYTEA,
                col_boolean BOOLEAN,
                col_date DATE,
                col_time TIME,
                col_timestamp TIMESTAMP,
                col_timestamptz TIMESTAMPTZ,
                col_json JSON,
                col_jsonb JSONB,
                col_uuid UUID,
                col_int_array INTEGER[],
                col_text_array TEXT[]
            )",
            table_name
        ))
        .await?;
        info!("📋 Created all-types test table: {}", table_name);
        Ok(())
    }

    /// Execute SQL query and return rows
    pub async fn query(&self, sql: &str) -> Result<Vec<tokio_postgres::Row>> {
        let client = self.new_client().await?;
        let rows = client.query(sql, &[]).await?;
        Ok(rows)
    }

    /// Execute multiple statements in a single transaction
    /// If commit is false, the transaction will be rolled back instead
    pub async fn execute_transaction(&self, statements: &[&str], commit: bool) -> Result<()> {
        let client = self.new_client().await?;
        client.execute("BEGIN", &[]).await?;
        for sql in statements {
            client.execute(*sql, &[]).await?;
        }
        if commit {
            client.execute("COMMIT", &[]).await?;
        } else {
            client.execute("ROLLBACK", &[]).await?;
        }
        Ok(())
    }

    pub fn cdc_connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.replication_user, self.replication_password, self.host, self.port, self.database
        )
    }

    /// Cleanup any leftover test replication slots from previous runs
    pub async fn cleanup_test_slots(&self) -> Result<()> {
        let client = self.new_client().await?;

        // Find slots that match our test pattern
        let rows = client
            .query(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'test_slot_%'",
                &[],
            )
            .await?;

        for row in rows {
            let slot_name: &str = row.get(0);
            tracing::debug!("Cleaning up leftover slot: {}", slot_name);

            // Terminate any active connection
            let _ = client
                .execute(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&slot_name],
                )
                .await;

            sleep(Duration::from_millis(100)).await;

            // Drop the slot
            let _ = client
                .execute(
                    &format!("SELECT pg_drop_replication_slot('{}')", slot_name),
                    &[],
                )
                .await;
        }

        Ok(())
    }
}

/// Test context with unique identifiers for isolation
pub struct TestContext {
    pub pg: Arc<PostgresTestContainer>,
    pub slot_name: String,
    pub publication_name: String,
}

impl TestContext {
    pub async fn new(pg: Arc<PostgresTestContainer>) -> Result<Self> {
        let test_id = &uuid::Uuid::new_v4().to_string()[..8];
        let slot_name = format!("test_slot_{}", test_id);
        let publication_name = format!("test_pub_{}", test_id);

        pg.create_publication(&publication_name).await?;
        pg.create_replication_slot(&slot_name).await?;

        Ok(Self {
            pg,
            slot_name,
            publication_name,
        })
    }

    pub async fn cleanup(&self) -> Result<()> {
        let _ = self.pg.execute(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{}'",
            self.slot_name
        )).await;

        sleep(Duration::from_millis(100)).await;
        self.pg.drop_replication_slot(&self.slot_name).await.ok();
        let _ = self
            .pg
            .execute(&format!(
                "DROP PUBLICATION IF EXISTS {}",
                self.publication_name
            ))
            .await;

        Ok(())
    }
}

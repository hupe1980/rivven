use std::process::Command;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;

/// Atomic port counter to avoid conflicts when running multiple containers
static PORT_COUNTER: AtomicU16 = AtomicU16::new(5433);

/// Supported PostgreSQL versions for CDC testing
///
/// Only actively maintained versions are included:
/// - PostgreSQL 14: Supported until Nov 2026
/// - PostgreSQL 15: Supported until Nov 2027
/// - PostgreSQL 16: Supported until Nov 2028 (recommended)
/// - PostgreSQL 17: Supported until Nov 2029 (latest)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresVersion {
    /// PostgreSQL 14 - Streaming large transactions
    V14,
    /// PostgreSQL 15 - Row filters, column lists in publications
    V15,
    /// PostgreSQL 16 - Parallel apply, logical decoding improvements (recommended)
    V16,
    /// PostgreSQL 17 - Latest with enhanced logical replication
    V17,
}

#[allow(dead_code)]
impl PostgresVersion {
    /// Get the Docker image tag for this version
    pub fn docker_image(&self) -> &'static str {
        match self {
            PostgresVersion::V14 => "postgres:14-alpine",
            PostgresVersion::V15 => "postgres:15-alpine",
            PostgresVersion::V16 => "postgres:16-alpine",
            PostgresVersion::V17 => "postgres:17-alpine",
        }
    }

    /// Get the major version number
    pub fn major_version(&self) -> u8 {
        match self {
            PostgresVersion::V14 => 14,
            PostgresVersion::V15 => 15,
            PostgresVersion::V16 => 16,
            PostgresVersion::V17 => 17,
        }
    }

    /// Check if this version supports row filters in publications (PG15+)
    pub fn supports_row_filters(&self) -> bool {
        self.major_version() >= 15
    }

    /// Check if this version supports column lists in publications (PG15+)
    pub fn supports_column_lists(&self) -> bool {
        self.major_version() >= 15
    }

    /// Check if this version supports parallel apply (PG16+)
    pub fn supports_parallel_apply(&self) -> bool {
        self.major_version() >= 16
    }

    /// All supported versions for matrix testing
    pub fn all() -> &'static [PostgresVersion] {
        &[
            PostgresVersion::V14,
            PostgresVersion::V15,
            PostgresVersion::V16,
            PostgresVersion::V17,
        ]
    }

    /// Default version for single-version tests (recommended stable)
    pub fn default() -> PostgresVersion {
        PostgresVersion::V16
    }
}

impl std::fmt::Display for PostgresVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PostgreSQL {}", self.major_version())
    }
}

/// PostgreSQL container for CDC integration tests
///
/// Supports multiple PostgreSQL versions (14-17) for comprehensive testing.
/// Uses direct Docker commands for simplicity and reliability.
///
/// # Example
///
/// ```rust,ignore
/// // Start with default version (PostgreSQL 16)
/// let pg = PostgresContainer::start().await?;
///
/// // Start with specific version
/// let pg = PostgresContainer::start_version(PostgresVersion::V17).await?;
///
/// // Execute SQL
/// pg.execute_sql("INSERT INTO users (name) VALUES ('test')").await?;
/// ```
#[allow(dead_code)]
pub struct PostgresContainer {
    pub port: u16,
    pub version: PostgresVersion,
    container_name: String,
}

#[allow(dead_code)]
impl PostgresContainer {
    /// Start a PostgreSQL container with the default version (PostgreSQL 16)
    pub async fn start() -> anyhow::Result<Self> {
        Self::start_version(PostgresVersion::default()).await
    }

    /// Start a PostgreSQL container with a specific version
    pub async fn start_version(version: PostgresVersion) -> anyhow::Result<Self> {
        let container_name = format!(
            "rivven-test-pg{}-{}",
            version.major_version(),
            uuid::Uuid::new_v4()
        );
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        println!(
            "🐘 Starting {} container: {} on port {}",
            version, container_name, port
        );

        // Start container with appropriate settings for the version
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container_name,
                "-e",
                "POSTGRES_PASSWORD=test_password",
                "-e",
                "POSTGRES_USER=postgres",
                "-e",
                "POSTGRES_DB=testdb",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=scram-sha-256",
                "-e",
                "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256",
                "-p",
                &format!("{}:5432", port),
                version.docker_image(),
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=10",
                "-c",
                "max_wal_senders=10",
                "-c",
                "password_encryption=scram-sha-256",
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to start {}: {}",
                version,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Wait for Postgres to be ready
        let mut ready = false;
        for _ in 0..60 {
            let check = Command::new("docker")
                .args(["exec", &container_name, "pg_isready", "-U", "postgres"])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);

            if check {
                ready = true;
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }

        if !ready {
            // Cleanup on failure
            let _ = Command::new("docker")
                .args(["rm", "-f", &container_name])
                .output();
            anyhow::bail!("Timeout waiting for {} to be ready", version);
        }

        let container = Self {
            port,
            version,
            container_name: container_name.clone(),
        };

        // Setup CDC
        container.setup_cdc().await?;

        println!(
            "✅ {} container ready: {} on port {}",
            version, container_name, port
        );

        Ok(container)
    }

    async fn setup_cdc(&self) -> anyhow::Result<()> {
        // Step 1: Create user and table
        let setup_sql = r#"
            CREATE USER rivven_cdc WITH REPLICATION PASSWORD 'test_password';
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO rivven_cdc;
            GRANT USAGE ON SCHEMA public TO rivven_cdc;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rivven_cdc;
            
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email VARCHAR(255),
                age INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            -- Set REPLICA IDENTITY FULL for complete before/after images
            ALTER TABLE users REPLICA IDENTITY FULL;
        "#;

        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "psql",
                "-U",
                "postgres",
                "-d",
                "testdb",
                "-c",
                setup_sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to setup CDC users/tables: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Step 2: Create publication
        let pub_sql = "CREATE PUBLICATION rivven_pub FOR ALL TABLES;";
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "psql",
                "-U",
                "postgres",
                "-d",
                "testdb",
                "-c",
                pub_sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to create publication: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Step 3: Create replication slot
        let slot_sql = "SELECT pg_create_logical_replication_slot('rivven_slot', 'pgoutput');";
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "psql",
                "-U",
                "postgres",
                "-d",
                "testdb",
                "-c",
                slot_sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to create replication slot: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }

    /// Execute SQL and return the output
    pub async fn execute_sql(&self, sql: &str) -> anyhow::Result<String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "psql",
                "-U",
                "postgres",
                "-d",
                "testdb",
                "-c",
                sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "SQL execution failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Get the connection string for this container
    pub fn connection_string(&self) -> String {
        format!(
            "postgresql://rivven_cdc:test_password@localhost:{}/testdb",
            self.port
        )
    }

    /// Get the PostgreSQL server version string
    pub async fn server_version(&self) -> anyhow::Result<String> {
        let output = self.execute_sql("SHOW server_version;").await?;
        Ok(output.trim().to_string())
    }

    /// Create a publication with row filters (PG15+ only)
    pub async fn create_filtered_publication(
        &self,
        pub_name: &str,
        table: &str,
        filter: &str,
    ) -> anyhow::Result<()> {
        if !self.version.supports_row_filters() {
            anyhow::bail!(
                "Row filters require PostgreSQL 15+, but {} is running",
                self.version
            );
        }

        let sql = format!(
            "CREATE PUBLICATION {} FOR TABLE {} WHERE ({});",
            pub_name, table, filter
        );
        self.execute_sql(&sql).await?;
        Ok(())
    }

    /// Create a publication with column lists (PG15+ only)
    pub async fn create_column_list_publication(
        &self,
        pub_name: &str,
        table: &str,
        columns: &[&str],
    ) -> anyhow::Result<()> {
        if !self.version.supports_column_lists() {
            anyhow::bail!(
                "Column lists require PostgreSQL 15+, but {} is running",
                self.version
            );
        }

        let cols = columns.join(", ");
        let sql = format!(
            "CREATE PUBLICATION {} FOR TABLE {} ({});",
            pub_name, table, cols
        );
        self.execute_sql(&sql).await?;
        Ok(())
    }
}

impl Drop for PostgresContainer {
    fn drop(&mut self) {
        println!(
            "🧹 Cleaning up {} container: {}",
            self.version, self.container_name
        );
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output();
    }
}

/// Run a test across all supported PostgreSQL versions
#[macro_export]
macro_rules! test_all_postgres_versions {
    ($test_fn:ident) => {
        paste::paste! {
            #[tokio::test]
            #[ignore]
            async fn [<$test_fn _pg14>]() -> anyhow::Result<()> {
                $test_fn(PostgresVersion::V14).await
            }

            #[tokio::test]
            #[ignore]
            async fn [<$test_fn _pg15>]() -> anyhow::Result<()> {
                $test_fn(PostgresVersion::V15).await
            }

            #[tokio::test]
            #[ignore]
            async fn [<$test_fn _pg16>]() -> anyhow::Result<()> {
                $test_fn(PostgresVersion::V16).await
            }

            #[tokio::test]
            #[ignore]
            async fn [<$test_fn _pg17>]() -> anyhow::Result<()> {
                $test_fn(PostgresVersion::V17).await
            }
        }
    };
}

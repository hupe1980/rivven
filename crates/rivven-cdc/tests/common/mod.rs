use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

/// Simple PostgreSQL container for basic integration tests
/// Uses direct Docker commands for simplicity
pub struct PostgresContainer {
    pub port: u16,
    container_name: String,
}

impl PostgresContainer {
    pub async fn start() -> anyhow::Result<Self> {
        let container_name = format!("rivven-test-pg-{}", uuid::Uuid::new_v4());
        let port = 5433; // Avoid conflict
        
        println!("🐘 Starting Postgres container: {}", container_name);
        
        // Start container with pg11 (uses md5 auth by default)
        let output = Command::new("docker")
            .args(&[
                "run", "-d",
                "--name", &container_name,
                "-e", "POSTGRES_PASSWORD=test_password",
                "-e", "POSTGRES_USER=postgres",
                "-e", "POSTGRES_DB=testdb",
                "-e", "POSTGRES_HOST_AUTH_METHOD=md5",
                "-p", &format!("{}:5432", port),
                "postgres:11-alpine",
                "-c", "wal_level=logical",
                "-c", "max_replication_slots=10",
                "-c", "max_wal_senders=10",
            ])
            .output()?;
        
        if !output.status.success() {
            anyhow::bail!("Failed to start Postgres: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        // Wait for Postgres to be ready
        for _ in 0..30 {
            let ready = Command::new("docker")
                .args(&["exec", &container_name, "pg_isready", "-U", "postgres"])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            
            if ready {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        
        let container = Self {
            port,
            container_name: container_name.clone(),
        };
        
        // Setup CDC
        container.setup_cdc().await?;
        
        println!("✅ Postgres container ready: {}", container_name);
        
        Ok(container)
    }
    
    async fn setup_cdc(&self) -> anyhow::Result<()> {
        // Step 1: Create user and table (can be in one transaction)
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
        "#;
        
        let output = Command::new("docker")
            .args(&[
                "exec", "-i", &self.container_name,
                "psql", "-U", "postgres", "-d", "testdb",
                "-c", setup_sql,
            ])
            .output()?;
        
        if !output.status.success() {
            anyhow::bail!("Failed to setup CDC users/tables: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        // Step 2: Create publication (must be separate)
        let pub_sql = "CREATE PUBLICATION rivven_pub FOR ALL TABLES;";
        let output = Command::new("docker")
            .args(&[
                "exec", "-i", &self.container_name,
                "psql", "-U", "postgres", "-d", "testdb",
                "-c", pub_sql,
            ])
            .output()?;
        
        if !output.status.success() {
            anyhow::bail!("Failed to create publication: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        // Step 3: Create replication slot (must be in its own transaction with no writes)
        let slot_sql = "SELECT pg_create_logical_replication_slot('rivven_slot', 'pgoutput');";
        let output = Command::new("docker")
            .args(&[
                "exec", "-i", &self.container_name,
                "psql", "-U", "postgres", "-d", "testdb",
                "-c", slot_sql,
            ])
            .output()?;
        
        if !output.status.success() {
            anyhow::bail!("Failed to create replication slot: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        Ok(())
    }
    
    pub async fn execute_sql(&self, sql: &str) -> anyhow::Result<String> {
        let output = Command::new("docker")
            .args(&[
                "exec", "-i", &self.container_name,
                "psql", "-U", "postgres", "-d", "testdb",
                "-c", sql,
            ])
            .output()?;
        
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

impl Drop for PostgresContainer {
    fn drop(&mut self) {
        println!("🧹 Cleaning up Postgres container: {}", self.container_name);
        let _ = Command::new("docker")
            .args(&["rm", "-f", &self.container_name])
            .output();
    }
}

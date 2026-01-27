//! MySQL/MariaDB testcontainer harness for CDC integration tests

use anyhow::{Context, Result};
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;
use tokio::time::Duration;
use tracing::info;

/// MySQL test container wrapper
pub struct MySqlTestContainer {
    #[allow(dead_code)]
    container: ContainerAsync<Mysql>,
    host: String,
    port: u16,
}

#[allow(dead_code)]
impl MySqlTestContainer {
    /// Start a MySQL container configured for CDC/binlog replication
    pub async fn start() -> Result<Self> {
        info!("Starting MySQL test container...");

        let container = Mysql::default()
            // Enable row-based binlog for CDC
            .with_env_var("MYSQL_ROOT_PASSWORD", "rootpassword")
            // MySQL 8.0 settings for replication
            .with_cmd(vec![
                "--server-id=1",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--gtid_mode=ON",
                "--enforce-gtid-consistency=ON",
                "--log-slave-updates=ON",
                // Use mysql_native_password for test compatibility (no SSL)
                "--default-authentication-plugin=mysql_native_password",
            ])
            .start()
            .await
            .context("Failed to start MySQL container")?;

        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(3306).await?;

        info!("MySQL container started at {}:{}", host, port);

        // Wait for MySQL to be ready
        Self::wait_for_mysql(&host, port).await?;

        Ok(Self {
            container,
            host,
            port,
        })
    }

    /// Wait for MySQL to accept connections
    async fn wait_for_mysql(host: &str, port: u16) -> Result<()> {
        let timeout_duration = Duration::from_secs(60);
        let check_interval = Duration::from_millis(500);

        let start = std::time::Instant::now();
        while start.elapsed() < timeout_duration {
            // Try to connect using our MySQL client
            match mysql_connect_check(host, port).await {
                Ok(_) => {
                    info!("MySQL is ready");
                    return Ok(());
                }
                Err(e) => {
                    tracing::debug!("MySQL not ready yet: {}", e);
                    tokio::time::sleep(check_interval).await;
                }
            }
        }

        anyhow::bail!("MySQL did not become ready within {:?}", timeout_duration);
    }

    /// Get connection host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get connection port
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get root user
    pub fn user(&self) -> &str {
        "root"
    }

    /// Get root password
    pub fn password(&self) -> &str {
        "rootpassword"
    }

    /// Execute SQL statement using our MySQL protocol client
    pub async fn execute(&self, sql: &str) -> Result<()> {
        use rivven_cdc::mysql::MySqlBinlogClient;

        let mut client = MySqlBinlogClient::connect(
            &self.host,
            self.port,
            self.user(),
            Some(self.password()),
            None,
        )
        .await?;

        client.query(sql).await?;
        Ok(())
    }

    /// Execute multiple SQL statements  
    pub async fn execute_batch(&self, statements: &[&str]) -> Result<()> {
        use rivven_cdc::mysql::MySqlBinlogClient;

        let mut client = MySqlBinlogClient::connect(
            &self.host,
            self.port,
            self.user(),
            Some(self.password()),
            None,
        )
        .await?;

        for stmt in statements {
            client.query(stmt).await?;
        }
        Ok(())
    }

    /// Create a test database
    pub async fn create_database(&self, name: &str) -> Result<()> {
        self.execute(&format!("CREATE DATABASE IF NOT EXISTS `{}`", name))
            .await
    }

    /// Create a replication user for CDC
    pub async fn create_replication_user(&self, user: &str, password: &str) -> Result<()> {
        let statements = [
            &format!(
                "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'",
                user, password
            ) as &str,
            &format!(
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'",
                user
            ),
            &format!("GRANT SELECT ON *.* TO '{}'@'%'", user),
            "FLUSH PRIVILEGES",
        ];

        self.execute_batch(&statements).await
    }

    /// Create MySQL CDC config for this container
    pub fn cdc_config(&self, server_id: u32) -> rivven_cdc::mysql::MySqlCdcConfig {
        rivven_cdc::mysql::MySqlCdcConfig::new(&self.host, self.user())
            .with_password(self.password())
            .with_port(self.port)
            .with_server_id(server_id)
    }
}

/// Simple MySQL connectivity check
async fn mysql_connect_check(host: &str, port: u16) -> Result<()> {
    use rivven_cdc::mysql::MySqlBinlogClient;

    // Try to connect
    let _client =
        MySqlBinlogClient::connect(host, port, "root", Some("rootpassword"), None).await?;

    // Connection successful
    Ok(())
}

/// MariaDB test container wrapper
pub struct MariaDbTestContainer {
    #[allow(dead_code)]
    container: ContainerAsync<testcontainers_modules::mariadb::Mariadb>,
    host: String,
    port: u16,
}

#[allow(dead_code)]
impl MariaDbTestContainer {
    /// Start a MariaDB container configured for CDC/binlog replication
    pub async fn start() -> Result<Self> {
        use testcontainers_modules::mariadb::Mariadb;

        info!("Starting MariaDB test container...");

        let container = Mariadb::default()
            .with_env_var("MARIADB_ROOT_PASSWORD", "rootpassword")
            // MariaDB settings for replication
            .with_cmd(vec![
                "--server-id=1",
                "--log-bin=mariadb-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
            ])
            .start()
            .await
            .context("Failed to start MariaDB container")?;

        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(3306).await?;

        info!("MariaDB container started at {}:{}", host, port);

        // Wait for MariaDB to be ready (uses same protocol as MySQL)
        MySqlTestContainer::wait_for_mysql(&host, port).await?;

        Ok(Self {
            container,
            host,
            port,
        })
    }

    /// Get connection host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get connection port
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get root user
    pub fn user(&self) -> &str {
        "root"
    }

    /// Get root password
    pub fn password(&self) -> &str {
        "rootpassword"
    }

    /// Execute SQL statement using our MySQL protocol client
    pub async fn execute(&self, sql: &str) -> Result<()> {
        use rivven_cdc::mysql::MySqlBinlogClient;

        let mut client = MySqlBinlogClient::connect(
            &self.host,
            self.port,
            self.user(),
            Some(self.password()),
            None,
        )
        .await?;

        client.query(sql).await?;
        Ok(())
    }

    /// Execute multiple SQL statements  
    pub async fn execute_batch(&self, statements: &[&str]) -> Result<()> {
        use rivven_cdc::mysql::MySqlBinlogClient;

        let mut client = MySqlBinlogClient::connect(
            &self.host,
            self.port,
            self.user(),
            Some(self.password()),
            None,
        )
        .await?;

        for stmt in statements {
            client.query(stmt).await?;
        }
        Ok(())
    }

    /// Create a test database
    pub async fn create_database(&self, name: &str) -> Result<()> {
        self.execute(&format!("CREATE DATABASE IF NOT EXISTS `{}`", name))
            .await
    }

    /// Create MariaDB CDC config for this container
    pub fn cdc_config(&self, server_id: u32) -> rivven_cdc::mysql::MySqlCdcConfig {
        rivven_cdc::mysql::MySqlCdcConfig::new(&self.host, self.user())
            .with_password(self.password())
            .with_port(self.port)
            .with_server_id(server_id)
    }
}

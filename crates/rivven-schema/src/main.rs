//! Rivven Schema Registry Server
//!
//! Confluent-compatible schema registry as a standalone binary.
//!
//! ## Usage
//!
//! ```bash
//! # Start server with in-memory storage
//! rivven-schema serve --port 8081
//!
//! # Check health
//! rivven-schema health --url http://localhost:8081
//! ```

use clap::{Parser, Subcommand};
use rivven_schema::{RegistryConfig, SchemaRegistry};
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "rivven-schema")]
#[command(
    author,
    version,
    about = "Rivven Schema Registry - Confluent-compatible schema management"
)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the schema registry server
    Serve {
        /// Port to listen on
        #[arg(short, long, default_value = "8081")]
        port: u16,

        /// Host to bind to
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Default compatibility level
        #[arg(long, default_value = "BACKWARD")]
        compatibility: String,

        /// Enable schema normalization
        #[arg(long, default_value = "true")]
        normalize: bool,
    },

    /// Check server health
    Health {
        /// Schema registry URL
        #[arg(long, default_value = "http://localhost:8081")]
        url: String,
    },

    /// Register a schema
    Register {
        /// Schema registry URL
        #[arg(long, default_value = "http://localhost:8081")]
        url: String,

        /// Subject name
        #[arg(short, long)]
        subject: String,

        /// Schema type (avro, json, protobuf)
        #[arg(short = 't', long, default_value = "avro")]
        schema_type: String,

        /// Schema file path
        #[arg(short = 'f', long)]
        file: String,
    },

    /// Get schema by ID
    Get {
        /// Schema registry URL
        #[arg(long, default_value = "http://localhost:8081")]
        url: String,

        /// Schema ID
        #[arg(short, long)]
        id: u32,
    },

    /// List subjects
    Subjects {
        /// Schema registry URL
        #[arg(long, default_value = "http://localhost:8081")]
        url: String,
    },

    /// Check schema compatibility
    Compat {
        /// Schema registry URL
        #[arg(long, default_value = "http://localhost:8081")]
        url: String,

        /// Subject name
        #[arg(short, long)]
        subject: String,

        /// Schema file path
        #[arg(short = 'f', long)]
        file: String,

        /// Version to check against (default: latest)
        #[arg(short, long)]
        version: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = match cli.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    match cli.command {
        Commands::Serve {
            port,
            host,
            compatibility,
            normalize,
        } => serve(port, host, compatibility, normalize).await,
        Commands::Health { url } => health_check(&url).await,
        Commands::Register {
            url,
            subject,
            schema_type,
            file,
        } => register_schema(&url, &subject, &schema_type, &file).await,
        Commands::Get { url, id } => get_schema(&url, id).await,
        Commands::Subjects { url } => list_subjects(&url).await,
        Commands::Compat {
            url,
            subject,
            file,
            version,
        } => check_compatibility(&url, &subject, &file, version).await,
    }
}

#[cfg(feature = "server")]
async fn serve(
    port: u16,
    host: String,
    compatibility: String,
    _normalize: bool,
) -> anyhow::Result<()> {
    use rivven_schema::{SchemaServer, ServerConfig};

    info!("Starting Rivven Schema Registry on {}:{}", host, port);
    info!("Default compatibility: {}", compatibility);

    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await?;

    let server_config = ServerConfig {
        host: host.clone(),
        port,
        #[cfg(feature = "auth")]
        auth: None, // Authentication disabled by default
    };

    let server = SchemaServer::new(registry, server_config);
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    info!("Schema Registry listening on http://{}", addr);
    info!("API endpoints:");
    info!("  POST /subjects/{{subject}}/versions - Register schema");
    info!("  GET  /schemas/ids/{{id}} - Get schema by ID");
    info!("  GET  /subjects - List subjects");
    info!("  GET  /subjects/{{subject}}/versions - List versions");

    server.run(addr).await?;

    Ok(())
}

#[cfg(not(feature = "server"))]
async fn serve(
    _port: u16,
    _host: String,
    _compatibility: String,
    _normalize: bool,
) -> anyhow::Result<()> {
    anyhow::bail!("Server feature not enabled. Rebuild with --features server")
}

async fn health_check(url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client.get(format!("{}/", url)).send().await?;

    if response.status().is_success() {
        println!("✓ Schema Registry is healthy");
        let body: serde_json::Value = response.json().await?;
        println!(
            "  Version: {}",
            body.get("version").unwrap_or(&serde_json::json!("unknown"))
        );
        Ok(())
    } else {
        anyhow::bail!("Schema Registry health check failed: {}", response.status())
    }
}

async fn register_schema(
    url: &str,
    subject: &str,
    schema_type: &str,
    file: &str,
) -> anyhow::Result<()> {
    let schema_content = std::fs::read_to_string(file)?;
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "schemaType": schema_type.to_uppercase(),
        "schema": schema_content,
    });

    let response = client
        .post(format!("{}/subjects/{}/versions", url, subject))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&body)
        .send()
        .await?;

    if response.status().is_success() {
        let result: serde_json::Value = response.json().await?;
        let id = result.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("✓ Schema registered successfully");
        println!("  Subject: {}", subject);
        println!("  Schema ID: {}", id);
        Ok(())
    } else {
        let error: serde_json::Value = response.json().await?;
        anyhow::bail!(
            "Failed to register schema: {}",
            error
                .get("message")
                .unwrap_or(&serde_json::json!("Unknown error"))
        )
    }
}

async fn get_schema(url: &str, id: u32) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/schemas/ids/{}", url, id))
        .send()
        .await?;

    if response.status().is_success() {
        let result: serde_json::Value = response.json().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    } else {
        anyhow::bail!("Schema not found: {}", id)
    }
}

async fn list_subjects(url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let response = client.get(format!("{}/subjects", url)).send().await?;

    if response.status().is_success() {
        let subjects: Vec<String> = response.json().await?;
        println!("Subjects ({}):", subjects.len());
        for subject in subjects {
            println!("  - {}", subject);
        }
        Ok(())
    } else {
        anyhow::bail!("Failed to list subjects")
    }
}

async fn check_compatibility(
    url: &str,
    subject: &str,
    file: &str,
    version: Option<u32>,
) -> anyhow::Result<()> {
    let schema_content = std::fs::read_to_string(file)?;
    let client = reqwest::Client::new();

    let version_str = version
        .map(|v| v.to_string())
        .unwrap_or_else(|| "latest".to_string());
    let endpoint = format!(
        "{}/compatibility/subjects/{}/versions/{}",
        url, subject, version_str
    );

    let body = serde_json::json!({
        "schema": schema_content,
    });

    let response = client
        .post(&endpoint)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&body)
        .send()
        .await?;

    if response.status().is_success() {
        let result: serde_json::Value = response.json().await?;
        let is_compatible = result
            .get("is_compatible")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if is_compatible {
            println!("✓ Schema is compatible");
        } else {
            println!("✗ Schema is NOT compatible");
            if let Some(messages) = result.get("messages") {
                println!("  Errors: {}", messages);
            }
        }
        Ok(())
    } else {
        let error: serde_json::Value = response.json().await?;
        anyhow::bail!(
            "Compatibility check failed: {}",
            error
                .get("message")
                .unwrap_or(&serde_json::json!("Unknown error"))
        )
    }
}

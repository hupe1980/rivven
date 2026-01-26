//! Rivven CLI - Command line interface for Rivven
//!
//! This is the main CLI for interacting with Rivven servers.
//! For CDC (Change Data Capture), use the separate `rivven-connect` binary.

use clap::{Parser, Subcommand};
use rivven_client::Client;
use rivven_core::Config;
use rivven_server::Server;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "rivven")]
#[command(about = "Rivven - A high-performance distributed event streaming platform")]
#[command(version)]
#[command(after_help = "For CDC (Change Data Capture), use the separate 'rivven-connect' binary.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Rivven server (broker)
    Server {
        /// Port to bind to
        #[arg(short, long, default_value = "9092")]
        port: u16,

        /// Bind address
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,

        /// Number of default partitions for new topics
        #[arg(short = 'n', long, default_value = "3")]
        partitions: u32,

        /// Enable persistence to disk
        #[arg(long)]
        persist: bool,

        /// Data directory for persistence
        #[arg(long, default_value = "./data")]
        data_dir: String,
    },

    /// Topic management
    Topic {
        #[command(subcommand)]
        action: TopicCommands,
    },

    /// Consumer group management
    Group {
        #[command(subcommand)]
        action: GroupCommands,
    },

    /// Publish a message to a topic
    Produce {
        /// Topic name
        topic: String,

        /// Message to publish
        message: String,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,

        /// Partition (optional, uses round-robin if not specified)
        #[arg(short, long)]
        partition: Option<u32>,

        /// Message key (optional)
        #[arg(short, long)]
        key: Option<String>,
    },

    /// Consume messages from a topic
    Consume {
        /// Topic name
        topic: String,

        /// Partition
        #[arg(short, long, default_value = "0")]
        partition: u32,

        /// Starting offset
        #[arg(short, long, default_value = "0")]
        offset: u64,

        /// Maximum messages to consume
        #[arg(short, long, default_value = "100")]
        max: usize,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,

        /// Follow mode - continuously consume new messages (like tail -f)
        #[arg(short, long)]
        follow: bool,
    },

    /// Ping the server to check connectivity
    Ping {
        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },
}

#[derive(Subcommand)]
enum TopicCommands {
    /// Create a new topic
    Create {
        /// Topic name
        name: String,

        /// Number of partitions
        #[arg(short, long, default_value = "3")]
        partitions: u32,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },

    /// List all topics
    List {
        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },

    /// Delete a topic
    Delete {
        /// Topic name
        name: String,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },

    /// Get topic metadata
    Info {
        /// Topic name
        name: String,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },
}

#[derive(Subcommand)]
enum GroupCommands {
    /// List all consumer groups
    List {
        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },

    /// Describe a consumer group (show all committed offsets)
    Describe {
        /// Consumer group name
        name: String,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },

    /// Delete a consumer group
    Delete {
        /// Consumer group name
        name: String,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            port,
            bind,
            partitions,
            persist,
            data_dir,
        } => {
            println!("Starting Rivven server...");
            println!("  Bind: {}:{}", bind, port);
            println!("  Partitions: {}", partitions);
            println!("  Persistence: {}", if persist { &data_dir } else { "disabled" });
            println!();
            
            let config = Config::default()
                .with_port(port)
                .with_bind_address(bind)
                .with_default_partitions(partitions)
                .with_persistence(persist)
                .with_data_dir(data_dir);

            let server = Server::new(config).await?;
            
            // Setup graceful shutdown
            let running = Arc::new(AtomicBool::new(true));
            let r = running.clone();
            
            ctrlc::set_handler(move || {
                println!("\nShutting down...");
                r.store(false, Ordering::SeqCst);
            })?;
            
            server.start().await?;
        }

        Commands::Topic { action } => match action {
            TopicCommands::Create {
                name,
                partitions,
                server,
            } => {
                let mut client = Client::connect(&server).await?;
                let num_partitions = client.create_topic(name.clone(), Some(partitions)).await?;
                println!("✓ Created topic '{}' with {} partitions", name, num_partitions);
            }

            TopicCommands::List { server } => {
                let mut client = Client::connect(&server).await?;
                let topics = client.list_topics().await?;
                
                if topics.is_empty() {
                    println!("No topics found");
                } else {
                    println!("Topics:");
                    for topic in topics {
                        println!("  • {}", topic);
                    }
                }
            }

            TopicCommands::Delete { name, server } => {
                let mut client = Client::connect(&server).await?;
                client.delete_topic(&name).await?;
                println!("✓ Deleted topic '{}'", name);
            }

            TopicCommands::Info { name, server } => {
                let mut client = Client::connect(&server).await?;
                let (topic_name, partitions) = client.get_metadata(&name).await?;
                println!("Topic: {}", topic_name);
                println!("Partitions: {}", partitions);
            }
        },

        Commands::Group { action } => match action {
            GroupCommands::List { server } => {
                let mut client = Client::connect(&server).await?;
                let groups = client.list_groups().await?;
                
                if groups.is_empty() {
                    println!("No consumer groups found");
                } else {
                    println!("Consumer Groups:");
                    for group in groups {
                        println!("  • {}", group);
                    }
                }
            }

            GroupCommands::Describe { name, server } => {
                let mut client = Client::connect(&server).await?;
                let offsets = client.describe_group(&name).await?;
                
                println!("Consumer Group: {}", name);
                if offsets.is_empty() {
                    println!("  No committed offsets");
                } else {
                    for (topic, partitions) in offsets.iter() {
                        println!("  Topic: {}", topic);
                        let mut partitions_vec: Vec<_> = partitions.iter().collect();
                        partitions_vec.sort_by_key(|(p, _)| *p);
                        for (partition, offset) in partitions_vec {
                            println!("    Partition {}: offset {}", partition, offset);
                        }
                    }
                }
            }

            GroupCommands::Delete { name, server } => {
                let mut client = Client::connect(&server).await?;
                client.delete_group(&name).await?;
                println!("✓ Deleted consumer group '{}'", name);
            }
        },

        Commands::Produce {
            topic,
            message,
            server,
            partition,
            key,
        } => {
            let mut client = Client::connect(&server).await?;
            
            let offset = if let Some(p) = partition {
                client
                    .publish_to_partition(
                        &topic,
                        p,
                        key.clone().map(Bytes::from),
                        Bytes::from(message),
                    )
                    .await?
            } else {
                client
                    .publish_with_key(
                        &topic,
                        key.clone().map(Bytes::from),
                        Bytes::from(message),
                    )
                    .await?
            };

            println!("✓ Published to topic '{}' at offset {}", topic, offset);
        }

        Commands::Consume {
            topic,
            partition,
            offset,
            max,
            server,
            follow,
        } => {
            let mut client = Client::connect(&server).await?;
            
            if follow {
                // Follow mode - continuous consumption
                let running = Arc::new(AtomicBool::new(true));
                let r = running.clone();
                
                ctrlc::set_handler(move || {
                    println!("\n✓ Stopping consumer...");
                    r.store(false, Ordering::SeqCst);
                })?;
                
                let mut current_offset = offset;
                println!("Following topic '{}' partition {} (Ctrl+C to stop)...", topic, partition);
                
                while running.load(Ordering::SeqCst) {
                    let messages = client.consume(&topic, partition, current_offset, 10).await?;
                    
                    for msg in &messages {
                        let value = String::from_utf8_lossy(&msg.value);
                        let key = msg
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k))
                            .unwrap_or_else(|| "null".into());
                        
                        println!(
                            "[offset={}] key={} value={}",
                            msg.offset, key, value
                        );
                        current_offset = msg.offset + 1;
                    }
                    
                    if messages.is_empty() {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            } else {
                // One-shot consumption
                let messages = client.consume(&topic, partition, offset, max).await?;

                if messages.is_empty() {
                    println!("No messages found");
                } else {
                    println!(
                        "Consumed {} messages from topic '{}' partition {}:",
                        messages.len(),
                        topic,
                        partition
                    );
                    for msg in messages {
                        let value = String::from_utf8_lossy(&msg.value);
                        let key = msg
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k))
                            .unwrap_or_else(|| "null".into());
                        
                        println!(
                            "  [offset={}] key={} value={}",
                            msg.offset, key, value
                        );
                    }
                }
            }
        }

        Commands::Ping { server } => {
            let mut client = Client::connect(&server).await?;
            client.ping().await?;
            println!("✓ Pong from {}", server);
        }
    }

    Ok(())
}

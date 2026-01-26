use rivven_cdc::common::CdcSource;
/// Basic CDC Example
///
/// This demonstrates how to use Rivven's native Postgres CDC connector.
///
/// Prerequisites:
/// 1. Postgres 10+ with `wal_level=logical`
/// 2. CREATE PUBLICATION rivven_pub FOR ALL TABLES;
/// 3. Replication user with appropriate permissions
///
/// Run with:
/// ```
/// cargo run --example basic_cdc --features postgres
/// ```
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Connection string format: host=localhost port=5432 dbname=testdb user=postgres password=postgres
    let conn_str = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "host=localhost port=5432 dbname=testdb user=postgres password=postgres".to_string()
    });

    let slot_name = "rivven_slot";
    let publication = "rivven_pub";

    let config = PostgresCdcConfig::builder()
        .connection_string(conn_str)
        .slot_name(slot_name)
        .publication_name(publication)
        .build()?;

    let mut cdc = PostgresCdc::new(config);

    println!("Starting Postgres CDC...");
    println!("Slot: {}", slot_name);
    println!("Publication: {}", publication);
    println!("\nListening for changes...");

    cdc.start().await?;

    // Take the event receiver and print events
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
        }
        _ = async {
            while let Some(event) = rx.recv().await {
                println!("CDC Event: {:?}", event);
            }
        } => {}
    }

    cdc.stop().await?;
    Ok(())
}

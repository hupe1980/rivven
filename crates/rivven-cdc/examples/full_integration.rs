/// Full CDC Integration Example
/// 
/// This demonstrates the complete Rivven CDC pipeline:
/// 1. Postgres CDC → 2. Schema Inference → 3. Topic Routing → 4. Rivven Storage
///
/// Prerequisites:
/// ```sql
/// -- postgresql.conf
/// wal_level = logical
/// max_replication_slots = 5
/// 
/// -- Setup
/// CREATE DATABASE testdb;
/// \c testdb
/// 
/// CREATE TABLE users (
///     id SERIAL PRIMARY KEY,
///     name VARCHAR(100) NOT NULL,
///     email VARCHAR(255) UNIQUE,
///     created_at TIMESTAMP DEFAULT NOW()
/// );
/// 
/// CREATE PUBLICATION rivven_pub FOR ALL TABLES;
/// SELECT pg_create_logical_replication_slot('rivven_slot', 'pgoutput');
/// ```

use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_cdc::common::CdcSource;
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 Rivven CDC Full Integration Example\n");

    // Setup Postgres CDC
    let conn_str = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "host=localhost port=5432 dbname=testdb user=postgres password=postgres".to_string());

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("rivven_slot")
        .publication_name("rivven_pub")
        .buffer_size(1000)
        .build()?;

    let mut cdc = PostgresCdc::new(config);

    println!("📡 Starting CDC Streaming...");
    println!("   Connection: {}", conn_str);
    println!("   Slot: rivven_slot");
    println!("   Publication: rivven_pub\n");

    cdc.start().await?;
    
    // Get event receiver
    let mut event_rx = cdc.take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("Failed to get event receiver"))?;

    println!("🎯 CDC pipeline is now active!\n");
    println!("Try this in psql:");
    println!("   INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');");
    println!("   UPDATE users SET name = 'Alice Smith' WHERE id = 1;");
    println!("   DELETE FROM users WHERE id = 1;\n");
    println!("Press Ctrl+C to stop...\n");

    // Monitor events
    let mut event_count = 0u64;
    
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n🛑 Shutting down...");
                break;
            }
            event = event_rx.recv() => {
                match event {
                    Some(cdc_event) => {
                        event_count += 1;
                        println!("📥 Event #{}: {:?}", event_count, cdc_event.op);
                        println!("   Table: {}.{}", 
                            if cdc_event.schema.is_empty() { "public" } else { &cdc_event.schema },
                            cdc_event.table);
                        
                        if let Some(before) = &cdc_event.before {
                            println!("   Before: {:?}", before);
                        }
                        if let Some(after) = &cdc_event.after {
                            println!("   After: {:?}", after);
                        }
                        println!();
                    }
                    None => {
                        println!("⚠️  Event channel closed");
                        break;
                    }
                }
            }
        }
    }

    cdc.stop().await?;
    println!("✅ Processed {} CDC events total", event_count);
    
    Ok(())
}

//! Test harness for CDC integration tests
//!
//! Provides production-grade test infrastructure using testcontainers-rs.
//!
//! Features:
//! - Automatic PostgreSQL container lifecycle management
//! - Automatic MySQL/MariaDB container lifecycle management
//! - Pre-configured logical replication setup
//! - Async-first design with proper cleanup
//! - Test isolation with unique slot/publication names

pub mod assertions;
pub mod data_generators;
pub mod mysql;
pub mod postgres;
pub mod rivven_context;

pub use assertions::{get_string_field, CdcEventVecExt};
pub use data_generators::{
    generate_all_types_insert, generate_bulk_inserts, generate_mixed_workload,
    generate_user_inserts_for_table, scenarios,
};
pub use mysql::{MariaDbTestContainer, MySqlTestContainer};
pub use postgres::{PostgresTestContainer, TestContext};
pub use rivven_context::RivvenTestContext;

use std::sync::Once;

static INIT: Once = Once::new();

/// Initialize test logging (idempotent)
pub fn init_test_logging() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("rivven_cdc=debug".parse().unwrap())
                    .add_directive("testcontainers=info".parse().unwrap()),
            )
            .with_test_writer()
            .try_init()
            .ok();
    });
}

/// CDC startup wait time
pub const CDC_STARTUP_DELAY_MS: u64 = 2000;

/// Event propagation wait time  
pub const EVENT_PROPAGATION_DELAY_MS: u64 = 1000;

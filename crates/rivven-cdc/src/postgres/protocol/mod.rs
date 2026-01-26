//! PostgreSQL replication protocol implementation
//!
//! Custom TCP client for PostgreSQL logical replication using pgoutput.

pub mod client;
pub mod decoder;
pub mod message;

pub use client::*;
pub use decoder::*;
pub use message::*;

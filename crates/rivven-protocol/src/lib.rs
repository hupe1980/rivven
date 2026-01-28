//! Rivven Wire Protocol
//!
//! This crate defines the wire protocol types shared between rivven-client and rivvend.
//! It provides serialization/deserialization for all protocol messages.
//!
//! # Protocol Stability
//!
//! The enum variant order is significant for postcard serialization. Changes to variant
//! order will break wire compatibility with existing clients/servers.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_protocol::{Request, Response};
//!
//! // Serialize a request
//! let request = Request::Ping;
//! let bytes = request.to_bytes()?;
//!
//! // Deserialize a response
//! let response = Response::from_bytes(&bytes)?;
//! ```

mod error;
mod messages;
mod metadata;
mod types;

pub use error::{ProtocolError, Result};
pub use messages::{
    DeleteRecordsResult, QuotaAlteration, QuotaEntry, Request, Response, TopicConfigDescription,
    TopicConfigEntry, TopicConfigValue,
};
pub use metadata::{BrokerInfo, PartitionMetadata, TopicMetadata};
pub use types::MessageData;

/// Protocol version for compatibility checking
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum message size (64 MiB)
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

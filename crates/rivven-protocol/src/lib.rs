//! Rivven Wire Protocol
//!
//! This crate defines the wire protocol types shared between rivven-client and rivvend.
//! It provides serialization/deserialization for all protocol messages.
//!
//! # Wire Format
//!
//! All messages use a unified wire format with format auto-detection and correlation ID:
//!
//! ```text
//! ┌─────────────────┬─────────────────┬──────────────────────────┬──────────────────────┐
//! │ Length (4 bytes)│ Format (1 byte) │ Correlation ID (4 bytes) │ Payload (N bytes)    │
//! │ Big-endian u32  │ 0x00 = postcard │ Big-endian u32           │ Serialized message   │
//! │                 │ 0x01 = protobuf │                          │                      │
//! └─────────────────┴─────────────────┴──────────────────────────┴──────────────────────┘
//! ```
//!
//! - **postcard** (0x00): High-performance Rust-native binary format
//! - **protobuf** (0x01): Cross-language format for Go, Java, Python clients
//! - **correlation_id**: Matches responses to their originating requests
//!
//! # Protocol Stability
//!
//! The enum variant order is significant for postcard serialization. Changes to variant
//! order will break wire compatibility with existing clients/servers.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_protocol::{Request, Response, WireFormat};
//!
//! // Serialize with format prefix
//! let request = Request::Ping;
//! let bytes = request.to_wire(WireFormat::Postcard, 1)?;
//!
//! // Deserialize with auto-detection
//! let (response, format, correlation_id) = Response::from_wire(&bytes)?;
//! ```

mod error;
mod messages;
mod metadata;
pub mod serde_utils;
mod types;

// Protobuf types (optional, for cross-language clients)
#[cfg(feature = "protobuf")]
pub mod proto {
    //! Protobuf-generated types for cross-language client support.
    //!
    //! Enable with `--features protobuf`. Used by Go, Java, and other language clients.
    include!(concat!(env!("OUT_DIR"), "/rivven.protocol.v1.rs"));
}

// Protobuf conversion utilities
#[cfg(feature = "protobuf")]
mod proto_convert;

pub use error::{ProtocolError, Result};
pub use messages::{
    DeleteRecordsResult, QuotaAlteration, QuotaEntry, Request, Response, TopicConfigDescription,
    TopicConfigEntry, TopicConfigValue,
};
pub use metadata::{BrokerInfo, PartitionMetadata, TopicMetadata};
pub use types::{MessageData, SchemaType};

/// Protocol version for compatibility checking
pub const PROTOCOL_VERSION: u32 = 2;

/// Wire header size: format byte (1) + correlation_id (4)
pub const WIRE_HEADER_SIZE: usize = 5;

/// Maximum message size (10 MiB)
///
/// Aligned with the server default (`max_request_size` = 10 MB) and
/// `SecureServerConfig::max_message_size` to avoid silent truncation or
/// misleading protocol-level limits.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Wire format identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum WireFormat {
    /// Postcard format (Rust-native, fastest)
    #[default]
    Postcard = 0x00,
    /// Protobuf format (cross-language)
    Protobuf = 0x01,
}

impl WireFormat {
    /// Parse format from byte
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::Postcard),
            0x01 => Some(Self::Protobuf),
            _ => None,
        }
    }

    /// Convert to byte
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

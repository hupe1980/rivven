//! Auto-generated protobuf types
//!
//! This module contains Rust types generated from `proto/rivven.proto`.
//! Regenerate with: `cargo build --features protobuf`

#[cfg(feature = "protobuf")]
include!(concat!(env!("OUT_DIR"), "/rivven.protocol.v1.rs"));

#[cfg(not(feature = "protobuf"))]
pub fn protobuf_not_enabled() {
    compile_error!("Enable the 'protobuf' feature to use protobuf types");
}

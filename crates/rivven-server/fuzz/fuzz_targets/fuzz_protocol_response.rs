#![no_main]
//! Fuzz test for protocol response deserialization
//! 
//! Tests that the response parser handles malformed data gracefully.

use libfuzzer_sys::fuzz_target;
use rivven_server::protocol::Response;

fuzz_target!(|data: &[u8]| {
    // Try to deserialize arbitrary bytes as a Response
    let _ = Response::from_bytes(data);
    
    // Also try truncated inputs
    for i in 0..data.len().min(100) {
        let _ = Response::from_bytes(&data[..i]);
    }
});

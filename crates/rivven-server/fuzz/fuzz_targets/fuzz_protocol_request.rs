#![no_main]
//! Fuzz test for protocol request deserialization
//! 
//! This fuzzer tests that arbitrary bytes cannot crash the request parser.
//! It covers:
//! - Bincode deserialization edge cases
//! - Invalid enum variants
//! - Truncated data
//! - Malformed strings
//! - Integer overflow

use libfuzzer_sys::fuzz_target;
use rivven_server::protocol::Request;

fuzz_target!(|data: &[u8]| {
    // Try to deserialize arbitrary bytes as a Request
    // This should never panic - only return Ok or Err
    let _ = Request::from_bytes(data);
    
    // Also try with different sizes to catch buffer issues
    for i in 0..data.len().min(100) {
        let _ = Request::from_bytes(&data[..i]);
    }
});

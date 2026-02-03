//! Pluggable storage backends for Raft log persistence
//!
//! This module provides storage implementations for OpenRaft:
//!
//! - **redb** (default): Pure Rust, ACID, zero C dependencies
//! - **memory**: In-memory storage for testing
//!
//! # Why redb?
//!
//! Rivven chose `redb` over RocksDB for several key reasons:
//!
//! | Aspect | redb | RocksDB |
//! |--------|------|---------|
//! | **Build time** | ~10s | 2-5 min |
//! | **Binary size** | Minimal | +10-15 MB |
//! | **Cross-compile** | Works everywhere | Needs C++ toolchain |
//! | **Docker musl** | ✅ Works | ❌ Needs musl-g++ |
//! | **ACID** | ✅ Full | ✅ Full |
//! | **Maintainer** | dtolnay (serde) | Facebook |
//!
//! For Raft log workloads (append-heavy, sequential reads), redb's B-tree
//! structure provides excellent performance with zero C dependencies.
//!
//! # Example
//!
//! ```ignore
//! use rivven_cluster::storage::RedbLogStore;
//! use std::path::Path;
//!
//! let store = RedbLogStore::new(Path::new("/data/raft"))?;
//! ```

mod redb_store;

pub use redb_store::RedbLogStore;

#[cfg(test)]
mod tests;

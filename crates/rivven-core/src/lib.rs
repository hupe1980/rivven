pub mod error;
pub mod message;
pub mod partition;
pub mod storage;
pub mod topic;
pub mod offset;
pub mod config;
pub mod consumer_group;
pub mod schema_registry;
pub mod serde_utils;
pub mod metrics;
pub mod buffer_pool;
pub mod concurrent;
pub mod bloom;
pub mod zero_copy;
pub mod wal;
pub mod vectorized;
pub mod backpressure;
pub mod validation;
pub mod auth;
pub mod service_auth;

#[cfg(feature = "compression")]
pub mod compression;

pub mod async_io;

// Cedar authorization (optional)
#[cfg(feature = "cedar")]
pub mod cedar_authz;

// OIDC authentication (optional)
#[cfg(feature = "oidc")]
pub mod oidc;

// TLS/mTLS support (optional)
#[cfg(feature = "tls")]
pub mod tls;

pub use error::{Error, Result};
pub use buffer_pool::{BufferPool, BufferPoolConfig, PooledBuffer, BufferChain, SizeClass};
pub use concurrent::{
    LockFreeQueue, AppendOnlyLog, AppendLogConfig, 
    ConcurrentHashMap, ConcurrentSkipList, QueueStats,
};
pub use bloom::{
    BloomFilter, CountingBloomFilter, HyperLogLog, 
    OffsetBloomFilter, AdaptiveBatcher, BatchConfig, BatcherStats,
};
pub use storage::{
    TieredStorage, TieredStorageConfig, StorageTier,
    ColdStorageConfig, ColdStorageBackend, LocalFsColdStorage,
    HotTier, WarmTier, SegmentMetadata,
    TieredStorageStats, TieredStorageStatsSnapshot,
    HotTierStats, WarmTierStats,
};
pub use zero_copy::{
    ZeroCopyBuffer, BufferSlice, BufferRef, SmallVec,
    ZeroCopyBufferPool, ZeroCopyProducer, ZeroCopyConsumer,
    ConsumedMessage,
};
pub use wal::{
    GroupCommitWal, WalConfig, WalRecord, RecordType,
    WalReader, SyncMode, WalStatsSnapshot,
};
pub use vectorized::{
    BatchEncoder, BatchDecoder, BatchMessage, BatchProcessor,
    RecordBatch, RecordBatchIter,
};
pub use backpressure::{
    TokenBucket, TokenBucketStatsSnapshot,
    CreditFlowControl, CreditStatsSnapshot,
    AdaptiveRateLimiter, AdaptiveRateLimiterConfig, AdaptiveStatsSnapshot,
    CircuitBreaker, CircuitBreakerConfig, CircuitState, CircuitBreakerStatsSnapshot,
    BackpressureChannel, ChannelStatsSnapshot,
    WindowedRateTracker,
};
pub use message::Message;
pub use partition::Partition;
pub use topic::{Topic, TopicManager};
pub use offset::OffsetManager;
pub use config::Config;
pub use schema_registry::{SchemaRegistry, MemorySchemaRegistry};

pub use async_io::{AsyncIo, AsyncIoConfig, AsyncFile, BatchBuilder, AsyncSegment};
pub use validation::{Validator, ValidationError};
pub use auth::{
    AuthManager, AuthConfig, AuthError, AuthResult, AuthSession,
    Principal, PrincipalType, PasswordHash,
    Role, Permission, ResourceType,
    AclEntry, SaslPlainAuth, SaslScramAuth, ScramState,
};
pub use service_auth::{
    ServiceAuthManager, ServiceAuthConfig, ServiceAuthError, ServiceAuthResult,
    ServiceAccount, ServiceSession, ApiKey, AuthMethod,
    ServiceAuthRequest, ServiceAuthResponse,
};

// TLS re-exports
#[cfg(feature = "tls")]
pub use tls::{
    TlsConfig, TlsConfigBuilder, TlsError, TlsResult,
    TlsAcceptor, TlsConnector,
    TlsServerStream, TlsClientStream,
    TlsIdentity, TlsVersion, MtlsMode,
    CertificateSource, PrivateKeySource,
    TlsSecurityAudit,
    load_certificates, load_private_key, generate_self_signed,
    certificate_fingerprint, CertificateWatcher,
};

pub mod auth;
pub mod backpressure;
pub mod bloom;
pub mod buffer_pool;
pub mod concurrent;
pub mod config;
pub mod consumer_group;
pub mod error;
pub mod idempotent;
pub mod io_uring;
pub mod message;
pub mod metrics;
pub mod offset;
pub mod partition;
pub mod quota;
pub mod serde_utils;
pub mod service_auth;
pub mod storage;
pub mod topic;
pub mod topic_config;
pub mod transaction;
pub mod validation;
pub mod vectorized;
pub mod wal;
pub mod zero_copy;

#[cfg(feature = "compression")]
pub mod compression;

pub mod async_io;

// Encryption at rest (optional but default)
#[cfg(feature = "encryption")]
pub mod encryption;

// Cedar authorization (optional)
#[cfg(feature = "cedar")]
pub mod cedar_authz;
#[cfg(feature = "cedar")]
pub use cedar_authz::{
    AuthzContext, AuthzDecision, CedarAuthorizer, CedarError, CedarResult, RivvenAction,
    RivvenResource,
};

// OIDC authentication (optional)
#[cfg(feature = "oidc")]
pub mod oidc;

// TLS/mTLS support (optional)
#[cfg(feature = "tls")]
pub mod tls;

pub use backpressure::{
    AdaptiveRateLimiter, AdaptiveRateLimiterConfig, AdaptiveStatsSnapshot, BackpressureChannel,
    ChannelStatsSnapshot, CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStatsSnapshot,
    CircuitState, CreditFlowControl, CreditStatsSnapshot, TokenBucket, TokenBucketStatsSnapshot,
    WindowedRateTracker,
};
pub use bloom::{
    AdaptiveBatcher, BatchConfig, BatcherStats, BloomFilter, CountingBloomFilter, HyperLogLog,
    OffsetBloomFilter,
};
pub use buffer_pool::{BufferChain, BufferPool, BufferPoolConfig, PooledBuffer, SizeClass};
pub use concurrent::{
    AppendLogConfig, AppendOnlyLog, ConcurrentHashMap, ConcurrentSkipList, LockFreeQueue,
    QueueStats,
};
pub use config::Config;
pub use error::{Error, Result};
pub use idempotent::{
    IdempotentProducerManager, IdempotentProducerStats, PartitionProducerState, ProducerEpoch,
    ProducerId, ProducerMetadata, SequenceNumber, SequenceResult, NO_SEQUENCE,
};
pub use message::Message;
pub use offset::OffsetManager;
pub use partition::Partition;
pub use quota::{
    EntityQuotaStats, QuotaConfig, QuotaEntity, QuotaEntityType, QuotaManager, QuotaResult,
    QuotaStats, QuotaStatsSnapshot, QuotaType, DEFAULT_CONSUME_BYTES_RATE,
    DEFAULT_PRODUCE_BYTES_RATE, DEFAULT_REQUEST_RATE, UNLIMITED,
};
pub use storage::{
    ColdStorageBackend, ColdStorageConfig, HotTier, HotTierStats, LocalFsColdStorage,
    SegmentMetadata, StorageTier, TieredStorage, TieredStorageConfig, TieredStorageStats,
    TieredStorageStatsSnapshot, WarmTier, WarmTierStats,
};
pub use topic::{Topic, TopicManager};
pub use topic_config::{
    CleanupPolicy, CompressionType, ConfigValue, TopicConfig, TopicConfigManager,
    DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_RETENTION_BYTES, DEFAULT_RETENTION_MS,
    DEFAULT_SEGMENT_BYTES, DEFAULT_SEGMENT_MS,
};
pub use transaction::{
    AbortedTransaction, AbortedTransactionIndex, IsolationLevel, PendingWrite, Transaction,
    TransactionCoordinator, TransactionId, TransactionMarker, TransactionOffsetCommit,
    TransactionPartition, TransactionResult, TransactionState, TransactionStats,
    TransactionStatsSnapshot, DEFAULT_TRANSACTION_TIMEOUT, MAX_PENDING_TRANSACTIONS,
};
pub use vectorized::{
    BatchDecoder, BatchEncoder, BatchMessage, BatchProcessor, RecordBatch, RecordBatchIter,
};
pub use wal::{
    GroupCommitWal, RecordType, SyncMode, WalConfig, WalReader, WalRecord, WalStatsSnapshot,
};
pub use zero_copy::{
    BufferRef, BufferSlice, ConsumedMessage, SmallVec, ZeroCopyBuffer, ZeroCopyBufferPool,
    ZeroCopyConsumer, ZeroCopyProducer,
};

pub use async_io::{AsyncFile, AsyncIo, AsyncIoConfig, AsyncSegment, BatchBuilder};
pub use auth::{
    AclEntry, AuthConfig, AuthError, AuthManager, AuthResult, AuthSession, PasswordHash,
    Permission, Principal, PrincipalType, ResourceType, Role, SaslPlainAuth, SaslScramAuth,
    ScramState,
};
pub use io_uring::{
    is_io_uring_available, AsyncReader, AsyncWriter, BatchExecutor, BatchReadResult, BatchStats,
    IoBatch, IoOperation, IoUringConfig, IoUringStats, IoUringStatsSnapshot, SegmentReader,
    WalWriter,
};
pub use service_auth::{
    ApiKey, AuthMethod, ServiceAccount, ServiceAuthConfig, ServiceAuthError, ServiceAuthManager,
    ServiceAuthRequest, ServiceAuthResponse, ServiceAuthResult, ServiceSession,
};
pub use validation::{ValidationError, Validator};

// TLS re-exports
#[cfg(feature = "tls")]
pub use tls::{
    certificate_fingerprint, generate_self_signed, load_certificates, load_private_key,
    CertificateSource, CertificateWatcher, MtlsMode, PrivateKeySource, TlsAcceptor,
    TlsClientStream, TlsConfig, TlsConfigBuilder, TlsConnector, TlsError, TlsIdentity, TlsResult,
    TlsSecurityAudit, TlsServerStream, TlsVersion,
};

//! Pinecone gRPC data-plane client using `tonic` (rustls).
//!
//! This replaces the `pinecone-sdk` crate which hardcodes `reqwest` with
//! `native-tls` (OpenSSL), making musl and cross-compilation builds fail.
//!
//! Uses gRPC (HTTP/2 + protobuf binary encoding) for maximum throughput —
//! significantly faster than REST/JSON for high-volume vector operations
//! (smaller payloads, lower serialization overhead, connection multiplexing).
//!
//! Only the two data-plane gRPC operations used by the Pinecone sink are
//! implemented:
//!
//! - `VectorService/Upsert` — batch vector upsert
//! - `VectorService/DescribeIndexStats` — index health/dimension check
//!
//! Authentication is via the `api-key` gRPC metadata header.
//!
//! # HTTP/2 Connection Tuning
//!
//! The channel is configured for high-throughput vector workloads:
//!
//! - **Keep-alive**: HTTP/2 PING frames every 30 s prevent silent connection
//!   drops behind cloud load balancers (ALB, NLB, GCP LB) that kill idle
//!   connections after 60–350 s.
//! - **Flow control**: Enlarged initial window sizes (2 MiB connection,
//!   1 MiB per-stream) with adaptive resizing avoid sender stalls on
//!   large batches (~1.5 MiB for 1000 × 384-dim vectors).
//! - **TCP_NODELAY**: Disables Nagle's algorithm for sub-millisecond
//!   frame dispatch.
//!
//! # API Key Lifecycle
//!
//! The API key is stored as a `tonic::metadata::AsciiMetadataValue`
//! inside the gRPC interceptor.  Unlike `SensitiveString` (zeroize-on-drop),
//! `AsciiMetadataValue` does **not** zeroize memory on drop.  The connector
//! minimizes exposure by constructing the client in `write()` scope and
//! dropping it on completion, but the key may persist in freed heap pages
//! until overwritten.

use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status};

// ─────────────────────────────────────────────────────────────────
// Error types
// ─────────────────────────────────────────────────────────────────

/// Errors returned by the Pinecone gRPC client.
#[derive(Debug, thiserror::Error)]
pub enum PineconeError {
    /// gRPC transport / connection error.
    #[error("Connection error: {0}")]
    Connection(String),

    /// Server returned a non-OK gRPC status.
    #[error("API error (gRPC {code}): {message}")]
    Api { code: String, message: String },

    /// Request timed out (gRPC `DeadlineExceeded`).
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Protobuf serialization / deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Convert a `tonic::Status` into a `PineconeError`.
impl From<Status> for PineconeError {
    fn from(status: Status) -> Self {
        let code = status.code();
        let message = status.message().to_string();
        let code_str = format!("{:?}", code);

        match code {
            tonic::Code::DeadlineExceeded => Self::Timeout(message),
            // Transient / connection-level codes — all retryable
            tonic::Code::Unavailable | tonic::Code::Aborted | tonic::Code::Cancelled => {
                Self::Connection(format!("{}: {}", code_str, message))
            }
            _ => Self::Api {
                code: code_str,
                message,
            },
        }
    }
}

/// Convert a `tonic::transport::Error` into a `PineconeError`.
impl From<tonic::transport::Error> for PineconeError {
    fn from(e: tonic::transport::Error) -> Self {
        PineconeError::Connection(e.to_string())
    }
}

// ─────────────────────────────────────────────────────────────────
// Protobuf message types — mirrors of Pinecone VectorService proto
// ─────────────────────────────────────────────────────────────────
//
// These match the official `pinecone-io/pinecone-rust-client` proto
// definitions. Tag numbers must be exact for wire compatibility.

/// Sparse vector representation (indices + values).
#[derive(Clone, PartialEq, prost::Message)]
pub struct SparseValues {
    /// Dimension indices.
    #[prost(uint32, repeated, tag = "1")]
    pub indices: Vec<u32>,
    /// Corresponding float values (same length as `indices`).
    #[prost(float, repeated, tag = "2")]
    pub values: Vec<f32>,
}

/// A dense (and optionally sparse) vector for upsert.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Vector {
    /// Unique vector ID (max 512 bytes).
    #[prost(string, tag = "1")]
    pub id: String,
    /// Dense embedding values.
    #[prost(float, repeated, tag = "2")]
    pub values: Vec<f32>,
    /// Optional metadata key-value pairs (protobuf Struct).
    #[prost(message, optional, tag = "3")]
    pub metadata: Option<prost_types::Struct>,
    /// Optional sparse vector for hybrid search.
    #[prost(message, optional, tag = "5")]
    pub sparse_values: Option<SparseValues>,
}

/// Upsert request message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct UpsertRequest {
    /// An array containing the vectors to upsert (recommended batch ≤ 1000).
    #[prost(message, repeated, tag = "1")]
    pub vectors: Vec<Vector>,
    /// The namespace where you upsert vectors.
    #[prost(string, tag = "2")]
    pub namespace: String,
}

/// Upsert response message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct UpsertResponse {
    /// Number of vectors upserted.
    #[prost(uint32, tag = "1")]
    pub upserted_count: u32,
}

/// Describe-index-stats request message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct DescribeIndexStatsRequest {
    /// Optional metadata filter.
    #[prost(message, optional, tag = "1")]
    pub filter: Option<prost_types::Struct>,
}

/// Per-namespace statistics.
#[derive(Clone, PartialEq, prost::Message)]
pub struct NamespaceSummary {
    #[prost(uint32, tag = "1")]
    pub vector_count: u32,
}

/// Describe-index-stats response message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct DescribeIndexStatsResponse {
    /// Per-namespace vector counts.
    #[prost(map = "string, message", tag = "1")]
    pub namespaces: std::collections::HashMap<String, NamespaceSummary>,
    /// Vector dimensionality of the index.
    #[prost(uint32, tag = "2")]
    pub dimension: u32,
    /// Index fullness (pod-based indexes only).
    #[prost(float, tag = "3")]
    pub index_fullness: f32,
    /// Total vectors in the index.
    #[prost(uint32, tag = "4")]
    pub total_vector_count: u32,
}

// ─────────────────────────────────────────────────────────────────
// Metadata helpers — JSON → protobuf Struct conversion
// ─────────────────────────────────────────────────────────────────

/// Convert a `serde_json::Value` to a `prost_types::Value`.
///
/// Primitive types (null, bool, number, string) are converted natively.
/// Complex types (arrays, objects) are serialized to JSON strings — Pinecone
/// metadata only supports flat primitives and lists of strings.
#[inline]
pub fn json_to_prost_value(v: &serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match v {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(*b),
        serde_json::Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Kind::StringValue(s.clone()),
        // Complex types: serialize to JSON string (Pinecone metadata = flat primitives)
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Kind::StringValue(v.to_string())
        }
    };
    prost_types::Value { kind: Some(kind) }
}

/// Create a protobuf `Value` wrapping a string.
///
/// Convenience helper for inserting string metadata entries without verbose
/// `prost_types::value::Kind::StringValue` construction.
#[inline]
pub fn string_value(s: impl Into<String>) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StringValue(s.into())),
    }
}

/// Convert a JSON object map to a protobuf `Struct`.
#[inline]
pub fn json_map_to_struct(map: &serde_json::Map<String, serde_json::Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: map
            .iter()
            .map(|(k, v)| (k.clone(), json_to_prost_value(v)))
            .collect(),
    }
}

// ─────────────────────────────────────────────────────────────────
// Namespace wrapper
// ─────────────────────────────────────────────────────────────────

/// A Pinecone namespace string.  Use `Namespace::default()` for the
/// default (empty) namespace.
#[derive(Debug, Clone, Default)]
pub struct Namespace(String);

impl Namespace {
    pub fn new(ns: impl Into<String>) -> Self {
        Self(ns.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Namespace {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            f.write_str("<default>")
        } else {
            f.write_str(&self.0)
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Client
// ─────────────────────────────────────────────────────────────────

/// Configuration for creating a [`PineconeClient`].
#[derive(Debug, Clone)]
pub struct PineconeClientConfig {
    /// Pinecone API key.
    pub api_key: String,
}

/// gRPC interceptor that injects the `api-key` metadata header.
#[derive(Clone)]
struct ApiKeyInterceptor {
    api_key: AsciiMetadataValue,
}

impl Interceptor for ApiKeyInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("api-key", self.api_key.clone());
        Ok(request)
    }
}

/// Inner gRPC client type (Channel + API key interceptor).
type InnerGrpc = tonic::client::Grpc<
    tonic::service::interceptor::InterceptedService<Channel, ApiKeyInterceptor>,
>;

/// A lightweight Pinecone data-plane gRPC client.
///
/// Uses `tonic` with rustls — no OpenSSL dependency.  gRPC (HTTP/2 +
/// protobuf binary encoding) provides significantly better throughput
/// than REST/JSON: smaller payloads, lower CPU overhead, connection
/// multiplexing.
///
/// The connection is established lazily on the first RPC call, so
/// construction is synchronous and infallible (aside from config validation).
#[derive(Clone)]
pub struct PineconeClient {
    inner: InnerGrpc,
}

impl std::fmt::Debug for PineconeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PineconeClient")
            .field("transport", &"gRPC/tonic")
            .finish()
    }
}

impl PineconeClient {
    /// Create a new client for the given index host.
    ///
    /// The `host` should be the Pinecone index data-plane endpoint, e.g.
    /// `myindex-abc1234.svc.us-east-1-aws.pinecone.io` or a full
    /// `https://...` URL.  The connection is established lazily on the
    /// first RPC call.
    pub fn new(config: &PineconeClientConfig, host: &str) -> Result<Self, PineconeError> {
        // Normalize host URL: ensure https:// prefix, strip trailing slashes
        let trimmed = host.trim_end_matches('/');
        let host_url = if trimmed.starts_with("https://") || trimmed.starts_with("http://") {
            trimmed.to_string()
        } else {
            format!("https://{}", trimmed)
        };

        let tls = ClientTlsConfig::new();

        let user_agent = concat!("rivven-connect/", env!("CARGO_PKG_VERSION"));

        let endpoint = Channel::from_shared(host_url)
            .map_err(|e| PineconeError::Connection(format!("Invalid endpoint: {}", e)))?
            .user_agent(user_agent)
            .map_err(|e| PineconeError::Connection(format!("Invalid user-agent: {}", e)))?
            .tls_config(tls)
            .map_err(|e| PineconeError::Connection(format!("TLS config error: {}", e)))?
            // ── Connection establishment ──────────────────────────
            .connect_timeout(Duration::from_secs(10))
            // ── TCP tuning ──────────────────────────────────────────
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            // ── HTTP/2 keep-alive ───────────────────────────────────
            // PING frames every 30 s prevent silent connection drops
            // behind cloud load balancers (ALB/NLB/GCP LB kill idle
            // connections after 60–350 s).
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true)
            // ── HTTP/2 flow control ─────────────────────────────────
            // Default 65 KiB is too small for 1000-vector batches
            // (~1.5 MiB).  Larger windows + adaptive resizing avoid
            // sender stalls waiting for WINDOW_UPDATE frames.
            .http2_adaptive_window(true)
            .initial_connection_window_size(2 * 1024 * 1024) // 2 MiB
            .initial_stream_window_size(1024 * 1024); // 1 MiB

        // connect_lazy() returns immediately — TCP/TLS handshake happens on
        // the first RPC call.  This keeps the constructor synchronous.
        let channel = endpoint.connect_lazy();

        let api_key: AsciiMetadataValue = config.api_key.parse().map_err(|_| {
            PineconeError::Connection("Invalid API key: contains non-ASCII characters".to_string())
        })?;

        let intercepted = tonic::service::interceptor::InterceptedService::new(
            channel,
            ApiKeyInterceptor { api_key },
        );

        Ok(Self {
            inner: tonic::client::Grpc::new(intercepted),
        })
    }

    /// Upsert a batch of vectors into the index.
    ///
    /// Returns the number of vectors upserted.
    pub async fn upsert(
        &self,
        vectors: &[Vector],
        namespace: &Namespace,
    ) -> Result<UpsertResponse, PineconeError> {
        let request = UpsertRequest {
            vectors: vectors.to_vec(),
            namespace: namespace.as_str().to_string(),
        };

        let mut client = self.inner.clone();
        client
            .ready()
            .await
            .map_err(|e| PineconeError::Connection(format!("Channel not ready: {}", e)))?;

        let codec = tonic::codec::ProstCodec::default();
        let path = tonic::codegen::http::uri::PathAndQuery::from_static("/VectorService/Upsert");

        let response = client
            .unary(tonic::Request::new(request), path, codec)
            .await?;
        Ok(response.into_inner())
    }

    /// Retrieve index statistics (dimension, vector count, etc.).
    pub async fn describe_index_stats(&self) -> Result<DescribeIndexStatsResponse, PineconeError> {
        let request = DescribeIndexStatsRequest { filter: None };

        let mut client = self.inner.clone();
        client
            .ready()
            .await
            .map_err(|e| PineconeError::Connection(format!("Channel not ready: {}", e)))?;

        let codec = tonic::codec::ProstCodec::default();
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/VectorService/DescribeIndexStats",
        );

        let response = client
            .unary(tonic::Request::new(request), path, codec)
            .await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::collections::BTreeMap;

    // ── Protobuf round-trip tests ──────────────────────────────

    #[test]
    fn test_vector_protobuf_roundtrip() {
        let v = Vector {
            id: "vec-1".to_string(),
            values: vec![0.1, 0.2, 0.3],
            sparse_values: None,
            metadata: None,
        };
        let bytes = v.encode_to_vec();
        let decoded = Vector::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.id, "vec-1");
        assert_eq!(decoded.values.len(), 3);
        assert!(decoded.sparse_values.is_none());
        assert!(decoded.metadata.is_none());
    }

    #[test]
    fn test_vector_with_sparse_protobuf_roundtrip() {
        let v = Vector {
            id: "vec-2".to_string(),
            values: vec![0.1, 0.2],
            sparse_values: Some(SparseValues {
                indices: vec![1, 5],
                values: vec![0.5, 0.5],
            }),
            metadata: Some(prost_types::Struct {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("genre".to_string(), string_value("drama"));
                    m
                },
            }),
        };
        let bytes = v.encode_to_vec();
        let decoded = Vector::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.id, "vec-2");
        let sv = decoded.sparse_values.unwrap();
        assert_eq!(sv.indices, vec![1, 5]);
        assert_eq!(sv.values, vec![0.5, 0.5]);
        let meta = decoded.metadata.unwrap();
        assert_eq!(
            meta.fields["genre"].kind,
            Some(prost_types::value::Kind::StringValue("drama".to_string()))
        );
    }

    #[test]
    fn test_upsert_request_protobuf_roundtrip() {
        let req = UpsertRequest {
            vectors: vec![Vector {
                id: "v1".to_string(),
                values: vec![1.0, 2.0],
                sparse_values: None,
                metadata: None,
            }],
            namespace: "my-ns".to_string(),
        };
        let bytes = req.encode_to_vec();
        let decoded = UpsertRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.namespace, "my-ns");
        assert_eq!(decoded.vectors[0].id, "v1");
    }

    #[test]
    fn test_upsert_request_empty_namespace() {
        let req = UpsertRequest {
            vectors: vec![],
            namespace: String::new(),
        };
        let bytes = req.encode_to_vec();
        let decoded = UpsertRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.namespace, "");
    }

    #[test]
    fn test_upsert_response_protobuf_roundtrip() {
        let resp = UpsertResponse { upserted_count: 42 };
        let bytes = resp.encode_to_vec();
        let decoded = UpsertResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.upserted_count, 42);
    }

    #[test]
    fn test_describe_index_stats_response_roundtrip() {
        let resp = DescribeIndexStatsResponse {
            namespaces: {
                let mut m = std::collections::HashMap::new();
                m.insert("ns1".to_string(), NamespaceSummary { vector_count: 100 });
                m.insert("ns2".to_string(), NamespaceSummary { vector_count: 200 });
                m
            },
            dimension: 384,
            index_fullness: 0.1,
            total_vector_count: 300,
        };
        let bytes = resp.encode_to_vec();
        let decoded = DescribeIndexStatsResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.dimension, 384);
        assert_eq!(decoded.total_vector_count, 300);
        assert_eq!(decoded.namespaces.len(), 2);
        assert_eq!(decoded.namespaces["ns1"].vector_count, 100);
    }

    #[test]
    fn test_describe_index_stats_empty_response() {
        let resp = DescribeIndexStatsResponse::default();
        let bytes = resp.encode_to_vec();
        let decoded = DescribeIndexStatsResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.dimension, 0);
        assert_eq!(decoded.total_vector_count, 0);
        assert!(decoded.namespaces.is_empty());
        assert_eq!(decoded.index_fullness, 0.0);
    }

    // ── Namespace tests ────────────────────────────────────────

    #[test]
    fn test_namespace_default() {
        let ns = Namespace::default();
        assert_eq!(ns.as_str(), "");
    }

    #[test]
    fn test_namespace_from_str() {
        let ns = Namespace::from("my-namespace");
        assert_eq!(ns.as_str(), "my-namespace");
    }

    #[test]
    fn test_namespace_display() {
        assert_eq!(format!("{}", Namespace::default()), "<default>");
        assert_eq!(format!("{}", Namespace::new("prod")), "prod");
    }

    // ── Metadata conversion tests ──────────────────────────────

    #[test]
    fn test_json_to_prost_value_primitives() {
        let v = json_to_prost_value(&serde_json::Value::Null);
        assert_eq!(v.kind, Some(prost_types::value::Kind::NullValue(0)));

        let v = json_to_prost_value(&serde_json::json!(true));
        assert_eq!(v.kind, Some(prost_types::value::Kind::BoolValue(true)));

        let v = json_to_prost_value(&serde_json::json!(42));
        assert_eq!(v.kind, Some(prost_types::value::Kind::NumberValue(42.0)));

        let v = json_to_prost_value(&serde_json::json!("hello"));
        assert_eq!(
            v.kind,
            Some(prost_types::value::Kind::StringValue("hello".to_string()))
        );
    }

    #[test]
    fn test_json_to_prost_value_complex() {
        let arr = serde_json::json!([1, 2, 3]);
        let result = json_to_prost_value(&arr);
        assert_eq!(
            result.kind,
            Some(prost_types::value::Kind::StringValue("[1,2,3]".to_string()))
        );

        let obj = serde_json::json!({"key": "val"});
        let result = json_to_prost_value(&obj);
        assert_eq!(
            result.kind,
            Some(prost_types::value::Kind::StringValue(
                "{\"key\":\"val\"}".to_string()
            ))
        );
    }

    #[test]
    fn test_string_value_helper() {
        let v = string_value("test");
        assert_eq!(
            v.kind,
            Some(prost_types::value::Kind::StringValue("test".to_string()))
        );
    }

    #[test]
    fn test_json_map_to_struct() {
        let map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"name": "test", "count": 42, "active": true}"#).unwrap();
        let s = json_map_to_struct(&map);
        assert_eq!(s.fields.len(), 3);
        assert_eq!(
            s.fields["name"].kind,
            Some(prost_types::value::Kind::StringValue("test".to_string()))
        );
        assert_eq!(
            s.fields["count"].kind,
            Some(prost_types::value::Kind::NumberValue(42.0))
        );
    }

    // ── Error tests ────────────────────────────────────────────

    #[test]
    fn test_pinecone_error_display() {
        let err = PineconeError::Api {
            code: "Unauthenticated".to_string(),
            message: "Invalid API key".to_string(),
        };
        assert!(err.to_string().contains("Unauthenticated"));
        assert!(err.to_string().contains("Invalid API key"));
    }

    #[test]
    fn test_from_grpc_status_unauthenticated() {
        let status = Status::unauthenticated("bad key");
        let err = PineconeError::from(status);
        match err {
            PineconeError::Api { code, message } => {
                assert_eq!(code, "Unauthenticated");
                assert_eq!(message, "bad key");
            }
            _ => panic!("Expected Api variant"),
        }
    }

    #[test]
    fn test_from_grpc_status_deadline_exceeded() {
        let status = Status::deadline_exceeded("30s elapsed");
        let err = PineconeError::from(status);
        assert!(matches!(err, PineconeError::Timeout(_)));
    }

    #[test]
    fn test_from_grpc_status_unavailable() {
        let status = Status::unavailable("server down");
        let err = PineconeError::from(status);
        assert!(matches!(err, PineconeError::Connection(_)));
    }

    #[test]
    fn test_from_grpc_status_resource_exhausted() {
        let status = Status::resource_exhausted("rate limit");
        let err = PineconeError::from(status);
        match err {
            PineconeError::Api { code, .. } => assert_eq!(code, "ResourceExhausted"),
            _ => panic!("Expected Api variant"),
        }
    }

    #[test]
    fn test_error_variant_display() {
        let conn = PineconeError::Connection("refused".to_string());
        assert!(conn.to_string().contains("refused"));

        let timeout = PineconeError::Timeout("30s".to_string());
        assert!(timeout.to_string().contains("30s"));

        let ser = PineconeError::Serialization("bad proto".to_string());
        assert!(ser.to_string().contains("bad proto"));
    }

    // ── Client construction tests ──────────────────────────────

    #[tokio::test]
    async fn test_client_host_normalization() {
        let config = PineconeClientConfig {
            api_key: "test-key".to_string(),
        };

        // With https:// prefix — should succeed (connect_lazy doesn't connect)
        let c1 = PineconeClient::new(&config, "https://idx.svc.pinecone.io/");
        assert!(c1.is_ok());

        // Without prefix — should add https://
        let c2 = PineconeClient::new(&config, "idx.svc.pinecone.io");
        assert!(c2.is_ok());

        // Multiple trailing slashes
        let c3 = PineconeClient::new(&config, "https://idx.svc.pinecone.io///");
        assert!(c3.is_ok());
    }

    /// HTTP scheme is accepted for local development / mock servers.
    #[tokio::test]
    async fn test_client_http_scheme_accepted() {
        let config = PineconeClientConfig {
            api_key: "test-key".to_string(),
        };
        let c = PineconeClient::new(&config, "http://localhost:5081");
        assert!(c.is_ok(), "http:// should be accepted for local dev");
    }

    /// Control characters in API key must be rejected (gRPC metadata header
    /// only allows visible ASCII 0x20\u20130xFF, excluding NUL/CR/LF/DEL).
    #[tokio::test]
    async fn test_invalid_api_key_rejected() {
        for bad_key in [
            "key\nnewline",  // LF (0x0A) — rejected by http::HeaderValue
            "key\rcarriage", // CR (0x0D)
            "key\0null",     // NUL (0x00)
        ] {
            let config = PineconeClientConfig {
                api_key: bad_key.to_string(),
            };
            let result = PineconeClient::new(&config, "https://test.pinecone.io");
            assert!(
                result.is_err(),
                "API key with control char should be rejected: {:?}",
                bad_key
            );
        }
    }

    #[test]
    fn test_from_grpc_status_cancelled() {
        let status = Status::cancelled("client cancelled");
        let err = PineconeError::from(status);
        assert!(
            matches!(err, PineconeError::Connection(_)),
            "Cancelled should map to Connection (transient), got: {:?}",
            err
        );
    }

    /// PineconeClient must be Send + Sync for use across async tasks.
    #[test]
    fn test_pinecone_client_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PineconeClient>();
    }

    /// PineconeClient Debug output should not leak API key.
    #[tokio::test]
    async fn test_pinecone_client_debug_no_key_leak() {
        let config = PineconeClientConfig {
            api_key: "sk-SECRET-KEY-12345".to_string(),
        };
        let client = PineconeClient::new(&config, "https://test.pinecone.io").unwrap();
        let debug = format!("{:?}", client);
        assert!(
            !debug.contains("SECRET"),
            "Debug output must not leak API key: {}",
            debug
        );
        assert!(debug.contains("gRPC"));
    }

    #[test]
    fn test_upsert_request_with_metadata() {
        let mut fields = BTreeMap::new();
        fields.insert("genre".to_string(), string_value("action"));
        fields.insert(
            "year".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(2024.0)),
            },
        );

        let v = Vector {
            id: "v1".to_string(),
            values: vec![1.0, 2.0],
            sparse_values: None,
            metadata: Some(prost_types::Struct { fields }),
        };

        let bytes = v.encode_to_vec();
        let decoded = Vector::decode(bytes.as_slice()).unwrap();
        let meta = decoded.metadata.unwrap();
        assert_eq!(
            meta.fields["genre"].kind,
            Some(prost_types::value::Kind::StringValue("action".to_string()))
        );
        assert_eq!(
            meta.fields["year"].kind,
            Some(prost_types::value::Kind::NumberValue(2024.0))
        );
    }
}

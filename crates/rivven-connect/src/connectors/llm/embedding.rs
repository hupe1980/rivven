//! LLM Embedding Transform — generate vector embeddings for event fields
//!
//! Sends text from event fields through an embedding model and adds
//! the resulting vector to the event for downstream vector DB sinks.
//!
//! # Pipeline Example
//!
//! ```text
//! Source → LLM Embedding Transform → Qdrant/Pinecone Sink
//!
//! Input:  {"text": "The product is excellent", "id": 123}
//! Output: {"text": "...", "id": 123, "embedding": [0.01, -0.03, ...]}
//! ```
//!
//! # Configuration
//!
//! ```yaml
//! transforms:
//!   vectorize:
//!     transform: llm-embedding
//!     config:
//!       provider: openai
//!       api_key: "${OPENAI_API_KEY}"
//!       model: text-embedding-3-small
//!       input_field: text
//!       output_field: embedding
//! ```

use crate::error::{ConnectorError, Result};
use crate::traits::event::SourceEvent;
use crate::traits::spec::ConnectorSpec;
use crate::traits::transform::{Transform, TransformOutput};
use crate::types::SensitiveString;
use async_trait::async_trait;
use metrics::{counter, histogram};
use rivven_llm::{EmbeddingRequest, LlmProvider};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::{debug, warn};
use validator::Validate;

use super::{resolve_field, LlmProviderType};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the LLM embedding transform
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct LlmEmbeddingTransformConfig {
    /// LLM provider (openai or bedrock)
    #[serde(default)]
    pub provider: LlmProviderType,

    /// API key (required for OpenAI; Bedrock uses AWS credential chain)
    #[serde(default)]
    pub api_key: Option<SensitiveString>,

    /// Base URL override (for Azure OpenAI, Ollama, vLLM, etc.)
    #[serde(default)]
    pub base_url: Option<String>,

    /// AWS region (Bedrock only, default: us-east-1)
    #[serde(default)]
    pub region: Option<String>,

    /// Embedding model identifier
    #[validate(length(min = 1, max = 256))]
    pub model: String,

    /// Input field containing text to embed
    ///
    /// Supports nested fields with dot notation: `content.body`
    #[serde(default = "default_input_field")]
    #[validate(length(min = 1, max = 256))]
    pub input_field: String,

    /// Output field where the embedding vector is written
    #[serde(default = "default_embedding_output_field")]
    #[validate(length(min = 1, max = 256))]
    pub output_field: String,

    /// Expected embedding dimensions (for validation; 0 = skip check)
    ///
    /// When set to a non-zero value, this is also passed to the embedding
    /// API as a hint — models that support dimension control (OpenAI, Titan)
    /// will produce output at the requested size.
    #[serde(default)]
    pub dimensions: usize,

    /// Input type hint for the embedding model (e.g. `search_document`, `search_query`)
    ///
    /// Used by Cohere Bedrock models to optimize retrieval quality.
    /// Ignored by providers that don't support it.
    #[serde(default)]
    pub input_type: Option<String>,

    /// Request timeout in seconds
    #[serde(default = "default_timeout_secs")]
    #[validate(range(min = 1, max = 600))]
    pub timeout_secs: u64,

    /// Skip events where the input field is missing (instead of erroring)
    #[serde(default)]
    pub skip_on_missing: bool,

    /// Skip events where the embedding call fails (instead of erroring)
    #[serde(default)]
    pub skip_on_error: bool,
}

fn default_input_field() -> String {
    "text".to_string()
}

fn default_embedding_output_field() -> String {
    "embedding".to_string()
}

fn default_timeout_secs() -> u64 {
    60
}

impl LlmEmbeddingTransformConfig {
    /// Validate provider-specific requirements
    pub fn validate_provider(&self) -> std::result::Result<(), String> {
        match self.provider {
            LlmProviderType::OpenAi => {
                if self.api_key.is_none() {
                    return Err("api_key is required for OpenAI provider".to_string());
                }
                if let Some(ref key) = self.api_key {
                    if key.expose_secret().is_empty() {
                        return Err("api_key must not be empty".to_string());
                    }
                }
            }
            LlmProviderType::Bedrock => {
                // Bedrock uses AWS credential chain — no API key needed
            }
        }
        Ok(())
    }
}

// ============================================================================
// Transform implementation
// ============================================================================

/// LLM Embedding Transform
///
/// Generates vector embeddings for event text fields using an LLM provider.
pub struct LlmEmbeddingTransform {
    provider: OnceCell<Arc<dyn LlmProvider>>,
}

impl LlmEmbeddingTransform {
    pub fn new() -> Self {
        Self {
            provider: OnceCell::new(),
        }
    }

    /// Create or get the LLM provider (lazy initialization)
    async fn get_provider(
        &self,
        config: &LlmEmbeddingTransformConfig,
    ) -> Result<&Arc<dyn LlmProvider>> {
        self.provider
            .get_or_try_init(|| async { create_provider(config).await })
            .await
    }
}

impl Default for LlmEmbeddingTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LlmEmbeddingTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmEmbeddingTransform")
            .field("initialized", &self.provider.initialized())
            .finish()
    }
}

/// Extract text from a JSON value
fn extract_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Array(arr) => {
            // Join array of strings
            let texts: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if texts.is_empty() {
                None
            } else {
                Some(texts.join(" "))
            }
        }
        serde_json::Value::Object(_) => Some(value.to_string()),
        serde_json::Value::Null => None,
    }
}

#[async_trait]
impl Transform for LlmEmbeddingTransform {
    type Config = LlmEmbeddingTransformConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("llm-embedding", env!("CARGO_PKG_VERSION"))
            .description("Generate vector embeddings for event fields (OpenAI, Bedrock)")
            .config_schema_from::<LlmEmbeddingTransformConfig>()
            .metadata("provider", "openai,bedrock")
            .metadata("type", "transform")
    }

    async fn init(&self, config: &Self::Config) -> Result<()> {
        config.validate_provider().map_err(ConnectorError::Config)?;
        // Eagerly initialize provider to fail fast
        let provider = self.get_provider(config).await?;

        // Verify provider supports embeddings
        if !provider.capabilities().embeddings {
            return Err(ConnectorError::Config(format!(
                "provider '{}' does not support embeddings",
                provider.name(),
            ))
            .into());
        }

        debug!(model = %config.model, provider = %config.provider, "LLM embedding transform initialized");
        Ok(())
    }

    async fn transform(
        &self,
        config: &Self::Config,
        event: SourceEvent,
    ) -> Result<TransformOutput> {
        let provider = self.get_provider(config).await?;

        // Extract text from input field
        let text = resolve_field(&event.data, &config.input_field).and_then(extract_text);

        let text = match text {
            Some(t) if !t.trim().is_empty() => t,
            _ => {
                if config.skip_on_missing {
                    warn!(
                        stream = %event.stream,
                        field = %config.input_field,
                        "input field missing or empty, passing through"
                    );
                    return Ok(TransformOutput::single(event));
                }
                return Err(ConnectorError::Config(format!(
                    "input field '{}' is missing or empty in event data",
                    config.input_field
                ))
                .into());
            }
        };

        // Build embedding request
        let mut req_builder = EmbeddingRequest::builder()
            .input(&text)
            .model(&config.model);

        if config.dimensions > 0 {
            req_builder = req_builder.dimensions(config.dimensions as u32);
        }

        if let Some(ref input_type) = config.input_type {
            req_builder = req_builder.input_type(input_type);
        }

        let request = req_builder.build();

        // Call LLM with timeout
        let timeout = Duration::from_secs(config.timeout_secs);
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(timeout, provider.embed(&request)).await;
        let elapsed_ms = start.elapsed().as_millis() as f64;

        histogram!("llm.embedding.duration_ms").record(elapsed_ms);

        let response = match result {
            Ok(Ok(resp)) => {
                counter!("llm.embedding.requests.success").increment(1);
                resp
            }
            Ok(Err(e)) => {
                counter!("llm.embedding.requests.failed").increment(1);
                if config.skip_on_error {
                    warn!(error = %e, stream = %event.stream, "embedding failed, passing through");
                    return Ok(TransformOutput::single(event));
                }
                if e.is_retryable() {
                    return Err(ConnectorError::Transient(format!(
                        "embedding error: {}",
                        e.truncated_message(512)
                    ))
                    .into());
                }
                return Err(ConnectorError::Fatal(format!(
                    "embedding error: {}",
                    e.truncated_message(512)
                ))
                .into());
            }
            Err(_timeout) => {
                counter!("llm.embedding.requests.timeout").increment(1);
                if config.skip_on_error {
                    warn!(
                        stream = %event.stream,
                        timeout_secs = config.timeout_secs,
                        "embedding timed out, passing through"
                    );
                    return Ok(TransformOutput::single(event));
                }
                return Err(ConnectorError::Timeout(format!(
                    "embedding timed out after {}s",
                    config.timeout_secs
                ))
                .into());
            }
        };

        // Record token usage
        counter!("llm.embedding.tokens.prompt").increment(response.usage.prompt_tokens as u64);
        counter!("llm.embedding.tokens.total").increment(response.usage.total_tokens as u64);

        // Extract the first embedding vector
        let vector = response
            .first_embedding()
            .ok_or_else(|| {
                ConnectorError::Fatal("no embedding returned from provider".to_string())
            })?
            .to_vec();

        // Validate dimensions if configured
        if config.dimensions > 0 && vector.len() != config.dimensions {
            return Err(ConnectorError::Config(format!(
                "expected {} dimensions, got {}",
                config.dimensions,
                vector.len()
            ))
            .into());
        }

        // Add embedding to event data
        let embedding_json: Vec<serde_json::Value> =
            vector.iter().map(|&v| serde_json::Value::from(v)).collect();

        let mut enriched = event;
        if let serde_json::Value::Object(ref mut map) = enriched.data {
            map.insert(
                config.output_field.clone(),
                serde_json::Value::Array(embedding_json),
            );
        } else {
            let mut map = serde_json::Map::new();
            map.insert("_original".to_string(), std::mem::take(&mut enriched.data));
            map.insert(
                config.output_field.clone(),
                serde_json::Value::Array(embedding_json),
            );
            enriched.data = serde_json::Value::Object(map);
        }

        Ok(TransformOutput::single(enriched))
    }
}

// Bridge macro for type-erased dispatch
crate::impl_any_transform!(LlmEmbeddingTransform, LlmEmbeddingTransformConfig);

/// Factory for the LLM embedding transform
pub struct LlmEmbeddingTransformFactory;

impl crate::TransformFactory for LlmEmbeddingTransformFactory {
    fn spec(&self) -> ConnectorSpec {
        LlmEmbeddingTransform::spec()
    }

    fn create(&self) -> Box<dyn crate::AnyTransform> {
        Box::new(LlmEmbeddingTransform::new())
    }
}

// ============================================================================
// Provider construction
// ============================================================================

async fn create_provider(config: &LlmEmbeddingTransformConfig) -> Result<Arc<dyn LlmProvider>> {
    match config.provider {
        LlmProviderType::OpenAi => {
            #[cfg(feature = "llm-openai")]
            {
                let api_key = config.api_key.as_ref().ok_or_else(|| {
                    ConnectorError::Config("api_key is required for OpenAI".to_string())
                })?;

                let mut builder = rivven_llm::openai::OpenAiProvider::builder()
                    .api_key(api_key.expose_secret())
                    .embedding_model(&config.model)
                    .timeout(Duration::from_secs(config.timeout_secs));

                if let Some(ref url) = config.base_url {
                    builder = builder.base_url(url);
                }

                let provider = builder
                    .build()
                    .map_err(|e| ConnectorError::Config(e.to_string()))?;

                Ok(Arc::new(provider) as Arc<dyn LlmProvider>)
            }
            #[cfg(not(feature = "llm-openai"))]
            {
                Err(ConnectorError::Config(
                    "OpenAI provider requires the 'llm-openai' feature".to_string(),
                )
                .into())
            }
        }
        LlmProviderType::Bedrock => {
            #[cfg(feature = "llm-bedrock")]
            {
                let mut builder = rivven_llm::bedrock::BedrockProvider::builder()
                    .embedding_model(&config.model)
                    .timeout(Duration::from_secs(config.timeout_secs));

                if let Some(ref region) = config.region {
                    builder = builder.region(region);
                }

                if let Some(ref url) = config.base_url {
                    builder = builder.endpoint_url(url);
                }

                let provider = builder
                    .build()
                    .await
                    .map_err(|e| ConnectorError::Config(e.to_string()))?;

                Ok(Arc::new(provider) as Arc<dyn LlmProvider>)
            }
            #[cfg(not(feature = "llm-bedrock"))]
            {
                Err(ConnectorError::Config(
                    "Bedrock provider requires the 'llm-bedrock' feature".to_string(),
                )
                .into())
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── extract_text ────────────────────────────────────────────────

    #[test]
    fn test_extract_text_string() {
        let val = json!("hello world");
        assert_eq!(extract_text(&val).unwrap(), "hello world");
    }

    #[test]
    fn test_extract_text_number() {
        let val = json!(42);
        assert_eq!(extract_text(&val).unwrap(), "42");
    }

    #[test]
    fn test_extract_text_bool() {
        let val = json!(true);
        assert_eq!(extract_text(&val).unwrap(), "true");
    }

    #[test]
    fn test_extract_text_array_of_strings() {
        let val = json!(["hello", "world"]);
        assert_eq!(extract_text(&val).unwrap(), "hello world");
    }

    #[test]
    fn test_extract_text_null() {
        let val = json!(null);
        assert!(extract_text(&val).is_none());
    }

    #[test]
    fn test_extract_text_object() {
        let val = json!({"key": "value"});
        let text = extract_text(&val).unwrap();
        assert!(text.contains("key"));
        assert!(text.contains("value"));
    }

    #[test]
    fn test_extract_text_empty_string() {
        let val = json!("");
        // Returns Some("") but callers should check emptiness
        assert_eq!(extract_text(&val).unwrap(), "");
    }

    #[test]
    fn test_extract_text_whitespace_only() {
        let val = json!("   ");
        let text = extract_text(&val).unwrap();
        // Caller uses trim().is_empty() to catch this
        assert!(text.trim().is_empty());
    }

    #[test]
    fn test_extract_text_array_non_strings() {
        let val = json!([1, 2, 3]);
        // Array of non-strings → filter_map returns empty
        assert!(extract_text(&val).is_none());
    }

    // ── Config ──────────────────────────────────────────────────────

    #[test]
    fn test_config_validate_openai_needs_api_key() {
        let config = LlmEmbeddingTransformConfig {
            provider: LlmProviderType::OpenAi,
            api_key: None,
            base_url: None,
            region: None,
            model: "text-embedding-3-small".to_string(),
            input_field: "text".to_string(),
            output_field: "embedding".to_string(),
            dimensions: 0,
            input_type: None,
            timeout_secs: 60,
            skip_on_missing: false,
            skip_on_error: false,
        };
        let err = config.validate_provider().unwrap_err();
        assert!(err.contains("api_key"));
    }

    #[test]
    fn test_config_validate_bedrock_ok() {
        let config = LlmEmbeddingTransformConfig {
            provider: LlmProviderType::Bedrock,
            api_key: None,
            base_url: None,
            region: Some("us-east-1".to_string()),
            model: "amazon.titan-embed-text-v2:0".to_string(),
            input_field: "text".to_string(),
            output_field: "embedding".to_string(),
            dimensions: 0,
            input_type: None,
            timeout_secs: 60,
            skip_on_missing: false,
            skip_on_error: false,
        };
        config.validate_provider().unwrap();
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let yaml = r#"
provider: openai
api_key: "sk-test"
model: "text-embedding-3-small"
input_field: "description"
output_field: "vector"
dimensions: 1536
timeout_secs: 30
"#;
        let config: LlmEmbeddingTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, LlmProviderType::OpenAi);
        assert_eq!(config.model, "text-embedding-3-small");
        assert_eq!(config.input_field, "description");
        assert_eq!(config.output_field, "vector");
        assert_eq!(config.dimensions, 1536);
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
model: "text-embedding-3-small"
"#;
        let config: LlmEmbeddingTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, LlmProviderType::OpenAi);
        assert_eq!(config.input_field, "text");
        assert_eq!(config.output_field, "embedding");
        assert_eq!(config.dimensions, 0);
        assert!(config.input_type.is_none());
        assert_eq!(config.timeout_secs, 60);
        assert!(!config.skip_on_missing);
        assert!(!config.skip_on_error);
    }

    #[test]
    fn test_config_input_type() {
        let yaml = r#"
model: "cohere.embed-english-v3"
input_type: "search_query"
"#;
        let config: LlmEmbeddingTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.input_type.as_deref(), Some("search_query"));
    }

    #[test]
    fn test_config_dimensions_pass_through() {
        let yaml = r#"
model: "text-embedding-3-small"
dimensions: 256
"#;
        let config: LlmEmbeddingTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.dimensions, 256);
    }

    #[test]
    fn test_config_skip_on_error_independent() {
        let yaml = r#"
model: "text-embedding-3-small"
skip_on_missing: true
skip_on_error: false
"#;
        let config: LlmEmbeddingTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.skip_on_missing);
        assert!(!config.skip_on_error);
    }

    #[test]
    fn test_config_skip_on_error_enabled() {
        let yaml = r#"
model: "text-embedding-3-small"
skip_on_error: true
"#;
        let config: LlmEmbeddingTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!config.skip_on_missing);
        assert!(config.skip_on_error);
    }

    // ── Spec ────────────────────────────────────────────────────────

    #[test]
    fn test_spec() {
        let spec = LlmEmbeddingTransform::spec();
        assert_eq!(spec.connector_type, "llm-embedding");
        assert!(spec.description.unwrap().contains("embedding"));
    }

    // ── Factory ─────────────────────────────────────────────────────

    #[test]
    fn test_factory() {
        let factory = LlmEmbeddingTransformFactory;
        let spec = <LlmEmbeddingTransformFactory as crate::TransformFactory>::spec(&factory);
        assert_eq!(spec.connector_type, "llm-embedding");
    }

    // ── Debug ───────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        let transform = LlmEmbeddingTransform::new();
        let debug = format!("{transform:?}");
        assert!(debug.contains("LlmEmbeddingTransform"));
    }
}

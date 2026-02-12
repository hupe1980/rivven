//! OpenAI provider — chat completions and embeddings via the OpenAI REST API
//!
//! Supports OpenAI, Azure OpenAI, and any OpenAI-compatible endpoint (Ollama,
//! vLLM, LocalAI, etc.) by configuring `base_url`.
//!
//! # Example
//!
//! ```rust,no_run
//! use rivven_llm::openai::OpenAiProvider;
//!
//! # async fn example() -> Result<(), rivven_llm::LlmError> {
//! let provider = OpenAiProvider::builder()
//!     .api_key("sk-...")
//!     .model("gpt-4o-mini")
//!     .build()?;
//! # Ok(())
//! # }
//! ```

use crate::error::{LlmError, LlmResult};
use crate::provider::{LlmProvider, ProviderCapabilities};
use crate::types::*;
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, warn};

/// Default OpenAI API base URL
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// Default chat model
const DEFAULT_CHAT_MODEL: &str = "gpt-4o-mini";

/// Default embedding model
const DEFAULT_EMBEDDING_MODEL: &str = "text-embedding-3-small";

/// Maximum error body bytes to read (prevent unbounded allocation)
const MAX_ERROR_BODY_BYTES: usize = 4096;

/// OpenAI provider configuration
#[derive(Debug, Clone)]
pub struct OpenAiConfig {
    /// API key
    pub api_key: SecretString,
    /// Base URL (default: `https://api.openai.com/v1`)
    pub base_url: String,
    /// Default chat model
    pub chat_model: String,
    /// Default embedding model
    pub embedding_model: String,
    /// Request timeout
    pub timeout: Duration,
    /// Organization ID (optional, for OpenAI multi-org accounts)
    pub organization: Option<String>,
}

/// Builder for `OpenAiProvider`
#[derive(Debug, Default)]
pub struct OpenAiProviderBuilder {
    api_key: Option<SecretString>,
    base_url: Option<String>,
    chat_model: Option<String>,
    embedding_model: Option<String>,
    timeout: Option<Duration>,
    organization: Option<String>,
}

impl OpenAiProviderBuilder {
    /// Set the API key (required)
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(SecretString::from(key.into()));
        self
    }

    /// Set a pre-built SecretString API key
    pub fn api_key_secret(mut self, key: SecretString) -> Self {
        self.api_key = Some(key);
        self
    }

    /// Override base URL (for Azure OpenAI, Ollama, vLLM, etc.)
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// Set default model (used for both chat and embedding if not set separately)
    pub fn model(mut self, model: impl Into<String>) -> Self {
        let m = model.into();
        self.chat_model = Some(m.clone());
        self.embedding_model = Some(m);
        self
    }

    /// Set default chat model
    pub fn chat_model(mut self, model: impl Into<String>) -> Self {
        self.chat_model = Some(model.into());
        self
    }

    /// Set default embedding model
    pub fn embedding_model(mut self, model: impl Into<String>) -> Self {
        self.embedding_model = Some(model.into());
        self
    }

    /// Set request timeout (default: 60s)
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set organization ID
    pub fn organization(mut self, org: impl Into<String>) -> Self {
        self.organization = Some(org.into());
        self
    }

    /// Build the provider
    pub fn build(self) -> LlmResult<OpenAiProvider> {
        let api_key = self
            .api_key
            .ok_or_else(|| LlmError::Config("api_key is required".to_string()))?;

        // Validate API key is not empty
        if api_key.expose_secret().is_empty() {
            return Err(LlmError::Config("api_key must not be empty".to_string()));
        }

        let base_url = self
            .base_url
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());

        // Validate base_url format
        if !base_url.starts_with("https://") && !base_url.starts_with("http://") {
            return Err(LlmError::Config(format!(
                "base_url must start with http:// or https://, got: {base_url}"
            )));
        }

        if base_url.starts_with("http://")
            && !base_url.contains("localhost")
            && !base_url.contains("127.0.0.1")
        {
            warn!("OpenAI base_url uses plain HTTP — API key will be sent in cleartext");
        }

        let timeout = self.timeout.unwrap_or(Duration::from_secs(60));

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| LlmError::Config(format!("failed to build HTTP client: {e}")))?;

        let config = OpenAiConfig {
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            chat_model: self
                .chat_model
                .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string()),
            embedding_model: self
                .embedding_model
                .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL.to_string()),
            timeout,
            organization: self.organization,
        };

        debug!(
            base_url = %config.base_url,
            chat_model = %config.chat_model,
            embedding_model = %config.embedding_model,
            "OpenAI provider initialized"
        );

        Ok(OpenAiProvider { config, client })
    }
}

/// OpenAI LLM provider
///
/// Supports OpenAI API, Azure OpenAI, and any compatible endpoint.
pub struct OpenAiProvider {
    config: OpenAiConfig,
    client: reqwest::Client,
}

impl std::fmt::Debug for OpenAiProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiProvider")
            .field("base_url", &self.config.base_url)
            .field("chat_model", &self.config.chat_model)
            .field("embedding_model", &self.config.embedding_model)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl OpenAiProvider {
    /// Create a builder
    pub fn builder() -> OpenAiProviderBuilder {
        OpenAiProviderBuilder::default()
    }

    /// Build the authorization header
    fn auth_headers(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let builder = builder.header(
            "Authorization",
            format!("Bearer {}", self.config.api_key.expose_secret()),
        );
        if let Some(ref org) = self.config.organization {
            builder.header("OpenAI-Organization", org)
        } else {
            builder
        }
    }

    /// Parse an error response from the API
    async fn parse_error_response(&self, response: reqwest::Response) -> LlmError {
        let status = response.status().as_u16();

        // Extract retry-after header before consuming the body
        let retry_after_secs = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        // Read body with bounded allocation
        let body = match response.bytes().await {
            Ok(b) => {
                if b.len() > MAX_ERROR_BODY_BYTES {
                    String::from_utf8_lossy(&b[..MAX_ERROR_BODY_BYTES]).to_string()
                } else {
                    String::from_utf8_lossy(&b).to_string()
                }
            }
            Err(_) => String::new(),
        };

        // Try to parse structured error
        let message = if let Ok(err) = serde_json::from_str::<OpenAiErrorResponse>(&body) {
            // Truncate parsed error message — attacker-controlled API could
            // send a small JSON envelope with a multi-megabyte message field.
            let msg = err.error.message;
            if msg.len() > MAX_ERROR_BODY_BYTES {
                msg[..MAX_ERROR_BODY_BYTES].to_string()
            } else {
                msg
            }
        } else {
            body
        };

        match status {
            401 => LlmError::Auth(message),
            403 => LlmError::Auth(format!("forbidden: {message}")),
            404 => LlmError::ModelNotFound(message),
            429 => {
                // Parse retry-after header if present
                LlmError::RateLimited {
                    message,
                    retry_after_secs,
                }
            }
            400 => {
                // Check for specific OpenAI error types
                let lower = message.to_lowercase();
                if lower.contains("context_length_exceeded") || lower.contains("maximum context") {
                    LlmError::TokenLimitExceeded(message)
                } else if lower.contains("content_filter") || lower.contains("content management") {
                    LlmError::ContentFiltered(message)
                } else {
                    LlmError::Provider { status, message }
                }
            }
            500..=599 => LlmError::Transient(format!("server error ({status}): {message}")),
            _ => LlmError::Provider { status, message },
        }
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    fn name(&self) -> &str {
        "openai"
    }

    fn default_model(&self) -> &str {
        &self.config.chat_model
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            chat: true,
            embeddings: true,
        }
    }

    async fn chat(&self, request: &ChatRequest) -> LlmResult<ChatResponse> {
        let model = request.model.as_deref().unwrap_or(&self.config.chat_model);

        let api_request = OpenAiChatRequest {
            model: model.to_string(),
            messages: request
                .messages
                .iter()
                .map(|m| OpenAiMessage {
                    role: m.role.to_string(),
                    content: Some(m.content.clone()),
                })
                .collect(),
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            top_p: request.top_p,
            stop: if request.stop.is_empty() {
                None
            } else {
                Some(request.stop.clone())
            },
        };

        let url = format!("{}/chat/completions", self.config.base_url);
        debug!(url = %url, model = %model, messages = request.messages.len(), "OpenAI chat request");

        let resp = self
            .auth_headers(self.client.post(&url))
            .json(&api_request)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(self.parse_error_response(resp).await);
        }

        let api_resp: OpenAiChatResponse = resp
            .json()
            .await
            .map_err(|e| LlmError::Serialization(format!("failed to parse chat response: {e}")))?;

        Ok(ChatResponse {
            id: api_resp.id,
            model: api_resp.model,
            choices: api_resp
                .choices
                .into_iter()
                .map(|c| ChatChoice {
                    index: c.index,
                    message: ChatMessage {
                        role: parse_role(&c.message.role),
                        content: c.message.content.unwrap_or_default(),
                    },
                    finish_reason: parse_finish_reason(c.finish_reason.as_deref()),
                })
                .collect(),
            usage: Usage {
                prompt_tokens: api_resp.usage.prompt_tokens,
                completion_tokens: api_resp.usage.completion_tokens,
                total_tokens: api_resp.usage.total_tokens,
            },
        })
    }

    async fn embed(&self, request: &EmbeddingRequest) -> LlmResult<EmbeddingResponse> {
        if request.input.is_empty() {
            return Err(LlmError::Config(
                "embedding input must not be empty".to_string(),
            ));
        }

        let model = request
            .model
            .as_deref()
            .unwrap_or(&self.config.embedding_model);

        let api_request = OpenAiEmbeddingRequest {
            model: model.to_string(),
            input: request.input.clone(),
            dimensions: request.dimensions,
            encoding_format: Some("float".to_string()),
        };

        let url = format!("{}/embeddings", self.config.base_url);
        debug!(url = %url, model = %model, inputs = request.input.len(), "OpenAI embedding request");

        let resp = self
            .auth_headers(self.client.post(&url))
            .json(&api_request)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(self.parse_error_response(resp).await);
        }

        let api_resp: OpenAiEmbeddingResponse = resp.json().await.map_err(|e| {
            LlmError::Serialization(format!("failed to parse embedding response: {e}"))
        })?;

        Ok(EmbeddingResponse {
            model: api_resp.model,
            embeddings: api_resp
                .data
                .into_iter()
                .map(|e| Embedding {
                    index: e.index,
                    values: e.embedding,
                })
                .collect(),
            usage: EmbeddingUsage {
                prompt_tokens: api_resp.usage.prompt_tokens,
                total_tokens: api_resp.usage.total_tokens,
            },
        })
    }
}

// ============================================================================
// OpenAI API wire types (private)
// ============================================================================

#[derive(Serialize)]
struct OpenAiChatRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct OpenAiMessage {
    role: String,
    #[serde(default)]
    content: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiChatResponse {
    id: String,
    model: String,
    choices: Vec<OpenAiChoice>,
    usage: OpenAiUsage,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    index: u32,
    message: OpenAiMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

#[derive(Serialize)]
struct OpenAiEmbeddingRequest {
    model: String,
    input: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding_format: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiEmbeddingResponse {
    model: String,
    data: Vec<OpenAiEmbeddingData>,
    usage: OpenAiEmbeddingUsage,
}

#[derive(Deserialize)]
struct OpenAiEmbeddingData {
    index: u32,
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct OpenAiEmbeddingUsage {
    prompt_tokens: u32,
    total_tokens: u32,
}

#[derive(Deserialize)]
struct OpenAiErrorResponse {
    error: OpenAiErrorDetail,
}

#[derive(Deserialize)]
struct OpenAiErrorDetail {
    message: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    error_type: Option<String>,
    #[allow(dead_code)]
    code: Option<String>,
}

// ============================================================================
// Helpers
// ============================================================================

fn parse_role(role: &str) -> Role {
    match role {
        "system" => Role::System,
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "tool" => Role::Tool,
        _ => Role::Assistant,
    }
}

fn parse_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("stop") => FinishReason::Stop,
        Some("length") => FinishReason::Length,
        Some("content_filter") => FinishReason::ContentFilter,
        Some("tool_calls") | Some("function_call") => FinishReason::ToolCalls,
        _ => FinishReason::Stop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_missing_api_key() {
        let err = OpenAiProvider::builder().build().unwrap_err();
        assert!(err.to_string().contains("api_key is required"));
    }

    #[test]
    fn test_builder_empty_api_key() {
        let err = OpenAiProvider::builder().api_key("").build().unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn test_builder_invalid_base_url() {
        let err = OpenAiProvider::builder()
            .api_key("sk-test")
            .base_url("ftp://invalid")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("must start with http"));
    }

    #[test]
    fn test_builder_defaults() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .build()
            .unwrap();
        assert_eq!(provider.config.base_url, DEFAULT_BASE_URL);
        assert_eq!(provider.config.chat_model, DEFAULT_CHAT_MODEL);
        assert_eq!(provider.config.embedding_model, DEFAULT_EMBEDDING_MODEL);
        assert_eq!(provider.config.timeout, Duration::from_secs(60));
        assert!(provider.config.organization.is_none());
    }

    #[test]
    fn test_builder_custom() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .base_url("https://custom.api.com/v1")
            .chat_model("gpt-4o")
            .embedding_model("text-embedding-3-large")
            .timeout(Duration::from_secs(120))
            .organization("org-123")
            .build()
            .unwrap();
        assert_eq!(provider.config.base_url, "https://custom.api.com/v1");
        assert_eq!(provider.config.chat_model, "gpt-4o");
        assert_eq!(provider.config.embedding_model, "text-embedding-3-large");
        assert_eq!(provider.config.timeout, Duration::from_secs(120));
        assert_eq!(provider.config.organization.as_deref(), Some("org-123"));
    }

    #[test]
    fn test_builder_trailing_slash_stripped() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .base_url("https://api.openai.com/v1/")
            .build()
            .unwrap();
        assert_eq!(provider.config.base_url, "https://api.openai.com/v1");
    }

    #[test]
    fn test_provider_capabilities() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .build()
            .unwrap();
        let caps = provider.capabilities();
        assert!(caps.chat);
        assert!(caps.embeddings);
    }

    #[test]
    fn test_provider_name() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .build()
            .unwrap();
        assert_eq!(provider.name(), "openai");
    }

    #[test]
    fn test_provider_default_model() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .chat_model("gpt-4o")
            .build()
            .unwrap();
        assert_eq!(provider.default_model(), "gpt-4o");
    }

    #[test]
    fn test_debug_redacts_api_key() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-secretkey123")
            .build()
            .unwrap();
        let debug = format!("{provider:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("sk-secretkey123"));
    }

    #[test]
    fn test_parse_role_all_variants() {
        assert_eq!(parse_role("system"), Role::System);
        assert_eq!(parse_role("user"), Role::User);
        assert_eq!(parse_role("assistant"), Role::Assistant);
        assert_eq!(parse_role("tool"), Role::Tool);
        assert_eq!(parse_role("unknown"), Role::Assistant);
    }

    #[test]
    fn test_parse_finish_reason_all_variants() {
        assert_eq!(parse_finish_reason(Some("stop")), FinishReason::Stop);
        assert_eq!(parse_finish_reason(Some("length")), FinishReason::Length);
        assert_eq!(
            parse_finish_reason(Some("content_filter")),
            FinishReason::ContentFilter
        );
        assert_eq!(
            parse_finish_reason(Some("tool_calls")),
            FinishReason::ToolCalls
        );
        assert_eq!(
            parse_finish_reason(Some("function_call")),
            FinishReason::ToolCalls
        );
        assert_eq!(parse_finish_reason(None), FinishReason::Stop);
        assert_eq!(parse_finish_reason(Some("other")), FinishReason::Stop);
    }

    #[test]
    fn test_openai_chat_request_serialization() {
        let req = OpenAiChatRequest {
            model: "gpt-4o".to_string(),
            messages: vec![OpenAiMessage {
                role: "user".to_string(),
                content: Some("Hello".to_string()),
            }],
            temperature: Some(0.7),
            max_tokens: None,
            top_p: None,
            stop: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["model"], "gpt-4o");
        assert_eq!(json["messages"][0]["role"], "user");
        // f32 temperature — check approximate equality due to f32→f64 precision
        let temp = json["temperature"].as_f64().unwrap();
        assert!((temp - 0.7).abs() < 1e-6);
        // max_tokens, top_p, stop should be absent (skip_serializing_if)
        assert!(json.get("max_tokens").is_none());
        assert!(json.get("top_p").is_none());
        assert!(json.get("stop").is_none());
    }

    #[test]
    fn test_openai_embedding_request_serialization() {
        let req = OpenAiEmbeddingRequest {
            model: "text-embedding-3-small".to_string(),
            input: vec!["hello".to_string(), "world".to_string()],
            dimensions: Some(256),
            encoding_format: Some("float".to_string()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["model"], "text-embedding-3-small");
        assert_eq!(json["input"].as_array().unwrap().len(), 2);
        assert_eq!(json["dimensions"], 256);
    }

    #[test]
    fn test_openai_error_response_deserialization() {
        let json = r#"{"error":{"message":"Invalid API key","type":"invalid_request_error","code":"invalid_api_key"}}"#;
        let err: OpenAiErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(err.error.message, "Invalid API key");
    }

    #[test]
    fn test_openai_chat_response_deserialization() {
        let json = r#"{
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": "gpt-4o-mini",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "Hello!"},
                    "finish_reason": "stop"
                }
            ],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6}
        }"#;
        let resp: OpenAiChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "chatcmpl-test");
        assert_eq!(resp.model, "gpt-4o-mini");
        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("Hello!"));
        assert_eq!(resp.usage.total_tokens, 6);
    }

    #[test]
    fn test_openai_embedding_response_deserialization() {
        let json = r#"{
            "object": "list",
            "model": "text-embedding-3-small",
            "data": [
                {"object": "embedding", "index": 0, "embedding": [0.1, 0.2, 0.3]}
            ],
            "usage": {"prompt_tokens": 2, "total_tokens": 2}
        }"#;
        let resp: OpenAiEmbeddingResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.model, "text-embedding-3-small");
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].embedding, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn test_model_shorthand_sets_both() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .model("gpt-4o")
            .build()
            .unwrap();
        // model() sets both chat_model and embedding_model
        assert_eq!(provider.config.chat_model, "gpt-4o");
        assert_eq!(provider.config.embedding_model, "gpt-4o");
    }

    #[test]
    fn test_separate_models_override_shorthand() {
        let provider = OpenAiProvider::builder()
            .api_key("sk-test")
            .model("gpt-4o")
            .chat_model("gpt-4o-mini")
            .embedding_model("text-embedding-3-large")
            .build()
            .unwrap();
        assert_eq!(provider.config.chat_model, "gpt-4o-mini");
        assert_eq!(provider.config.embedding_model, "text-embedding-3-large");
    }
}

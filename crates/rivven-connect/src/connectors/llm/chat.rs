//! LLM Chat Transform — enrich events via LLM chat completions
//!
//! Sends event data through an LLM using a configurable prompt template
//! and writes the response into a specified output field.
//!
//! # Pipeline Example
//!
//! ```text
//! Source → LLM Chat Transform (classify) → Sink
//!
//! Input:  {"text": "The product broke after 2 days", "id": 123}
//! Output: {"text": "...", "id": 123, "sentiment": "negative"}
//! ```
//!
//! # Configuration
//!
//! ```yaml
//! transforms:
//!   classify:
//!     transform: llm-chat
//!     config:
//!       provider: openai         # openai | bedrock
//!       api_key: "${OPENAI_API_KEY}"
//!       model: gpt-4o-mini
//!       system_prompt: "Classify sentiment as: positive, negative, neutral"
//!       prompt_template: "{{text}}"
//!       output_field: sentiment
//!       temperature: 0.0
//!       max_tokens: 10
//! ```

use crate::error::{ConnectorError, Result};
use crate::traits::event::SourceEvent;
use crate::traits::spec::ConnectorSpec;
use crate::traits::transform::{Transform, TransformOutput};
use crate::types::SensitiveString;
use async_trait::async_trait;
use metrics::{counter, histogram};
use rivven_llm::{ChatRequest, FinishReason, LlmProvider};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::{debug, warn};
use validator::Validate;

use super::{resolve_field, LlmProviderType};
use schemars::JsonSchema;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the LLM chat transform
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct LlmChatTransformConfig {
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

    /// Model identifier
    #[validate(length(min = 1, max = 256))]
    pub model: String,

    /// System prompt (optional — sets LLM behavior/persona)
    #[serde(default)]
    pub system_prompt: Option<String>,

    /// Prompt template with {{field}} placeholders
    ///
    /// Placeholders are replaced with values from the event data.
    /// Supports nested fields with dot notation: `{{address.city}}`
    ///
    /// Special placeholders:
    /// - `{{_data}}` — entire event data as JSON
    /// - `{{_stream}}` — stream/topic name
    /// - `{{_timestamp}}` — event timestamp
    #[validate(length(min = 1, max = 65536))]
    pub prompt_template: String,

    /// Output field name where the LLM response is written
    #[serde(default = "default_output_field")]
    #[validate(length(min = 1, max = 256))]
    pub output_field: String,

    /// Sampling temperature (0.0–2.0, lower = more deterministic)
    #[serde(default)]
    pub temperature: Option<f32>,

    /// Maximum tokens in the response
    #[serde(default)]
    pub max_tokens: Option<u32>,

    /// Request timeout in seconds
    #[serde(default = "default_timeout_secs")]
    #[validate(range(min = 1, max = 600))]
    pub timeout_secs: u64,

    /// Skip events where template rendering fails (instead of erroring)
    #[serde(default)]
    pub skip_on_error: bool,
}

fn default_output_field() -> String {
    "llm_response".to_string()
}

fn default_timeout_secs() -> u64 {
    60
}

impl LlmChatTransformConfig {
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
// Template rendering
// ============================================================================

/// Render a prompt template by replacing `{{field}}` placeholders with event data.
///
/// SECURITY: Replacement values are NOT re-scanned for placeholders, preventing
/// server-side template injection (SSTI) when event data contains `{{...}}`.
fn render_template(template: &str, event: &SourceEvent) -> String {
    let data = &event.data;
    let mut result = String::with_capacity(template.len());
    let mut rest = template;

    loop {
        let start = match rest.find("{{") {
            Some(i) => i,
            None => {
                result.push_str(rest);
                break;
            }
        };
        let after_open = &rest[start + 2..];
        let end_rel = match after_open.find("}}") {
            Some(i) => i,
            None => {
                // Unclosed placeholder — emit remainder as-is
                result.push_str(rest);
                break;
            }
        };

        // Emit text before the placeholder
        result.push_str(&rest[..start]);

        let placeholder = after_open[..end_rel].trim();
        let replacement = match placeholder {
            "_data" => serde_json::to_string(data).unwrap_or_default(),
            "_stream" => event.stream.clone(),
            "_timestamp" => event.timestamp.to_rfc3339(),
            field => resolve_field(data, field)
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default(),
        };

        // Append replacement (no re-scanning)
        result.push_str(&replacement);

        // Advance past the closing `}}`
        rest = &after_open[end_rel + 2..];
    }

    result
}

// ============================================================================
// Transform implementation
// ============================================================================

/// LLM Chat Transform
///
/// Enriches each event by sending data through an LLM and adding the response.
pub struct LlmChatTransform {
    provider: OnceCell<Arc<dyn LlmProvider>>,
}

impl LlmChatTransform {
    pub fn new() -> Self {
        Self {
            provider: OnceCell::new(),
        }
    }

    /// Create or get the LLM provider (lazy initialization)
    async fn get_provider(&self, config: &LlmChatTransformConfig) -> Result<&Arc<dyn LlmProvider>> {
        self.provider
            .get_or_try_init(|| async { create_provider(config).await })
            .await
    }
}

impl Default for LlmChatTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LlmChatTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmChatTransform")
            .field("initialized", &self.provider.initialized())
            .finish()
    }
}

#[async_trait]
impl Transform for LlmChatTransform {
    type Config = LlmChatTransformConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("llm-chat", env!("CARGO_PKG_VERSION"))
            .description("Enrich events via LLM chat completions (OpenAI, Bedrock)")
            .config_schema_from::<LlmChatTransformConfig>()
            .metadata("provider", "openai,bedrock")
            .metadata("type", "transform")
    }

    async fn init(&self, config: &Self::Config) -> Result<()> {
        config.validate_provider().map_err(ConnectorError::Config)?;
        // Eagerly initialize provider to fail fast
        let provider = self.get_provider(config).await?;

        // Verify provider supports chat
        if !provider.capabilities().chat {
            return Err(ConnectorError::Config(format!(
                "provider '{}' does not support chat completions",
                provider.name(),
            ))
            .into());
        }

        debug!(model = %config.model, provider = %config.provider, "LLM chat transform initialized");
        Ok(())
    }

    async fn transform(
        &self,
        config: &Self::Config,
        event: SourceEvent,
    ) -> Result<TransformOutput> {
        let provider = self.get_provider(config).await?;

        // Render prompt from template
        let prompt = render_template(&config.prompt_template, &event);

        if prompt.trim().is_empty() {
            if config.skip_on_error {
                warn!(stream = %event.stream, "empty prompt after template rendering, skipping");
                return Ok(TransformOutput::single(event));
            }
            return Err(ConnectorError::Config(
                "prompt template rendered to empty string".to_string(),
            )
            .into());
        }

        // Build chat request — system prompt first (if any), then user message
        let mut builder = ChatRequest::builder();

        if let Some(ref system) = config.system_prompt {
            builder = builder.system(system);
        }

        builder = builder.user(&prompt);

        if let Some(t) = config.temperature {
            builder = builder.temperature(t);
        }
        if let Some(m) = config.max_tokens {
            builder = builder.max_tokens(m);
        }

        let request = builder.model(&config.model).build();

        // Call LLM with timeout
        let timeout = Duration::from_secs(config.timeout_secs);
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(timeout, provider.chat(&request)).await;
        let elapsed_ms = start.elapsed().as_millis() as f64;

        histogram!("llm.chat.duration_ms").record(elapsed_ms);

        let response = match result {
            Ok(Ok(resp)) => {
                counter!("llm.chat.requests.success").increment(1);
                resp
            }
            Ok(Err(e)) => {
                counter!("llm.chat.requests.failed").increment(1);
                if config.skip_on_error {
                    warn!(error = %e, stream = %event.stream, "LLM chat failed, passing through");
                    return Ok(TransformOutput::single(event));
                }
                if e.is_retryable() {
                    return Err(ConnectorError::Transient(format!(
                        "LLM chat error: {}",
                        e.truncated_message(512)
                    ))
                    .into());
                }
                return Err(ConnectorError::Fatal(format!(
                    "LLM chat error: {}",
                    e.truncated_message(512)
                ))
                .into());
            }
            Err(_timeout) => {
                counter!("llm.chat.requests.timeout").increment(1);
                if config.skip_on_error {
                    warn!(stream = %event.stream, timeout_secs = config.timeout_secs, "LLM chat timed out, passing through");
                    return Ok(TransformOutput::single(event));
                }
                return Err(ConnectorError::Timeout(format!(
                    "LLM chat timed out after {}s",
                    config.timeout_secs
                ))
                .into());
            }
        };

        // Check finish reason — warn on truncation / content filtering
        if let Some(reason) = response.finish_reason() {
            match reason {
                FinishReason::Stop => {} // normal
                FinishReason::Length => {
                    counter!("llm.chat.finish_reason.length").increment(1);
                    warn!(
                        stream = %event.stream,
                        model = %config.model,
                        max_tokens = ?config.max_tokens,
                        "LLM response truncated — hit max_tokens limit"
                    );
                }
                FinishReason::ContentFilter => {
                    counter!("llm.chat.finish_reason.content_filter").increment(1);
                    warn!(
                        stream = %event.stream,
                        model = %config.model,
                        "LLM response censored by content filter"
                    );
                }
                other => {
                    counter!("llm.chat.finish_reason.other").increment(1);
                    warn!(
                        stream = %event.stream,
                        finish_reason = %other,
                        "unexpected LLM finish reason"
                    );
                }
            }
        }

        // Add response to event data
        let content = response.content().to_string();

        // Record token usage
        counter!("llm.chat.tokens.prompt").increment(response.usage.prompt_tokens as u64);
        counter!("llm.chat.tokens.completion").increment(response.usage.completion_tokens as u64);
        counter!("llm.chat.tokens.total").increment(response.usage.total_tokens as u64);

        let mut enriched = event;

        if let serde_json::Value::Object(ref mut map) = enriched.data {
            map.insert(
                config.output_field.clone(),
                serde_json::Value::String(content),
            );
        } else {
            // If data isn't an object, wrap it
            let mut map = serde_json::Map::new();
            map.insert("_original".to_string(), std::mem::take(&mut enriched.data));
            map.insert(
                config.output_field.clone(),
                serde_json::Value::String(content),
            );
            enriched.data = serde_json::Value::Object(map);
        }

        Ok(TransformOutput::single(enriched))
    }
}

// Bridge macro for type-erased dispatch
crate::impl_any_transform!(LlmChatTransform, LlmChatTransformConfig);

/// Factory for the LLM chat transform
pub struct LlmChatTransformFactory;

impl crate::TransformFactory for LlmChatTransformFactory {
    fn spec(&self) -> ConnectorSpec {
        LlmChatTransform::spec()
    }

    fn create(&self) -> Box<dyn crate::AnyTransform> {
        Box::new(LlmChatTransform::new())
    }
}

// ============================================================================
// Provider construction
// ============================================================================

async fn create_provider(config: &LlmChatTransformConfig) -> Result<Arc<dyn LlmProvider>> {
    match config.provider {
        LlmProviderType::OpenAi => {
            #[cfg(feature = "llm-openai")]
            {
                let api_key = config.api_key.as_ref().ok_or_else(|| {
                    ConnectorError::Config("api_key is required for OpenAI".to_string())
                })?;

                let mut builder = rivven_llm::openai::OpenAiProvider::builder()
                    .api_key(api_key.expose_secret())
                    .chat_model(&config.model)
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
                    .chat_model(&config.model)
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
    use crate::SourceEvent;
    use serde_json::json;

    // ── Template rendering ──────────────────────────────────────────

    #[test]
    fn test_render_template_simple() {
        let event = SourceEvent::record("test", json!({"name": "Alice", "age": 30}));
        let result = render_template("Hello {{name}}, you are {{age}}", &event);
        assert_eq!(result, "Hello Alice, you are 30");
    }

    #[test]
    fn test_render_template_nested_field() {
        let event = SourceEvent::record(
            "test",
            json!({"user": {"name": "Bob", "address": {"city": "Berlin"}}}),
        );
        let result = render_template("City: {{user.address.city}}", &event);
        assert_eq!(result, "City: Berlin");
    }

    #[test]
    fn test_render_template_data_placeholder() {
        let event = SourceEvent::record("test", json!({"x": 1}));
        let result = render_template("Data: {{_data}}", &event);
        assert_eq!(result, r#"Data: {"x":1}"#);
    }

    #[test]
    fn test_render_template_stream_placeholder() {
        let event = SourceEvent::record("my-topic", json!({}));
        let result = render_template("Stream: {{_stream}}", &event);
        assert_eq!(result, "Stream: my-topic");
    }

    #[test]
    fn test_render_template_missing_field() {
        let event = SourceEvent::record("test", json!({"a": 1}));
        let result = render_template("Value: {{missing}}", &event);
        assert_eq!(result, "Value: ");
    }

    #[test]
    fn test_render_template_no_placeholders() {
        let event = SourceEvent::record("test", json!({}));
        let result = render_template("static text", &event);
        assert_eq!(result, "static text");
    }

    #[test]
    fn test_render_template_multiple_placeholders() {
        let event = SourceEvent::record("test", json!({"first": "Jane", "last": "Doe"}));
        let result = render_template("{{first}} {{last}}", &event);
        assert_eq!(result, "Jane Doe");
    }

    #[test]
    fn test_render_template_string_value() {
        let event = SourceEvent::record("test", json!({"msg": "hello world"}));
        // String values should NOT be quoted
        let result = render_template("Say: {{msg}}", &event);
        assert_eq!(result, "Say: hello world");
    }

    // ── Config validation ───────────────────────────────────────────

    #[test]
    fn test_config_validate_openai_needs_api_key() {
        let config = LlmChatTransformConfig {
            provider: LlmProviderType::OpenAi,
            api_key: None,
            base_url: None,
            region: None,
            model: "gpt-4o-mini".to_string(),
            system_prompt: None,
            prompt_template: "{{text}}".to_string(),
            output_field: "response".to_string(),
            temperature: None,
            max_tokens: None,
            timeout_secs: 60,
            skip_on_error: false,
        };
        let err = config.validate_provider().unwrap_err();
        assert!(err.contains("api_key is required"));
    }

    #[test]
    fn test_config_validate_bedrock_no_api_key_needed() {
        let config = LlmChatTransformConfig {
            provider: LlmProviderType::Bedrock,
            api_key: None,
            base_url: None,
            region: Some("us-east-1".to_string()),
            model: "anthropic.claude-3-haiku-20240307-v1:0".to_string(),
            system_prompt: None,
            prompt_template: "{{text}}".to_string(),
            output_field: "response".to_string(),
            temperature: None,
            max_tokens: None,
            timeout_secs: 60,
            skip_on_error: false,
        };
        config.validate_provider().unwrap();
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let yaml = r#"
provider: openai
api_key: "sk-test"
model: "gpt-4o-mini"
prompt_template: "Summarize: {{text}}"
output_field: "summary"
temperature: 0.3
max_tokens: 100
timeout_secs: 30
"#;
        let config: LlmChatTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, LlmProviderType::OpenAi);
        assert_eq!(config.model, "gpt-4o-mini");
        assert_eq!(config.output_field, "summary");
        assert_eq!(config.temperature, Some(0.3));
        assert_eq!(config.max_tokens, Some(100));
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
model: "gpt-4o"
prompt_template: "{{text}}"
"#;
        let config: LlmChatTransformConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, LlmProviderType::OpenAi);
        assert_eq!(config.output_field, "llm_response");
        assert_eq!(config.timeout_secs, 60);
        assert!(!config.skip_on_error);
    }

    // ── Spec ────────────────────────────────────────────────────────

    #[test]
    fn test_spec() {
        let spec = LlmChatTransform::spec();
        assert_eq!(spec.connector_type, "llm-chat");
        assert!(spec.description.unwrap().contains("LLM"));
    }

    // ── Factory ─────────────────────────────────────────────────────

    #[test]
    fn test_factory() {
        let factory = LlmChatTransformFactory;
        let spec = <LlmChatTransformFactory as crate::TransformFactory>::spec(&factory);
        assert_eq!(spec.connector_type, "llm-chat");
    }

    // ── Debug ───────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        let transform = LlmChatTransform::new();
        let debug = format!("{transform:?}");
        assert!(debug.contains("LlmChatTransform"));
        assert!(debug.contains("initialized"));
    }

    // ── Security: SSTI prevention ───────────────────────────────────

    #[test]
    fn test_render_template_ssti_prevention() {
        // Field value containing placeholder syntax must NOT be re-expanded
        let event = SourceEvent::record(
            "test",
            json!({"user_input": "{{_stream}}", "secret": "LEAKED"}),
        );
        let result = render_template("Input: {{user_input}}", &event);
        // The {{_stream}} in the value must appear literally, NOT be expanded
        assert_eq!(result, "Input: {{_stream}}");
    }

    #[test]
    fn test_render_template_ssti_nested_placeholder() {
        // Field value with {{other_field}} must not cause injection
        let event = SourceEvent::record("test", json!({"a": "{{b}}", "b": "INJECTED"}));
        let result = render_template("Value: {{a}}", &event);
        assert_eq!(result, "Value: {{b}}");
    }

    #[test]
    fn test_render_template_unclosed_placeholder() {
        let event = SourceEvent::record("test", json!({"x": 1}));
        let result = render_template("Hello {{ world", &event);
        assert_eq!(result, "Hello {{ world");
    }

    #[test]
    fn test_render_template_whitespace_only() {
        let event = SourceEvent::record("test", json!({"blank": "   "}));
        let result = render_template("{{blank}}", &event);
        assert_eq!(result, "   ");
        // The transform should detect this via trim() check
        assert!(result.trim().is_empty());
    }
}

//! AWS Bedrock provider — chat completions and embeddings via the official
//! AWS SDK for Bedrock Runtime
//!
//! Uses `aws-sdk-bedrockruntime` for typed API calls with automatic SigV4
//! signing, credential refresh, retries, and error handling. Credentials
//! are resolved from the standard AWS credential chain (env vars, profiles,
//! IMDS, ECS).
//!
//! # Supported Models
//!
//! - **Chat**: Anthropic Claude 3.x, Amazon Titan, Meta Llama 3, Mistral
//!   (via the Converse API)
//! - **Embeddings**: Amazon Titan Embed, Cohere Embed (via InvokeModel)
//!
//! # Example
//!
//! ```rust,no_run
//! use rivven_llm::bedrock::BedrockProvider;
//!
//! # async fn example() -> Result<(), rivven_llm::LlmError> {
//! let provider = BedrockProvider::builder()
//!     .region("us-east-1")
//!     .model("anthropic.claude-3-haiku-20240307-v1:0")
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::error::{LlmError, LlmResult};
use crate::provider::{LlmProvider, ProviderCapabilities};
use crate::types::*;
use async_trait::async_trait;
use aws_sdk_bedrockruntime::operation::converse::ConverseError;
use aws_sdk_bedrockruntime::operation::invoke_model::InvokeModelError;
use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, InferenceConfiguration, Message, SystemContentBlock,
};
use aws_sdk_bedrockruntime::Client as BedrockClient;
use std::time::Duration;
use tracing::debug;

/// Default Bedrock region
const DEFAULT_REGION: &str = "us-east-1";

/// Default chat model (Claude 3 Haiku — fast, cheap)
const DEFAULT_CHAT_MODEL: &str = "anthropic.claude-3-haiku-20240307-v1:0";

/// Default embedding model
const DEFAULT_EMBEDDING_MODEL: &str = "amazon.titan-embed-text-v2:0";

/// Bedrock provider configuration
#[derive(Debug, Clone)]
pub struct BedrockConfig {
    /// AWS region
    pub region: String,
    /// Default chat model ID
    pub chat_model: String,
    /// Default embedding model ID
    pub embedding_model: String,
    /// Request timeout
    pub timeout: Duration,
    /// Optional endpoint override (for VPC endpoints, LocalStack, etc.)
    pub endpoint_url: Option<String>,
}

/// Builder for `BedrockProvider`
#[derive(Debug, Default)]
pub struct BedrockProviderBuilder {
    region: Option<String>,
    chat_model: Option<String>,
    embedding_model: Option<String>,
    timeout: Option<Duration>,
    endpoint_url: Option<String>,
}

impl BedrockProviderBuilder {
    /// Set AWS region (default: us-east-1)
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set default model for both chat and embedding
    ///
    /// Note: on Bedrock, chat models (e.g. Claude) cannot generate
    /// embeddings — use `chat_model()` / `embedding_model()` to set
    /// them independently when using both capabilities.
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

    /// Override endpoint URL (for VPC endpoints, LocalStack, etc.)
    pub fn endpoint_url(mut self, url: impl Into<String>) -> Self {
        self.endpoint_url = Some(url.into());
        self
    }

    /// Build the provider (async — resolves AWS credentials)
    pub async fn build(self) -> LlmResult<BedrockProvider> {
        let region = self.region.unwrap_or_else(|| DEFAULT_REGION.to_string());

        let timeout = self.timeout.unwrap_or(Duration::from_secs(60));

        // Load AWS config from environment / credential chain
        let mut aws_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()));

        if let Some(ref endpoint) = self.endpoint_url {
            aws_config_loader = aws_config_loader.endpoint_url(endpoint);
        }

        let sdk_config = aws_config_loader.load().await;

        // Build Bedrock runtime client with service-specific config
        let mut bedrock_config = aws_sdk_bedrockruntime::config::Builder::from(&sdk_config);

        // Apply timeout via stalled stream protection and operation timeout
        let timeout_config = aws_sdk_bedrockruntime::config::timeout::TimeoutConfig::builder()
            .operation_timeout(timeout)
            .build();
        bedrock_config = bedrock_config.timeout_config(timeout_config);

        let client = BedrockClient::from_conf(bedrock_config.build());

        let config = BedrockConfig {
            region: region.clone(),
            chat_model: self
                .chat_model
                .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string()),
            embedding_model: self
                .embedding_model
                .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL.to_string()),
            timeout,
            endpoint_url: self.endpoint_url,
        };

        debug!(
            region = %config.region,
            chat_model = %config.chat_model,
            embedding_model = %config.embedding_model,
            "Bedrock provider initialized (aws-sdk)"
        );

        Ok(BedrockProvider { config, client })
    }
}

/// AWS Bedrock LLM provider
///
/// Uses the Bedrock Converse API for chat and InvokeModel for embeddings.
/// Authenticates via the standard AWS credential chain through the official
/// AWS SDK — SigV4 signing, credential refresh, and retries are handled
/// automatically.
pub struct BedrockProvider {
    config: BedrockConfig,
    client: BedrockClient,
}

impl std::fmt::Debug for BedrockProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BedrockProvider")
            .field("region", &self.config.region)
            .field("chat_model", &self.config.chat_model)
            .field("embedding_model", &self.config.embedding_model)
            .finish()
    }
}

impl BedrockProvider {
    /// Create a builder
    pub fn builder() -> BedrockProviderBuilder {
        BedrockProviderBuilder::default()
    }

    /// Map a Converse API SDK error into our `LlmError`
    ///
    /// Handles both transport-level errors (timeouts, network failures,
    /// request construction errors) and service-level errors (typed
    /// exception variants from the Bedrock API).
    fn map_converse_error(err: aws_sdk_bedrockruntime::error::SdkError<ConverseError>) -> LlmError {
        use aws_sdk_bedrockruntime::error::SdkError;

        // Capture full error context before consuming the SdkError
        let full_msg = format!("{err}");

        match err {
            // Transport-level errors — classified before reaching the service
            SdkError::ConstructionFailure(_) => {
                LlmError::Config(format!("bedrock request construction failed: {full_msg}"))
            }
            SdkError::TimeoutError(_) => LlmError::Timeout(full_msg),
            SdkError::DispatchFailure(_) => LlmError::Connection(full_msg),
            SdkError::ResponseError(_) => {
                LlmError::Serialization(format!("bedrock response error: {full_msg}"))
            }

            // Service-level errors — typed exception variants
            SdkError::ServiceError(ctx) => match ctx.into_err() {
                ConverseError::AccessDeniedException(e) => {
                    LlmError::Auth(e.message().unwrap_or("access denied").to_string())
                }
                ConverseError::ThrottlingException(e) => LlmError::RateLimited {
                    message: e.message().unwrap_or("throttled").to_string(),
                    retry_after_secs: None,
                },
                ConverseError::ResourceNotFoundException(e) => {
                    LlmError::ModelNotFound(e.message().unwrap_or("resource not found").to_string())
                }
                ConverseError::ModelTimeoutException(e) => {
                    LlmError::Timeout(e.message().unwrap_or("model timeout").to_string())
                }
                ConverseError::ModelNotReadyException(e) => {
                    LlmError::Transient(e.message().unwrap_or("model not ready").to_string())
                }
                ConverseError::InternalServerException(e) => {
                    LlmError::Transient(e.message().unwrap_or("internal server error").to_string())
                }
                ConverseError::ServiceUnavailableException(e) => {
                    LlmError::Transient(e.message().unwrap_or("service unavailable").to_string())
                }
                ConverseError::ValidationException(e) => {
                    let msg = e.message().unwrap_or("validation error").to_string();
                    let lower = msg.to_lowercase();
                    if lower.contains("too many tokens") || lower.contains("maximum") {
                        LlmError::TokenLimitExceeded(msg)
                    } else {
                        LlmError::Provider {
                            status: 400,
                            message: msg,
                        }
                    }
                }
                ConverseError::ModelErrorException(e) => LlmError::Provider {
                    status: 400,
                    message: e.message().unwrap_or("model error").to_string(),
                },
                err => LlmError::Internal(format!("bedrock converse error: {err}")),
            },

            // Future SDK variants (non_exhaustive)
            _ => LlmError::Internal(full_msg),
        }
    }

    /// Map an InvokeModel SDK error into our `LlmError`
    ///
    /// Same transport/service split as `map_converse_error`.
    fn map_invoke_model_error(
        err: aws_sdk_bedrockruntime::error::SdkError<InvokeModelError>,
    ) -> LlmError {
        use aws_sdk_bedrockruntime::error::SdkError;

        let full_msg = format!("{err}");

        match err {
            SdkError::ConstructionFailure(_) => {
                LlmError::Config(format!("bedrock request construction failed: {full_msg}"))
            }
            SdkError::TimeoutError(_) => LlmError::Timeout(full_msg),
            SdkError::DispatchFailure(_) => LlmError::Connection(full_msg),
            SdkError::ResponseError(_) => {
                LlmError::Serialization(format!("bedrock response error: {full_msg}"))
            }

            SdkError::ServiceError(ctx) => match ctx.into_err() {
                InvokeModelError::AccessDeniedException(e) => {
                    LlmError::Auth(e.message().unwrap_or("access denied").to_string())
                }
                InvokeModelError::ThrottlingException(e) => LlmError::RateLimited {
                    message: e.message().unwrap_or("throttled").to_string(),
                    retry_after_secs: None,
                },
                InvokeModelError::ResourceNotFoundException(e) => {
                    LlmError::ModelNotFound(e.message().unwrap_or("resource not found").to_string())
                }
                InvokeModelError::ModelTimeoutException(e) => {
                    LlmError::Timeout(e.message().unwrap_or("model timeout").to_string())
                }
                InvokeModelError::ModelNotReadyException(e) => {
                    LlmError::Transient(e.message().unwrap_or("model not ready").to_string())
                }
                InvokeModelError::InternalServerException(e) => {
                    LlmError::Transient(e.message().unwrap_or("internal server error").to_string())
                }
                InvokeModelError::ServiceUnavailableException(e) => {
                    LlmError::Transient(e.message().unwrap_or("service unavailable").to_string())
                }
                InvokeModelError::ValidationException(e) => {
                    let msg = e.message().unwrap_or("validation error").to_string();
                    let lower = msg.to_lowercase();
                    if lower.contains("too many tokens") || lower.contains("maximum") {
                        LlmError::TokenLimitExceeded(msg)
                    } else {
                        LlmError::Provider {
                            status: 400,
                            message: msg,
                        }
                    }
                }
                InvokeModelError::ModelErrorException(e) => LlmError::Provider {
                    status: 400,
                    message: e.message().unwrap_or("model error").to_string(),
                },
                err => LlmError::Internal(format!("bedrock invoke_model error: {err}")),
            },

            _ => LlmError::Internal(full_msg),
        }
    }
}

#[async_trait]
impl LlmProvider for BedrockProvider {
    fn name(&self) -> &str {
        "bedrock"
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
        let model_id = request.model.as_deref().unwrap_or(&self.config.chat_model);

        // Build SDK messages and system prompts
        let mut messages = Vec::new();
        let mut system_prompts = Vec::new();

        for msg in &request.messages {
            match msg.role {
                Role::System => {
                    system_prompts.push(SystemContentBlock::Text(msg.content.clone()));
                }
                _ => {
                    let role = match msg.role {
                        Role::User => ConversationRole::User,
                        _ => ConversationRole::Assistant,
                    };
                    messages.push(
                        Message::builder()
                            .role(role)
                            .content(ContentBlock::Text(msg.content.clone()))
                            .build()
                            .map_err(|e| {
                                LlmError::Config(format!("failed to build message: {e}"))
                            })?,
                    );
                }
            }
        }

        // Build inference configuration
        let mut inference_config = InferenceConfiguration::builder();
        if let Some(t) = request.temperature {
            inference_config = inference_config.temperature(t);
        }
        if let Some(m) = request.max_tokens {
            inference_config = inference_config.max_tokens(m as i32);
        }
        if let Some(p) = request.top_p {
            inference_config = inference_config.top_p(p);
        }
        if !request.stop.is_empty() {
            inference_config = inference_config.set_stop_sequences(Some(request.stop.clone()));
        }

        debug!(model = %model_id, messages = messages.len(), "Bedrock converse request");

        // Call the Converse API via SDK
        let mut converse = self
            .client
            .converse()
            .model_id(model_id)
            .set_messages(Some(messages))
            .inference_config(inference_config.build());

        if !system_prompts.is_empty() {
            converse = converse.set_system(Some(system_prompts));
        }

        let resp = converse.send().await.map_err(Self::map_converse_error)?;

        // Extract text from the response content blocks
        let content = resp
            .output()
            .and_then(|o| match o {
                aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg) => Some(
                    msg.content()
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text(text) => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(""),
                ),
                _ => None,
            })
            .unwrap_or_default();

        // Map stop reason
        let finish_reason = match resp.stop_reason() {
            aws_sdk_bedrockruntime::types::StopReason::EndTurn
            | aws_sdk_bedrockruntime::types::StopReason::StopSequence => FinishReason::Stop,
            aws_sdk_bedrockruntime::types::StopReason::MaxTokens => FinishReason::Length,
            aws_sdk_bedrockruntime::types::StopReason::ContentFiltered => {
                FinishReason::ContentFilter
            }
            aws_sdk_bedrockruntime::types::StopReason::ToolUse => FinishReason::ToolCalls,
            _ => FinishReason::Stop,
        };

        // Extract usage
        let (prompt_tokens, completion_tokens) = resp
            .usage()
            .map(|u| (u.input_tokens() as u32, u.output_tokens() as u32))
            .unwrap_or((0, 0));

        Ok(ChatResponse {
            id: format!("bedrock-{}", uuid::Uuid::new_v4()),
            model: model_id.to_string(),
            choices: vec![ChatChoice {
                index: 0,
                message: ChatMessage::assistant(content),
                finish_reason,
            }],
            usage: Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens + completion_tokens,
            },
        })
    }

    async fn embed(&self, request: &EmbeddingRequest) -> LlmResult<EmbeddingResponse> {
        if request.input.is_empty() {
            return Err(LlmError::Config(
                "embedding input must not be empty".to_string(),
            ));
        }

        let model_id = request
            .model
            .as_deref()
            .unwrap_or(&self.config.embedding_model);

        // Bedrock embedding — one request per input (no native batching)
        let mut embeddings = Vec::with_capacity(request.input.len());
        let mut total_tokens = 0u32;

        for (i, text) in request.input.iter().enumerate() {
            let api_request = if model_id.starts_with("amazon.titan-embed") {
                // Titan Embed format
                let mut req = serde_json::json!({ "inputText": text });
                if let Some(dims) = request.dimensions {
                    req["dimensions"] = serde_json::json!(dims);
                }
                req
            } else if model_id.starts_with("cohere.embed") {
                // Cohere format — respect input_type if specified
                let input_type = request.input_type.as_deref().unwrap_or("search_document");
                serde_json::json!({
                    "texts": [text],
                    "input_type": input_type
                })
            } else {
                // Generic fallback
                serde_json::json!({ "inputText": text })
            };

            let body_bytes = serde_json::to_vec(&api_request)?;

            debug!(model = %model_id, index = i, "Bedrock embedding request");

            let resp = self
                .client
                .invoke_model()
                .model_id(model_id)
                .content_type("application/json")
                .accept("application/json")
                .body(aws_sdk_bedrockruntime::primitives::Blob::new(body_bytes))
                .send()
                .await
                .map_err(Self::map_invoke_model_error)?;

            let resp_body: serde_json::Value = serde_json::from_slice(resp.body().as_ref())
                .map_err(|e| {
                    LlmError::Serialization(format!("failed to parse embedding response: {e}"))
                })?;

            // Parse embedding from response (Titan format)
            let values = if let Some(emb) = resp_body.get("embedding") {
                emb.as_array()
                    .ok_or_else(|| {
                        LlmError::Serialization("embedding field is not an array".to_string())
                    })?
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                    .collect()
            } else if let Some(embs) = resp_body.get("embeddings") {
                // Cohere format
                let arr = embs
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|e| e.as_array())
                    .ok_or_else(|| {
                        LlmError::Serialization(
                            "embeddings field has unexpected format".to_string(),
                        )
                    })?;
                arr.iter()
                    .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                    .collect()
            } else {
                return Err(LlmError::Serialization(
                    "no embedding field in response".to_string(),
                ));
            };

            // Token count from response (if available)
            if let Some(tokens) = resp_body
                .get("inputTextTokenCount")
                .and_then(|v| v.as_u64())
            {
                total_tokens += tokens as u32;
            }

            embeddings.push(Embedding {
                index: i as u32,
                values,
            });
        }

        Ok(EmbeddingResponse {
            model: model_id.to_string(),
            embeddings,
            usage: EmbeddingUsage {
                prompt_tokens: total_tokens,
                total_tokens,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        assert_eq!(DEFAULT_REGION, "us-east-1");
        assert!(DEFAULT_CHAT_MODEL.contains("claude"));
        assert!(DEFAULT_EMBEDDING_MODEL.contains("titan"));
    }

    #[test]
    fn test_sdk_message_building() {
        // Verify SDK message builder produces valid messages
        let msg = Message::builder()
            .role(ConversationRole::User)
            .content(ContentBlock::Text("Hello".to_string()))
            .build()
            .unwrap();
        assert_eq!(msg.role(), &ConversationRole::User);
        assert_eq!(msg.content().len(), 1);
    }

    #[test]
    fn test_sdk_system_content_block() {
        let block = SystemContentBlock::Text("Be concise.".to_string());
        match &block {
            SystemContentBlock::Text(t) => assert_eq!(t, "Be concise."),
            _ => panic!("expected Text variant"),
        }
    }

    #[test]
    fn test_sdk_inference_configuration() {
        let config = InferenceConfiguration::builder()
            .temperature(0.5)
            .max_tokens(100)
            .top_p(0.9)
            .stop_sequences("STOP".to_string())
            .build();
        assert_eq!(config.temperature(), Some(0.5));
        assert_eq!(config.max_tokens(), Some(100));
        assert_eq!(config.top_p(), Some(0.9));
        assert_eq!(config.stop_sequences(), ["STOP"]);
    }

    #[test]
    fn test_sdk_inference_configuration_defaults() {
        let config = InferenceConfiguration::builder().build();
        assert_eq!(config.temperature(), None);
        assert_eq!(config.max_tokens(), None);
        assert_eq!(config.top_p(), None);
        assert!(config.stop_sequences().is_empty());
    }

    #[test]
    fn test_sdk_content_block_text() {
        let block = ContentBlock::Text("hello world".to_string());
        match &block {
            ContentBlock::Text(t) => assert_eq!(t, "hello world"),
            _ => panic!("expected Text variant"),
        }
    }

    #[test]
    fn test_sdk_conversation_roles() {
        assert_ne!(ConversationRole::User, ConversationRole::Assistant);
    }

    #[test]
    fn test_endpoint_default() {
        let config = BedrockConfig {
            region: "us-west-2".to_string(),
            chat_model: DEFAULT_CHAT_MODEL.to_string(),
            embedding_model: DEFAULT_EMBEDDING_MODEL.to_string(),
            timeout: Duration::from_secs(60),
            endpoint_url: None,
        };
        assert_eq!(config.region, "us-west-2");
        assert!(config.endpoint_url.is_none());
    }

    #[test]
    fn test_endpoint_override() {
        let config = BedrockConfig {
            region: "us-east-1".to_string(),
            chat_model: DEFAULT_CHAT_MODEL.to_string(),
            embedding_model: DEFAULT_EMBEDDING_MODEL.to_string(),
            timeout: Duration::from_secs(60),
            endpoint_url: Some("http://localhost:4566".to_string()),
        };
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:4566")
        );
    }

    #[test]
    fn test_builder_defaults() {
        let builder = BedrockProviderBuilder::default();
        assert!(builder.region.is_none());
        assert!(builder.chat_model.is_none());
        assert!(builder.embedding_model.is_none());
        assert!(builder.timeout.is_none());
        assert!(builder.endpoint_url.is_none());
    }

    #[test]
    fn test_builder_fluent_api() {
        let builder = BedrockProviderBuilder::default()
            .region("eu-west-1")
            .chat_model("anthropic.claude-3-sonnet-20240229-v1:0")
            .embedding_model("cohere.embed-english-v3")
            .timeout(Duration::from_secs(120))
            .endpoint_url("http://localhost:4566");

        assert_eq!(builder.region.as_deref(), Some("eu-west-1"));
        assert_eq!(
            builder.chat_model.as_deref(),
            Some("anthropic.claude-3-sonnet-20240229-v1:0")
        );
        assert_eq!(
            builder.embedding_model.as_deref(),
            Some("cohere.embed-english-v3")
        );
        assert_eq!(builder.timeout, Some(Duration::from_secs(120)));
        assert_eq!(
            builder.endpoint_url.as_deref(),
            Some("http://localhost:4566")
        );
    }

    #[test]
    fn test_model_sets_both_chat_and_embedding() {
        let builder = BedrockProviderBuilder::default().model("my-model");
        assert_eq!(builder.chat_model.as_deref(), Some("my-model"));
        assert_eq!(builder.embedding_model.as_deref(), Some("my-model"));
    }

    #[test]
    fn test_separate_models_override_shorthand() {
        let builder = BedrockProviderBuilder::default()
            .model("generic")
            .chat_model("anthropic.claude-3-haiku-20240307-v1:0")
            .embedding_model("amazon.titan-embed-text-v2:0");
        assert_eq!(
            builder.chat_model.as_deref(),
            Some("anthropic.claude-3-haiku-20240307-v1:0")
        );
        assert_eq!(
            builder.embedding_model.as_deref(),
            Some("amazon.titan-embed-text-v2:0")
        );
    }

    #[test]
    fn test_embedding_request_formats() {
        // Titan format
        let req = serde_json::json!({ "inputText": "hello", "dimensions": 256 });
        assert_eq!(req["inputText"], "hello");
        assert_eq!(req["dimensions"], 256);

        // Cohere format — default input_type
        let req = serde_json::json!({
            "texts": ["hello"],
            "input_type": "search_document"
        });
        assert!(req["texts"].is_array());
        assert_eq!(req["input_type"], "search_document");

        // Cohere format — search_query input_type
        let req = serde_json::json!({
            "texts": ["hello"],
            "input_type": "search_query"
        });
        assert_eq!(req["input_type"], "search_query");
    }

    #[test]
    fn test_embedding_input_type_builder() {
        let req = EmbeddingRequest::builder()
            .input("test")
            .input_type("search_query")
            .build();
        assert_eq!(req.input_type.as_deref(), Some("search_query"));
    }

    #[test]
    fn test_embedding_single_no_input_type() {
        let req = EmbeddingRequest::single("hello");
        assert!(req.input_type.is_none());
    }

    #[test]
    fn test_embedding_response_parsing_titan() {
        let resp = serde_json::json!({
            "embedding": [0.1, 0.2, 0.3],
            "inputTextTokenCount": 5
        });
        let emb = resp["embedding"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect::<Vec<_>>();
        assert_eq!(emb, vec![0.1f32, 0.2, 0.3]);
        assert_eq!(resp["inputTextTokenCount"].as_u64().unwrap(), 5);
    }

    #[test]
    fn test_embedding_response_parsing_cohere() {
        let resp = serde_json::json!({
            "embeddings": [[0.4, 0.5, 0.6]]
        });
        let arr = resp["embeddings"]
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .as_array()
            .unwrap();
        let emb: Vec<f32> = arr.iter().map(|v| v.as_f64().unwrap() as f32).collect();
        assert_eq!(emb, vec![0.4f32, 0.5, 0.6]);
    }
}

//! Core types for LLM interactions
//!
//! Provider-agnostic types for chat completions and embeddings.

use serde::{Deserialize, Serialize};
use tracing::warn;

// ============================================================================
// Chat Types
// ============================================================================

/// Role of a chat message participant
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// System message (sets behavior / persona)
    System,
    /// User message (the human prompt)
    User,
    /// Assistant message (the LLM response)
    Assistant,
    /// Tool/function call result
    Tool,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::System => write!(f, "system"),
            Role::User => write!(f, "user"),
            Role::Assistant => write!(f, "assistant"),
            Role::Tool => write!(f, "tool"),
        }
    }
}

/// A single message in a chat conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Role of the message sender
    pub role: Role,
    /// Message content
    pub content: String,
}

impl ChatMessage {
    /// Create a system message
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: content.into(),
        }
    }

    /// Create a user message
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
        }
    }

    /// Create an assistant message
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
        }
    }
}

/// Request for a chat completion
///
/// # Security — Prompt Injection
///
/// When building a `ChatRequest` from untrusted input, ensure that
/// user-supplied text is placed exclusively in `User`-role messages and is
/// never concatenated into `System` prompts without sanitization. See
/// [`LlmProvider`](crate::provider::LlmProvider) for detailed guidance.
#[derive(Debug, Clone)]
pub struct ChatRequest {
    /// Messages in the conversation
    pub messages: Vec<ChatMessage>,
    /// Model override (uses provider default if None)
    pub model: Option<String>,
    /// Sampling temperature (0.0–2.0, lower = more deterministic)
    pub temperature: Option<f32>,
    /// Maximum tokens in the response
    pub max_tokens: Option<u32>,
    /// Top-p nucleus sampling (0.0–1.0)
    pub top_p: Option<f32>,
    /// Stop sequences
    pub stop: Vec<String>,
}

impl ChatRequest {
    /// Create a builder for `ChatRequest`
    pub fn builder() -> ChatRequestBuilder {
        ChatRequestBuilder::default()
    }

    /// Quick single-prompt request
    pub fn prompt(content: impl Into<String>) -> Self {
        Self {
            messages: vec![ChatMessage::user(content)],
            model: None,
            temperature: None,
            max_tokens: None,
            top_p: None,
            stop: Vec::new(),
        }
    }

    /// Quick single-prompt with system message
    pub fn with_system(system: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            messages: vec![ChatMessage::system(system), ChatMessage::user(prompt)],
            model: None,
            temperature: None,
            max_tokens: None,
            top_p: None,
            stop: Vec::new(),
        }
    }
}

/// Builder for `ChatRequest`
#[derive(Debug, Default)]
pub struct ChatRequestBuilder {
    messages: Vec<ChatMessage>,
    model: Option<String>,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    top_p: Option<f32>,
    stop: Vec<String>,
}

impl ChatRequestBuilder {
    /// Add a message to the conversation
    pub fn message(mut self, msg: ChatMessage) -> Self {
        self.messages.push(msg);
        self
    }

    /// Add multiple messages
    pub fn messages(mut self, msgs: impl IntoIterator<Item = ChatMessage>) -> Self {
        self.messages.extend(msgs);
        self
    }

    /// Set the system prompt
    pub fn system(self, content: impl Into<String>) -> Self {
        self.message(ChatMessage::system(content))
    }

    /// Add a user message
    pub fn user(self, content: impl Into<String>) -> Self {
        self.message(ChatMessage::user(content))
    }

    /// Override model
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Set temperature (0.0–2.0). Values outside this range are clamped.
    pub fn temperature(mut self, t: f32) -> Self {
        let clamped = t.clamp(0.0, 2.0);
        if (clamped - t).abs() > f32::EPSILON {
            warn!(
                requested = t,
                clamped = clamped,
                "temperature value {t} out of range [0.0, 2.0], clamped to {clamped}"
            );
        }
        self.temperature = Some(clamped);
        self
    }

    /// Set max tokens
    pub fn max_tokens(mut self, n: u32) -> Self {
        self.max_tokens = Some(n);
        self
    }

    /// Set top-p nucleus sampling (0.0–1.0). Values outside this range are clamped.
    pub fn top_p(mut self, p: f32) -> Self {
        let clamped = p.clamp(0.0, 1.0);
        if (clamped - p).abs() > f32::EPSILON {
            warn!(
                requested = p,
                clamped = clamped,
                "top_p value {p} out of range [0.0, 1.0], clamped to {clamped}"
            );
        }
        self.top_p = Some(clamped);
        self
    }

    /// Add a stop sequence
    pub fn stop(mut self, s: impl Into<String>) -> Self {
        self.stop.push(s.into());
        self
    }

    /// Build the request
    pub fn build(self) -> ChatRequest {
        ChatRequest {
            messages: self.messages,
            model: self.model,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            top_p: self.top_p,
            stop: self.stop,
        }
    }
}

/// Reason why the model stopped generating
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// Model completed naturally
    Stop,
    /// Hit max_tokens limit
    Length,
    /// Content was filtered by safety systems
    ContentFilter,
    /// Model made a tool/function call
    ToolCalls,
}

impl std::fmt::Display for FinishReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FinishReason::Stop => write!(f, "stop"),
            FinishReason::Length => write!(f, "length"),
            FinishReason::ContentFilter => write!(f, "content_filter"),
            FinishReason::ToolCalls => write!(f, "tool_calls"),
        }
    }
}

/// A single choice in a chat completion response
#[derive(Debug, Clone)]
pub struct ChatChoice {
    /// Index of this choice (for n>1 requests)
    pub index: u32,
    /// The generated message
    pub message: ChatMessage,
    /// Why the model stopped
    pub finish_reason: FinishReason,
}

/// Token usage statistics
#[derive(Debug, Clone, Copy, Default)]
pub struct Usage {
    /// Tokens in the prompt
    pub prompt_tokens: u32,
    /// Tokens in the completion
    pub completion_tokens: u32,
    /// Total tokens consumed
    pub total_tokens: u32,
}

/// Response from a chat completion
#[derive(Debug, Clone)]
pub struct ChatResponse {
    /// Provider-assigned response ID
    pub id: String,
    /// Model that generated the response
    pub model: String,
    /// Generated choices
    pub choices: Vec<ChatChoice>,
    /// Token usage
    pub usage: Usage,
}

impl ChatResponse {
    /// Get the content of the first choice (convenience)
    pub fn content(&self) -> &str {
        self.choices
            .first()
            .map(|c| c.message.content.as_str())
            .unwrap_or("")
    }

    /// Get the finish reason of the first choice
    pub fn finish_reason(&self) -> Option<FinishReason> {
        self.choices.first().map(|c| c.finish_reason)
    }
}

// ============================================================================
// Embedding Types
// ============================================================================

/// Request for text embeddings
#[derive(Debug, Clone)]
pub struct EmbeddingRequest {
    /// Input texts to embed
    pub input: Vec<String>,
    /// Model override (uses provider default if None)
    pub model: Option<String>,
    /// Number of dimensions (if the model supports it)
    pub dimensions: Option<u32>,
    /// Input type hint (e.g. `"search_document"` or `"search_query"` for Cohere)
    ///
    /// Providers that don't support this field ignore it.
    pub input_type: Option<String>,
}

impl EmbeddingRequest {
    /// Create a builder for `EmbeddingRequest`
    pub fn builder() -> EmbeddingRequestBuilder {
        EmbeddingRequestBuilder::default()
    }

    /// Quick single-text embedding
    pub fn single(text: impl Into<String>) -> Self {
        Self {
            input: vec![text.into()],
            model: None,
            dimensions: None,
            input_type: None,
        }
    }

    /// Quick batch embedding
    pub fn batch(texts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            input: texts.into_iter().map(Into::into).collect(),
            model: None,
            dimensions: None,
            input_type: None,
        }
    }
}

/// Builder for `EmbeddingRequest`
#[derive(Debug, Default)]
pub struct EmbeddingRequestBuilder {
    input: Vec<String>,
    model: Option<String>,
    dimensions: Option<u32>,
    input_type: Option<String>,
}

impl EmbeddingRequestBuilder {
    /// Add a text input
    pub fn input(mut self, text: impl Into<String>) -> Self {
        self.input.push(text.into());
        self
    }

    /// Add multiple text inputs
    pub fn inputs(mut self, texts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.input.extend(texts.into_iter().map(Into::into));
        self
    }

    /// Override model
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Set embedding dimensions
    pub fn dimensions(mut self, d: u32) -> Self {
        self.dimensions = Some(d);
        self
    }

    /// Set input type hint (e.g. `"search_document"`, `"search_query"`)
    pub fn input_type(mut self, t: impl Into<String>) -> Self {
        self.input_type = Some(t.into());
        self
    }

    /// Build the request
    pub fn build(self) -> EmbeddingRequest {
        EmbeddingRequest {
            input: self.input,
            model: self.model,
            dimensions: self.dimensions,
            input_type: self.input_type,
        }
    }
}

/// A single embedding vector
#[derive(Debug, Clone)]
pub struct Embedding {
    /// Index of this embedding in the batch
    pub index: u32,
    /// The embedding vector (f32 values)
    pub values: Vec<f32>,
}

impl Embedding {
    /// Dimensionality of the embedding
    pub fn dimensions(&self) -> usize {
        self.values.len()
    }
}

/// Token usage for embedding requests
#[derive(Debug, Clone, Copy, Default)]
pub struct EmbeddingUsage {
    /// Total tokens processed
    pub prompt_tokens: u32,
    /// Total tokens (same as prompt_tokens for embeddings)
    pub total_tokens: u32,
}

/// Response from an embedding request
#[derive(Debug, Clone)]
pub struct EmbeddingResponse {
    /// Model that generated the embeddings
    pub model: String,
    /// The embedding vectors (one per input text)
    pub embeddings: Vec<Embedding>,
    /// Token usage
    pub usage: EmbeddingUsage,
}

impl EmbeddingResponse {
    /// Get the first embedding vector (convenience for single-text requests)
    pub fn first_embedding(&self) -> Option<&[f32]> {
        self.embeddings.first().map(|e| e.values.as_slice())
    }

    /// Number of embeddings
    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    /// Whether the response is empty
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chat_message_constructors() {
        let sys = ChatMessage::system("You are helpful.");
        assert_eq!(sys.role, Role::System);
        assert_eq!(sys.content, "You are helpful.");

        let usr = ChatMessage::user("Hello");
        assert_eq!(usr.role, Role::User);

        let ast = ChatMessage::assistant("Hi there!");
        assert_eq!(ast.role, Role::Assistant);
    }

    #[test]
    fn test_chat_request_builder() {
        let req = ChatRequest::builder()
            .system("Be concise.")
            .user("What is Rust?")
            .temperature(0.5)
            .max_tokens(100)
            .model("gpt-4o")
            .build();

        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].role, Role::System);
        assert_eq!(req.messages[1].role, Role::User);
        assert_eq!(req.temperature, Some(0.5));
        assert_eq!(req.max_tokens, Some(100));
        assert_eq!(req.model.as_deref(), Some("gpt-4o"));
    }

    #[test]
    fn test_chat_request_prompt() {
        let req = ChatRequest::prompt("Hello");
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::User);
        assert_eq!(req.messages[0].content, "Hello");
    }

    #[test]
    fn test_chat_request_with_system() {
        let req = ChatRequest::with_system("Be brief.", "Hi");
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].role, Role::System);
    }

    #[test]
    fn test_temperature_clamping() {
        let req = ChatRequest::builder().temperature(5.0).build();
        assert_eq!(req.temperature, Some(2.0));

        let req = ChatRequest::builder().temperature(-1.0).build();
        assert_eq!(req.temperature, Some(0.0));
    }

    #[test]
    fn test_top_p_clamping() {
        let req = ChatRequest::builder().top_p(1.5).build();
        assert_eq!(req.top_p, Some(1.0));
    }

    #[test]
    fn test_chat_response_content() {
        let resp = ChatResponse {
            id: "test".to_string(),
            model: "gpt-4o".to_string(),
            choices: vec![ChatChoice {
                index: 0,
                message: ChatMessage::assistant("Hello!"),
                finish_reason: FinishReason::Stop,
            }],
            usage: Usage {
                prompt_tokens: 5,
                completion_tokens: 1,
                total_tokens: 6,
            },
        };
        assert_eq!(resp.content(), "Hello!");
        assert_eq!(resp.finish_reason(), Some(FinishReason::Stop));
    }

    #[test]
    fn test_chat_response_empty() {
        let resp = ChatResponse {
            id: "test".to_string(),
            model: "test".to_string(),
            choices: vec![],
            usage: Usage::default(),
        };
        assert_eq!(resp.content(), "");
        assert_eq!(resp.finish_reason(), None);
    }

    #[test]
    fn test_embedding_request_single() {
        let req = EmbeddingRequest::single("Hello world");
        assert_eq!(req.input.len(), 1);
        assert_eq!(req.input[0], "Hello world");
    }

    #[test]
    fn test_embedding_request_batch() {
        let req = EmbeddingRequest::batch(["one", "two", "three"]);
        assert_eq!(req.input.len(), 3);
    }

    #[test]
    fn test_embedding_request_builder() {
        let req = EmbeddingRequest::builder()
            .input("hello")
            .input("world")
            .model("text-embedding-3-small")
            .dimensions(256)
            .build();
        assert_eq!(req.input.len(), 2);
        assert_eq!(req.model.as_deref(), Some("text-embedding-3-small"));
        assert_eq!(req.dimensions, Some(256));
        assert!(req.input_type.is_none());
    }

    #[test]
    fn test_embedding_request_builder_with_input_type() {
        let req = EmbeddingRequest::builder()
            .input("query")
            .model("cohere.embed-english-v3")
            .input_type("search_query")
            .build();
        assert_eq!(req.input_type.as_deref(), Some("search_query"));
    }

    #[test]
    fn test_embedding_response_first() {
        let resp = EmbeddingResponse {
            model: "test".to_string(),
            embeddings: vec![Embedding {
                index: 0,
                values: vec![0.1, 0.2, 0.3],
            }],
            usage: EmbeddingUsage {
                prompt_tokens: 2,
                total_tokens: 2,
            },
        };
        assert_eq!(resp.first_embedding(), Some([0.1, 0.2, 0.3].as_slice()));
        assert_eq!(resp.len(), 1);
        assert!(!resp.is_empty());
        assert_eq!(resp.embeddings[0].dimensions(), 3);
    }

    #[test]
    fn test_embedding_response_empty() {
        let resp = EmbeddingResponse {
            model: "test".to_string(),
            embeddings: vec![],
            usage: EmbeddingUsage::default(),
        };
        assert!(resp.is_empty());
        assert_eq!(resp.first_embedding(), None);
    }

    #[test]
    fn test_role_display() {
        assert_eq!(Role::System.to_string(), "system");
        assert_eq!(Role::User.to_string(), "user");
        assert_eq!(Role::Assistant.to_string(), "assistant");
        assert_eq!(Role::Tool.to_string(), "tool");
    }

    #[test]
    fn test_finish_reason_display() {
        assert_eq!(FinishReason::Stop.to_string(), "stop");
        assert_eq!(FinishReason::Length.to_string(), "length");
        assert_eq!(FinishReason::ContentFilter.to_string(), "content_filter");
        assert_eq!(FinishReason::ToolCalls.to_string(), "tool_calls");
    }

    #[test]
    fn test_role_serde_roundtrip() {
        let json = serde_json::to_string(&Role::System).unwrap();
        assert_eq!(json, r#""system""#);
        let back: Role = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Role::System);
    }

    #[test]
    fn test_finish_reason_serde_roundtrip() {
        let json = serde_json::to_string(&FinishReason::ContentFilter).unwrap();
        assert_eq!(json, r#""content_filter""#);
        let back: FinishReason = serde_json::from_str(&json).unwrap();
        assert_eq!(back, FinishReason::ContentFilter);
    }

    #[test]
    fn test_usage_default() {
        let u = Usage::default();
        assert_eq!(u.prompt_tokens, 0);
        assert_eq!(u.completion_tokens, 0);
        assert_eq!(u.total_tokens, 0);
    }
}

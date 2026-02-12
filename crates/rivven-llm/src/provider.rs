//! LLM provider trait — the core abstraction
//!
//! All providers (OpenAI, Bedrock, etc.) implement this trait.

use crate::error::LlmResult;
use crate::types::{ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse};
use async_trait::async_trait;

/// Capabilities advertised by a provider
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// Supports chat completions
    pub chat: bool,
    /// Supports text embeddings
    pub embeddings: bool,
}

/// Provider-agnostic LLM interface
///
/// Implementations handle authentication, HTTP transport, request serialization,
/// response parsing, and error mapping for a specific cloud provider.
///
/// # Example
///
/// ```rust,no_run
/// use rivven_llm::{LlmProvider, ChatRequest};
///
/// async fn summarize(provider: &dyn LlmProvider, text: &str) -> String {
///     let req = ChatRequest::with_system(
///         "Summarize concisely.",
///         text,
///     );
///     let resp = provider.chat(&req).await.unwrap();
///     resp.content().to_string()
/// }
/// ```
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Provider name (e.g. "openai", "bedrock")
    fn name(&self) -> &str;

    /// Default model for this provider instance
    fn default_model(&self) -> &str;

    /// Capabilities of this provider
    fn capabilities(&self) -> ProviderCapabilities;

    /// Perform a chat completion
    ///
    /// Returns `LlmError::Config` if the provider doesn't support chat.
    async fn chat(&self, request: &ChatRequest) -> LlmResult<ChatResponse>;

    /// Generate text embeddings
    ///
    /// Returns `LlmError::Config` if the provider doesn't support embeddings.
    async fn embed(&self, request: &EmbeddingRequest) -> LlmResult<EmbeddingResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_capabilities() {
        let caps = ProviderCapabilities {
            chat: true,
            embeddings: false,
        };
        assert!(caps.chat);
        assert!(!caps.embeddings);
    }
}

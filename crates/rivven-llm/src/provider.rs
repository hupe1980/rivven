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
/// # Security — Prompt Injection
///
/// **Callers must sanitize untrusted input before including it in chat messages.**
///
/// If user-supplied or externally-sourced text is passed directly as message
/// content, an attacker can craft inputs that override the system prompt,
/// extract sensitive context, or cause the model to produce harmful output.
///
/// Recommended mitigations:
/// - Clearly separate system instructions from user content using distinct
///   `ChatMessage` roles (`System` vs `User`).
/// - Validate and sanitize external inputs before embedding them in prompts.
/// - Apply output filtering on model responses when they are displayed to
///   end users or used to drive actions (tool calls, code execution, etc.).
/// - Consider using allow-lists or structured extraction rather than free-form
///   LLM output for safety-critical decisions.
///
/// This crate intentionally does **not** perform automatic prompt sanitization
/// because effective mitigation is application-specific.
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

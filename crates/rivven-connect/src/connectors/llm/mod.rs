//! LLM transform connectors — chat and embedding transforms via rivven-llm
//!
//! These transforms enrich events using Large Language Model providers.
//!
//! - **`llm-chat`** — Send event data through an LLM for summarization, classification,
//!   extraction, or generation. Adds the response to a configurable output field.
//! - **`llm-embedding`** — Generate vector embeddings from text fields. Perfect for
//!   feeding into vector database sinks (Qdrant, Pinecone).
//!
//! # Feature Gates
//!
//! - `llm-openai` — OpenAI provider (GPT-4o, text-embedding-3-small, etc.)
//! - `llm-bedrock` — AWS Bedrock provider (Claude 3, Titan Embed, etc.)
//! - `llm-full` — All providers

pub mod chat;
pub mod embedding;

pub use chat::{LlmChatTransform, LlmChatTransformConfig, LlmChatTransformFactory};
pub use embedding::{
    LlmEmbeddingTransform, LlmEmbeddingTransformConfig, LlmEmbeddingTransformFactory,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ============================================================================
// Shared types
// ============================================================================

/// LLM provider selection — shared by chat and embedding transforms
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LlmProviderType {
    /// OpenAI API (also supports Azure OpenAI, Ollama, vLLM)
    #[default]
    OpenAi,
    /// AWS Bedrock (Claude, Titan, Mistral, Llama)
    Bedrock,
}

impl std::fmt::Display for LlmProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmProviderType::OpenAi => write!(f, "openai"),
            LlmProviderType::Bedrock => write!(f, "bedrock"),
        }
    }
}

// ============================================================================
// Shared utilities
// ============================================================================

/// Resolve a potentially nested field path (e.g., `"address.city"`) from JSON data.
///
/// Returns `None` if any segment in the path is missing.
pub(crate) fn resolve_field<'a>(
    data: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_type_display() {
        assert_eq!(LlmProviderType::OpenAi.to_string(), "openai");
        assert_eq!(LlmProviderType::Bedrock.to_string(), "bedrock");
    }

    #[test]
    fn test_provider_type_default() {
        assert_eq!(LlmProviderType::default(), LlmProviderType::OpenAi);
    }

    #[test]
    fn test_provider_type_serde_roundtrip() {
        let json = serde_json::to_string(&LlmProviderType::Bedrock).unwrap();
        assert_eq!(json, r#""bedrock""#);
        let back: LlmProviderType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, LlmProviderType::Bedrock);
    }

    #[test]
    fn test_resolve_field_simple() {
        let data = serde_json::json!({"name": "test"});
        assert_eq!(
            resolve_field(&data, "name").unwrap(),
            &serde_json::json!("test")
        );
    }

    #[test]
    fn test_resolve_field_nested() {
        let data = serde_json::json!({"a": {"b": {"c": 42}}});
        assert_eq!(
            resolve_field(&data, "a.b.c").unwrap(),
            &serde_json::json!(42)
        );
    }

    #[test]
    fn test_resolve_field_missing() {
        let data = serde_json::json!({"a": 1});
        assert!(resolve_field(&data, "b").is_none());
    }

    #[test]
    fn test_resolve_field_nested_missing() {
        let data = serde_json::json!({"a": {"b": 1}});
        assert!(resolve_field(&data, "a.c").is_none());
    }
}

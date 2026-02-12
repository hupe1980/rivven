//! # rivven-llm — LLM Provider Facade
//!
//! Unified async API for Large Language Model providers.
//!
//! This crate provides a provider-agnostic interface for:
//! - **Chat completions** — send messages, get structured responses
//! - **Text embeddings** — generate vector representations of text
//!
//! ## Supported Providers
//!
//! | Provider | Feature | Chat | Embeddings |
//! |:---------|:--------|:-----|:-----------|
//! | OpenAI   | `openai` (default) | ✓ | ✓ |
//! | AWS Bedrock | `bedrock` | ✓ | ✓ |
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rivven_llm::{LlmProvider, ChatRequest, ChatMessage, Role};
//! use rivven_llm::openai::OpenAiProvider;
//!
//! # async fn example() -> Result<(), rivven_llm::LlmError> {
//! let provider = OpenAiProvider::builder()
//!     .api_key("sk-...")
//!     .model("gpt-4o-mini")
//!     .build()?;
//!
//! let request = ChatRequest::builder()
//!     .message(ChatMessage::user("Summarize this text: ..."))
//!     .temperature(0.3)
//!     .max_tokens(256)
//!     .build();
//!
//! let response = provider.chat(&request).await?;
//! println!("{}", response.content());
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod provider;
pub mod types;

#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "bedrock")]
pub mod bedrock;

// Re-export core types at crate root
pub use error::{LlmError, LlmResult};
pub use provider::LlmProvider;
pub use types::{
    ChatChoice, ChatMessage, ChatRequest, ChatRequestBuilder, ChatResponse, Embedding,
    EmbeddingRequest, EmbeddingRequestBuilder, EmbeddingResponse, EmbeddingUsage, FinishReason,
    Role, Usage,
};

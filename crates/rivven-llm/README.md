# rivven-llm

LLM provider facade for Rivven — unified async API for Large Language Model providers.

## Features

| Provider | Feature | Chat | Embeddings |
|:---------|:--------|:-----|:-----------|
| OpenAI | `openai` (default) | ✓ | ✓ |
| AWS Bedrock | `bedrock` | ✓ | ✓ |

Feature gates are additive. The `openai` feature pulls in `reqwest` + `secrecy`;
the `bedrock` feature pulls in `aws-config` + `aws-sdk-bedrockruntime`.
Enabling only one provider avoids compiling the other's dependency tree.

## Quick Start

```rust
use rivven_llm::{LlmProvider, ChatRequest, ChatMessage, Role};
use rivven_llm::openai::OpenAiProvider;

let provider = OpenAiProvider::builder()
    .api_key("sk-...")
    .model("gpt-4o-mini")
    .build()?;

let request = ChatRequest::builder()
    .system("Be concise.")
    .user("What is Rust?")
    .temperature(0.3)
    .max_tokens(256)
    .build();

let response = provider.chat(&request).await?;
println!("{}", response.content());
```

## Embeddings

```rust
use rivven_llm::{LlmProvider, EmbeddingRequest};
use rivven_llm::openai::OpenAiProvider;

let provider = OpenAiProvider::builder()
    .api_key("sk-...")
    .embedding_model("text-embedding-3-small")
    .build()?;

let request = EmbeddingRequest::single("Hello world");
let response = provider.embed(&request).await?;
let vector = response.first_embedding().unwrap();
println!("Dimensions: {}", vector.len());
```

## AWS Bedrock

```rust
use rivven_llm::bedrock::BedrockProvider;

let provider = BedrockProvider::builder()
    .region("us-east-1")
    .chat_model("anthropic.claude-3-haiku-20240307-v1:0")
    .build()
    .await?;
```

Credentials are resolved from the standard AWS credential chain (env vars, profiles, IMDS, ECS).
Uses the official `aws-sdk-bedrockruntime` — SigV4 signing, credential refresh, and retries are handled automatically by the SDK.

**Concurrent embeddings**: Bedrock embedding requests are dispatched concurrently, enabling high-throughput vector generation for batch workloads.

## HTTP Transport Security

All HTTP-based LLM providers (OpenAI) require HTTPS by default. To connect to an HTTP endpoint (e.g., a local proxy or development server), set `allow_insecure: true` in the provider configuration. This opt-in prevents accidental credential transmission over unencrypted connections.

## Error Handling

All errors return `LlmError` with helpful classification:

```rust
match provider.chat(&request).await {
    Ok(response) => println!("{}", response.content()),
    Err(e) if e.is_retryable() => {
        // Rate-limited, timeout, connection, transient — retry with backoff
        if let Some(secs) = e.retry_after_secs() {
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
    }
    Err(e) if e.is_auth() => panic!("check credentials: {e}"),
    Err(e) => eprintln!("permanent error: {e}"),
}
```

**Error variants:** `Config`, `Auth`, `RateLimited` (with optional `retry_after_secs`), `Timeout`, `ModelNotFound`, `ContentFiltered`, `TokenLimitExceeded`, `Provider`, `Connection`, `Serialization`, `Transient`, `Internal`.

## Security

- **API keys** stored with `secrecy` — never appear in Debug/Display output
- **Error messages** truncated to prevent log amplification from malicious API responses
- **UTF-8 safe** truncation — never panics on multi-byte characters
- **Retry-After** headers parsed from 429 responses (OpenAI)
- **SigV4 signing** for Bedrock — handled automatically by the official AWS SDK; no credentials in request URLs
- **Transport error classification** — SDK-level timeouts, network failures, and construction errors are mapped to correct `LlmError` variants (not lost as opaque internal errors)
- **Capability checks** — providers declare chat/embedding support; callers get clear errors
- **Minimal dependency surface** — `reqwest`/`secrecy` gated on `openai`; AWS SDK gated on `bedrock`

## Architecture

```text
┌─────────────────────────────────────────┐
│            rivven-llm facade            │
│                                         │
│  ┌──────────────────────────────────┐   │
│  │      LlmProvider trait           │   │
│  │  chat() → ChatResponse          │   │
│  │  embed() → EmbeddingResponse    │   │
│  └──────────────────────────────────┘   │
│         ▲              ▲                │
│         │              │                │
│  ┌──────┴──┐    ┌──────┴──────┐         │
│  │ OpenAI  │    │  Bedrock    │         │
│  │ reqwest │    │  aws-sdk-   │         │
│  │  REST   │    │ bedrockrt   │         │
│  └─────────┘    └─────────────┘         │
│  (openai feat)  (bedrock feat)          │
└─────────────────────────────────────────┘
```

## License

Apache-2.0. See [LICENSE](../../LICENSE).

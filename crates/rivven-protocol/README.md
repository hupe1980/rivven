# rivven-protocol

> Wire protocol types for the Rivven distributed event streaming platform.

## Overview

This crate defines canonical wire protocol types for client-server communication. Both `rivven-client` and `rivvend` depend on this crate to ensure wire compatibility.

## Features

| Category | Types |
|:---------|:------|
| **Requests** | Produce, Consume, CreateTopic, DeleteTopic, Metadata, etc. |
| **Responses** | ProduceResponse, ConsumeResponse, MetadataResponse, etc. |
| **Messages** | `MessageData` with key, value, headers, timestamp |
| **Metadata** | `BrokerInfo`, `TopicMetadata`, `PartitionMetadata` |

## Protocol Constants

| Constant | Value | Description |
|:---------|:------|:------------|
| `PROTOCOL_VERSION` | 1 | Current wire protocol version |
| `MAX_MESSAGE_SIZE` | 64 MB | Maximum serialized message size |

## Usage

```rust
use rivven_protocol::{Request, Response, MessageData};

// Create a produce request
let data = MessageData::new(b"Hello, Rivven!".to_vec());
let request = Request::Produce {
    topic: "events".to_string(),
    partition: Some(0),
    data: vec![data],
};

// Serialize for transmission
let bytes = request.to_bytes()?;

// Deserialize on receipt
let request = Request::from_bytes(&bytes)?;
```

## Wire Format

Messages are serialized using [postcard](https://docs.rs/postcard/) with the following guarantees:

- Compact binary encoding with COBS framing support
- Variable-length integer encoding (varint)
- `#[no_std]` compatible
- Maximum message size enforced before deserialization

## License

Apache-2.0. See [LICENSE](../../LICENSE).

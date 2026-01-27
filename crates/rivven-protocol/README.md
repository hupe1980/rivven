# rivven-protocol

Wire protocol types for Rivven, the distributed event streaming platform.

## Overview

This crate provides the canonical definitions for all wire protocol types used in client-server communication. Both `rivven-client` and `rivven-server` depend on this crate to ensure wire compatibility.

## Features

- **Request/Response enums** — All 20+ request and response types
- **Metadata types** — `BrokerInfo`, `TopicMetadata`, `PartitionMetadata`
- **Message types** — `MessageData` with key/value/headers
- **Serialization** — postcard-based wire format with size validation

## Protocol Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `PROTOCOL_VERSION` | 1 | Current wire protocol version |
| `MAX_MESSAGE_SIZE` | 64MB | Maximum serialized message size |

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
- #[no_std] compatible
- Maximum message size enforced before deserialization

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.

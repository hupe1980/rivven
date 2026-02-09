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
use rivven_protocol::{Request, Response, WireFormat};

// Create a produce request
let request = Request::Publish {
    topic: "events".to_string(),
    partition: Some(0),
    key: None,
    value: bytes::Bytes::from_static(b"Hello, Rivven!"),
};

// Serialize with wire format byte prefix (recommended)
let wire_bytes = request.to_wire(WireFormat::Postcard)?;

// Deserialize with auto-detection
let (request, format) = Request::from_wire(&wire_bytes)?;
assert_eq!(format, WireFormat::Postcard);
```

## Wire Format

Messages use a **format-prefixed** wire protocol:

```
┌─────────────────┬──────────────────┬─────────────────────────┐
│ Length (4 bytes)│ Format (1 byte)  │ Payload (N bytes)       │
│ Big-endian u32  │ 0x00 = postcard  │ Serialized Request/Resp │
│                 │ 0x01 = protobuf  │                         │
└─────────────────┴──────────────────┴─────────────────────────┘
```

### Format Byte Values

| Byte | Format | Description |
|:-----|:-------|:------------|
| `0x00` | postcard | Rust-native, fastest (~50ns serialize) |
| `0x01` | protobuf | Cross-language compatible |

### Serialization Methods

| Method | Description |
|:-------|:------------|
| `to_wire(format)` | Serialize with format byte prefix |
| `from_wire(data)` | Deserialize with auto-format detection |
| `to_bytes()` | Postcard only, no format prefix (legacy) |
| `from_bytes(data)` | Postcard only, no format prefix (legacy) |

### Server Behavior

The server auto-detects the wire format from the first byte and responds in the same format the client used.

### Protobuf Feature

Enable protobuf serialization with the `protobuf` feature:

```toml
[dependencies]
rivven-protocol = { version = "0.0.11", features = ["protobuf"] }
```

```rust
use rivven_protocol::{Request, WireFormat};

// Serialize request as protobuf
let request = Request::Ping;
let wire_bytes = request.to_wire(WireFormat::Protobuf)?;

// First byte is 0x01 (protobuf)
assert_eq!(wire_bytes[0], 0x01);
```

## Cross-Language Support

### Architecture

The Rivven broker uses **postcard** for maximum performance with Rust clients. For other languages, the protocol is documented via **Protocol Buffers**:

```
┌─────────────────┐                    ┌─────────────────┐
│   Rust Client   │───── postcard ────►│                 │
│ (rivven-client) │◄──── postcard ─────│     rivvend     │
└─────────────────┘                    │    (broker)     │
                                       └─────────────────┘
```

### Multi-Language Clients

For Go, Java, Python, and other languages, use the proto file as the **wire format specification**:

| Format | Use Case | Performance |
|:-------|:---------|:------------|
| **postcard** | Rust clients (native) | Fastest (~50ns serialize) |
| **protobuf** | Reference spec for other languages | Fast (~200ns serialize) |

### Protobuf Schema

The proto file at [`proto/rivven.proto`](proto/rivven.proto) documents the wire format. Client implementers specify their own package names:

```protobuf
package rivven.protocol.v1;

message PublishRequest {
  string topic = 1;
  optional uint32 partition = 2;
  Record record = 3;
}

message Record {
  bytes key = 1;
  bytes value = 2;
  repeated RecordHeader headers = 3;
  int64 timestamp = 4;
}
```

### Generate Client Stubs

```bash
# Go
protoc --go_out=. --go_opt=Mrivven.proto=github.com/yourorg/rivven-go/protocol proto/rivven.proto

# Java
protoc --java_out=. proto/rivven.proto

# Python
protoc --python_out=. proto/rivven.proto

# TypeScript (ts-proto)
protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto --ts_proto_out=. proto/rivven.proto
```

### Implementing a Client

To implement a client in another language:

1. Generate code from `proto/rivven.proto`
2. Connect to broker on port 9092
3. Use length-prefixed framing: `[4-byte length][1-byte format][payload]`
4. Use format byte `0x01` for protobuf
5. Serialize requests using protobuf
6. Parse responses using protobuf (server responds in same format)

### Proto Message Types

The protobuf schema defines 31+ message types covering all major protocol operations:

| Category | Message Types |
|:---------|:--------------|
| **Core** | `Request`, `Response`, `Record`, `ErrorCode` |
| **Publish/Consume** | `PublishRequest`, `BatchPublishRequest`, `ConsumeResponse` |
| **Metadata** | `GetMetadataResponse`, `GetClusterMetadataResponse`, `GetOffsetBoundsResponse` |
| **Consumer Groups** | `ListGroupsResponse`, `DescribeGroupResponse`, `DeleteGroupResponse` |
| **Transactions** | `BeginTransactionResponse`, `CommitTransactionResponse`, `AbortTransactionResponse` |
| **Idempotent** | `InitProducerIdResponse`, `IdempotentPublishResponse` |
| **Admin** | `AlterTopicConfigRequest`, `DescribeTopicConfigsResponse`, `CreatePartitionsRequest`, `DeleteRecordsRequest` |
| **Time Queries** | `GetOffsetForTimestampRequest` / `Response` |

## License

Apache-2.0. See [LICENSE](../../LICENSE).

# Schema Registry

Rivven Connect includes a built-in schema registry with comprehensive support for JSON Schema, Apache Avro, and Protocol Buffers.

## Overview

The schema registry provides:

- **Schema Evolution** — Track schema versions with compatibility checking
- **Multiple Formats** — Native support for JSON Schema, Avro, and Protobuf
- **Confluent Compatibility** — Wire format compatibility with Confluent Schema Registry
- **Two Deployment Modes** — Embedded (schemas in Rivven topics) or external (Confluent-compatible)
- **Schema Fingerprinting** — MD5 and SHA-256 fingerprints for caching and deduplication

## Supported Schema Formats

| Format | Status | Features |
|--------|--------|----------|
| **JSON Schema** | ✅ Full | Draft-07 support, validation, inference |
| **Apache Avro** | ✅ Full | Encoding/decoding, fingerprinting, Confluent wire format |
| **Protocol Buffers** | ✅ Full | Dynamic parsing, encoding/decoding, Confluent wire format |

## Deployment Modes

### Embedded Mode

Schemas are stored in a dedicated Rivven topic (`_schemas`). This mode requires no external dependencies and is ideal for single-cluster deployments.

```yaml
schema_registry:
  mode: embedded
  storage_topic: "_schemas"
  compatibility_level: backward
```

### External Mode

Connect to a Confluent-compatible schema registry for multi-cluster environments or when integrating with existing Kafka infrastructure.

```yaml
schema_registry:
  mode: external
  url: "http://schema-registry:8081"
  auth:
    username: "${SCHEMA_REGISTRY_USER}"
    password: "${SCHEMA_REGISTRY_PASSWORD}"
```

## Apache Avro

### Parsing Schemas

```rust
use rivven_connect::schema::{AvroSchema, AvroCodec};

let schema = AvroSchema::parse(r#"{
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["null", "string"], "default": null},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "metadata", "type": {"type": "map", "values": "string"}}
    ]
}"#)?;

// Access schema metadata
println!("Name: {}", schema.name().unwrap());           // "User"
println!("Namespace: {}", schema.namespace().unwrap()); // "com.example"
println!("Fullname: {}", schema.fullname().unwrap());   // "com.example.User"

// List fields
for field in schema.fields().unwrap() {
    println!("Field: {} ({})", field.name, field.schema_type);
}
```

### Encoding and Decoding

```rust
use rivven_connect::schema::AvroCodec;

let codec = AvroCodec::new(schema);

// Encode JSON to Avro binary
let json = serde_json::json!({
    "id": 12345,
    "name": "Alice",
    "email": "alice@example.com",
    "tags": ["admin", "developer"],
    "metadata": {"department": "engineering"}
});

let avro_bytes = codec.encode(&json)?;

// Decode Avro binary to JSON
let decoded = codec.decode(&avro_bytes)?;
assert_eq!(decoded["name"], "Alice");
```

### Confluent Wire Format

The Confluent wire format prefixes Avro data with a magic byte and 4-byte schema ID:

```
+----------+------------+------------------+
| Magic(1) | SchemaID(4)| Avro Data (N)    |
+----------+------------+------------------+
|   0x00   | Big-endian |  Avro binary     |
+----------+------------+------------------+
```

```rust
// Encode with schema ID
let schema_id: u32 = 12345;
let confluent_bytes = codec.encode_confluent(&json, schema_id)?;

// Decode and retrieve schema ID
let (schema_id, decoded) = codec.decode_confluent(&confluent_bytes)?;
println!("Schema ID: {}", schema_id);
```

### Schema Fingerprinting

Schema fingerprints are useful for caching and identifying schema versions:

```rust
let schema = AvroSchema::parse("...")?;

// MD5 fingerprint (16 bytes)
let md5 = schema.fingerprint_md5();
println!("MD5: {:02x?}", md5);

// SHA-256 fingerprint (32 bytes)
let sha256 = schema.fingerprint_sha256();
println!("SHA-256: {:02x?}", sha256);
```

### Compatibility Checking

```rust
use rivven_connect::schema::AvroCompatibility;

let old_schema = AvroSchema::parse(r#"{"type": "record", "name": "User", "fields": [
    {"name": "id", "type": "long"}
]}"#)?;

let new_schema = AvroSchema::parse(r#"{"type": "record", "name": "User", "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string", "default": ""}
]}"#)?;

// Backward compatibility: new readers can read old data
let backward = AvroCompatibility::check_backward(&new_schema, &old_schema)?;
assert!(backward); // ✅ Adding field with default is backward compatible

// Forward compatibility: old readers can read new data
let forward = AvroCompatibility::check_forward(&new_schema, &old_schema)?;

// Full compatibility: both directions
let full = AvroCompatibility::check_full(&new_schema, &old_schema)?;
```

#### Avro Compatibility Rules

| Change | Backward | Forward | Full |
|--------|----------|---------|------|
| Add field with default | ✅ | ✅ | ✅ |
| Add field without default | ❌ | ✅ | ❌ |
| Remove field with default | ✅ | ❌ | ❌ |
| Remove field without default | ✅ | ❌ | ❌ |
| Rename field | ❌ | ❌ | ❌ |
| Promote int → long | ✅ | ❌ | ❌ |
| Promote float → double | ✅ | ❌ | ❌ |

## Protocol Buffers

### Parsing Schemas

```rust
use rivven_connect::schema::{ProtobufSchema, ProtobufCodec};

let schema = ProtobufSchema::parse(r#"
    syntax = "proto3";
    package com.example;
    
    enum Status {
        UNKNOWN = 0;
        ACTIVE = 1;
        INACTIVE = 2;
    }
    
    message User {
        int64 id = 1;
        string name = 2;
        optional string email = 3;
        repeated string tags = 4;
        Status status = 5;
    }
"#)?;

// Access schema metadata
println!("Message: {}", schema.message_name());   // "User"
println!("Package: {:?}", schema.package());      // Some("com.example")
println!("Fullname: {}", schema.fullname());      // "com.example.User"

// List fields
for field in schema.fields()? {
    println!("Field: {} (#{}) - {:?}", 
        field.name, field.number, field.kind);
}
```

### Encoding and Decoding

```rust
let codec = ProtobufCodec::new(schema);

// Encode JSON to Protobuf binary
let json = serde_json::json!({
    "id": 12345,
    "name": "Alice",
    "email": "alice@example.com",
    "tags": ["admin", "developer"],
    "status": "ACTIVE"
});

let proto_bytes = codec.encode(&json)?;

// Decode Protobuf binary to JSON
let decoded = codec.decode(&proto_bytes)?;
assert_eq!(decoded["name"], "Alice");
assert_eq!(decoded["status"], "ACTIVE");
```

### Confluent Wire Format

The Confluent wire format for Protobuf includes a message index:

```
+----------+------------+--------------+------------------+
| Magic(1) | SchemaID(4)| MsgIndex(N)  | Protobuf Data(N) |
+----------+------------+--------------+------------------+
|   0x00   | Big-endian | Varint(0)    | Protobuf binary  |
+----------+------------+--------------+------------------+
```

```rust
// Encode with schema ID
let schema_id: u32 = 12345;
let confluent_bytes = codec.encode_confluent(&json, schema_id)?;

// Decode and retrieve schema ID
let (schema_id, decoded) = codec.decode_confluent(&confluent_bytes)?;
```

### Compatibility Checking

```rust
use rivven_connect::schema::ProtobufCompatibility;

let old_schema = ProtobufSchema::parse(r#"
    syntax = "proto3";
    message User { int64 id = 1; }
"#)?;

let new_schema = ProtobufSchema::parse(r#"
    syntax = "proto3";
    message User { int64 id = 1; string name = 2; }
"#)?;

// Adding new fields is compatible in proto3
let compatible = ProtobufCompatibility::check_full(&new_schema, &old_schema)?;
assert!(compatible); // ✅ New optional field is fully compatible
```

#### Protobuf Compatibility Rules

| Change | Backward | Forward | Full |
|--------|----------|---------|------|
| Add optional field | ✅ | ✅ | ✅ |
| Add repeated field | ✅ | ✅ | ✅ |
| Remove optional field | ✅ | ✅ | ✅ |
| Reuse field number | ❌ | ❌ | ❌ |
| Change field type | ❌ | ❌ | ❌ |
| Wire-compatible promotion* | ✅ | ✅ | ✅ |

*Wire-compatible type groups:
- **Varint**: int32, int64, uint32, uint64, bool
- **32-bit**: fixed32, sfixed32, float
- **64-bit**: fixed64, sfixed64, double

## Schema Evolution Strategies

### Compatibility Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| `backward` | New readers can read old data | Rolling upgrades (consumers first) |
| `forward` | Old readers can read new data | Rolling upgrades (producers first) |
| `full` | Both backward and forward | Maximum flexibility |
| `none` | No compatibility checking | Development only |

### Best Practices

1. **Always use defaults** for new fields in Avro
2. **Reserve field numbers** in Protobuf before removal
3. **Never reuse** field numbers or names
4. **Test compatibility** before deploying schema changes
5. **Use full compatibility** when possible for maximum safety

## Schema Inference

Automatically infer schemas from sample data:

```rust
use rivven_connect::schema::{SchemaInference, infer_schema_from_samples};

let samples = vec![
    serde_json::json!({"id": 1, "name": "Alice", "active": true}),
    serde_json::json!({"id": 2, "name": "Bob", "age": 30}),
    serde_json::json!({"id": 3, "name": "Carol", "active": false, "age": 25}),
];

let schema = infer_schema_from_samples(&samples)?;
// Produces JSON Schema with merged properties from all samples
```

## Integration Examples

### CDC with Avro Schemas

```yaml
connectors:
  - name: postgres-users
    type: postgres-cdc
    config:
      connection_string: "postgres://..."
      table: "users"
      schema_registry:
        mode: embedded
        format: avro
        compatibility: backward
```

### Kafka Bridge with Protobuf

```yaml
connectors:
  - name: kafka-orders
    type: kafka-source
    config:
      brokers: ["kafka:9092"]
      topic: "orders"
      schema_registry:
        mode: external
        url: "http://schema-registry:8081"
        format: protobuf
```

## API Reference

### AvroSchema

| Method | Description |
|--------|-------------|
| `parse(json: &str)` | Parse Avro schema from JSON |
| `name()` | Get record name |
| `namespace()` | Get namespace |
| `fullname()` | Get fully qualified name |
| `fields()` | List record fields |
| `fingerprint_md5()` | Get MD5 fingerprint |
| `fingerprint_sha256()` | Get SHA-256 fingerprint |

### AvroCodec

| Method | Description |
|--------|-------------|
| `new(schema)` | Create codec for schema |
| `encode(json)` | Encode JSON to Avro bytes |
| `decode(bytes)` | Decode Avro bytes to JSON |
| `encode_confluent(json, id)` | Encode with Confluent wire format |
| `decode_confluent(bytes)` | Decode Confluent wire format |

### ProtobufSchema

| Method | Description |
|--------|-------------|
| `parse(proto: &str)` | Parse .proto file content |
| `message_name()` | Get main message name |
| `package()` | Get package name |
| `fullname()` | Get fully qualified name |
| `fields()` | List message fields |
| `fingerprint_md5()` | Get MD5 fingerprint |

### ProtobufCodec

| Method | Description |
|--------|-------------|
| `new(schema)` | Create codec for schema |
| `encode(json)` | Encode JSON to Protobuf bytes |
| `decode(bytes)` | Decode Protobuf bytes to JSON |
| `encode_confluent(json, id)` | Encode with Confluent wire format |
| `decode_confluent(bytes)` | Decode Confluent wire format |

---

*For more information, see the [Architecture](architecture.md) and [Connector Development](connector-development.md) guides.*

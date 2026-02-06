---
layout: default
title: Schema Registry
nav_order: 16
---

# Schema Registry
{: .no_toc }

Schema management with JSON Schema, Avro, and Protobuf support.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven follows a **schema-agnostic broker** architecture where the broker handles only raw bytes. Schema management is handled by the dedicated **rivven-schema** crate, which provides:

1. **rivven-schema** — Standalone Schema Registry (lib + binary) with industry-standard REST API
2. **rivven-connect** — Schema integration for connectors (serialization/deserialization)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 Schema Registry Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Producer                                                       │
│      │                                                           │
│      ├──► Schema Registry (rivven-schema) ──► Register Schema   │
│      │          │                                                │
│      │          ▼                                                │
│      │    Get Schema ID                                          │
│      │          │                                                │
│      ├──► Serialize (Avro/Protobuf/JSON)                        │
│      │          │                                                │
│      ▼          ▼                                                │
│   ┌─────────────────────────────────────────────────────┐       │
│   │              Broker (rivvend)                        │       │
│   │        Raw bytes only - schema agnostic             │       │
│   │     Topics, Partitions, Messages, Offsets           │       │
│   └─────────────────────────────────────────────────────┘       │
│      │                                                           │
│      ▼                                                           │
│   Consumer                                                       │
│      │                                                           │
│      ├──► Extract Schema ID from message header                 │
│      ├──► Schema Registry (rivven-schema) ──► Get Schema        │
│      └──► Deserialize (Avro/Protobuf/JSON)                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- **Broker is schema-agnostic** — only handles raw bytes, no schema processing
- **rivven-schema** is the **single source of truth** for schema storage, versioning, and compatibility
- **rivven-connect** handles serialization/deserialization with schema registry integration
- **Standard wire format** — Compatible with common ecosystem tools

## Crate Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   rivven-schema (Schema Registry)                │
│  ├── Types: SchemaId, SchemaType, Subject, SchemaVersion        │
│  ├── Compatibility: CompatibilityLevel, CompatibilityChecker    │
│  ├── Fingerprinting: MD5, SHA-256 for deduplication             │
│  ├── Storage: MemoryStorage                                     │
│  └── Server: Industry-standard REST API                        │
├─────────────────────────────────────────────────────────────────┤
│                      rivven-connect (Connectors)                 │
│  ├── Re-exports from rivven-schema: SchemaId, SchemaType, etc.  │
│  ├── Codecs: AvroCodec, ProtobufCodec, JsonCodec (wire format)  │
│  ├── Config: ExternalConfig, GlueConfig, Disabled               │
│  └── Clients: ExternalRegistry, GlueRegistry                    │
├─────────────────────────────────────────────────────────────────┤
│                        rivvend (Broker)                          │
│  └── Schema-agnostic: only handles raw bytes                    │
└─────────────────────────────────────────────────────────────────┘
```

## Deployment Modes

Rivven supports **3 schema modes** for maximum flexibility:

| Mode | Component | Description | Use Case |
|------|-----------|-------------|----------|
| **External** | rivven-connect | Connect to an external Schema Registry (including rivven-schema) | Production, multi-cluster |
| **External (AWS Glue)** | rivven-connect | Connect to AWS Glue Schema Registry | AWS-native deployments |
| **Disabled** | rivven-connect | JSON events without schemas | Development, simple use cases |

The schema registry provides:

- **Schema Evolution** — Track schema versions with compatibility checking
- **Multiple Formats** — Native support for JSON Schema, Avro, and Protobuf
- **Industry-Standard API** — Wire format and REST API compatibility
- **Schema Fingerprinting** — MD5 and SHA-256 fingerprints for caching and deduplication

## Standalone Registry (rivven-schema)

The `rivven-schema` crate provides a standalone Schema Registry for schema storage, versioning, and compatibility checking.

> **Note**: The registry stores and validates schemas. It does **not** encode/decode data.
> Use `rivven-connect` for Avro/Protobuf/JSON codecs.

### Quick Start

```rust
use rivven_schema::{SchemaRegistry, RegistryConfig, SchemaType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create an in-memory registry
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await?;

    // Register an Avro schema
    let avro_schema = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;
    
    let schema_id = registry.register("user-value", SchemaType::Avro, avro_schema).await?;
    println!("Registered schema with ID: {}", schema_id.0);

    // Retrieve the schema
    let schema = registry.get_by_id(schema_id).await?;
    println!("Schema: {}", schema.schema);

    // Check compatibility for schema evolution
    let new_schema = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }"#;
    let result = registry.check_compatibility("user-value", SchemaType::Avro, new_schema, None).await?;
    println!("Compatible: {}", result.is_compatible);

    Ok(())
}
```

### Standalone Server

```bash
# Start with in-memory storage
rivven-schema serve --port 8081
```

### CLI Commands

```bash
# Check server health
rivven-schema health --url http://localhost:8081

# Register a schema
rivven-schema register --url http://localhost:8081 --subject user-value --schema schema.avsc

# Get a schema by ID
rivven-schema get --url http://localhost:8081 --id 1

# List all subjects
rivven-schema subjects --url http://localhost:8081

# Check compatibility
rivven-schema compat --url http://localhost:8081 --subject user-value --schema new.avsc
```

## Authentication & Security

The standalone Schema Registry (`rivven-schema`) supports enterprise-grade authentication methods.

### Authentication Methods

| Method | Header | Feature Flag | Use Case |
|--------|--------|--------------|----------|
| **HTTP Basic Auth** | `Authorization: Basic base64(user:pass)` | `auth` | Simple deployments |
| **Bearer Token** | `Authorization: Bearer <session-id>` | `auth` | Session-based auth |
| **JWT/OIDC** | `Authorization: Bearer <jwt>` | `jwt` | Enterprise SSO |
| **API Keys** | `X-API-Key: <key>` | `auth` | Service-to-service auth |

### Enable Authentication

```bash
# Basic auth + Bearer token + API Keys
cargo build -p rivven-schema --features auth

# JWT/OIDC support (includes auth)
cargo build -p rivven-schema --features jwt
```

### JWT/OIDC Configuration

```yaml
auth:
  enabled: true
  jwt:
    # Token validation
    secret: "${JWT_SECRET}"           # For HS256
    # Or RSA public key for RS256
    rsa_public_key_path: /etc/rivven/jwt-public.pem
    # Or JWKS endpoint for key rotation
    jwks_url: "https://auth.example.com/.well-known/jwks.json"
    
    # Validation options
    issuer: "https://auth.example.com"
    audience: "rivven-schema"
    algorithms: ["RS256", "ES256"]
```

### API Key Configuration

```yaml
auth:
  enabled: true
  api_keys:
    - key: "sk_prod_abc123..."
      principal: "service-account-1"
      description: "Production connector"
    - key: "sk_prod_xyz789..."
      principal: "service-account-2"
      description: "Analytics pipeline"
```

### RBAC Integration

Authentication integrates with rivven-core's RBAC system:

```yaml
auth:
  enabled: true
  allow_anonymous_read: false  # Require auth for all operations
  
  # Per-subject permissions
  acls:
    - principal: "team-a"
      subjects: ["team-a-*"]
      permissions: [read, write, delete]
    - principal: "readonly-service"
      subjects: ["*"]
      permissions: [read]
```

### Cedar Policy-Based Authorization

For fine-grained, policy-as-code authorization:

```bash
cargo build -p rivven-schema --features cedar
```

```cedar
// Allow schema admins full access
permit(
  principal in Rivven::Group::"schema-admins",
  action,
  resource is Rivven::Schema
);

// Allow teams to manage their own schemas
permit(
  principal,
  action in [Rivven::Action::"create", Rivven::Action::"alter"],
  resource is Rivven::Schema
) when {
  resource.name.startsWith(principal.team + "-")
};
```

---

## Kubernetes Deployment

Deploy the Schema Registry on Kubernetes using the **RivvenSchemaRegistry CRD**:

### Using the Operator

```yaml
apiVersion: rivven.hupe1980.github.io/v1alpha1
kind: RivvenSchemaRegistry
metadata:
  name: schema-registry
  namespace: default
spec:
  clusterRef:
    name: production
  
  replicas: 2
  version: "0.0.9"
  
  # Server configuration
  server:
    port: 8081
    requestTimeoutMs: 30000
    corsEnabled: true
  
  # Store schemas in broker topic
  storage:
    mode: broker
    topic: _schemas
    replicationFactor: 3
  
  # Compatibility settings
  compatibility:
    defaultLevel: BACKWARD
    perSubject:
      "order-events-value": FULL
  
  # Enable all schema formats
  formats:
    avro: true
    jsonSchema: true
    protobuf: true
  
  # JWT authentication
  auth:
    enabled: true
    method: jwt
    jwt:
      issuerUrl: "https://auth.example.com"
      audience: "schema-registry"
  
  # TLS encryption
  tls:
    enabled: true
    certSecretName: schema-registry-tls
  
  # Prometheus metrics
  metrics:
    enabled: true
    serviceMonitorEnabled: true
```

```bash
kubectl apply -f schema-registry.yaml

# Check status
kubectl get rivvenschemaregistries
kubectl describe rivvenschemaregistry schema-registry
```

### Manual Deployment

For manual deployments without the operator:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rivven-schema-registry
spec:
  replicas: 2
  selector:
    matchLabels:
      app: rivven-schema-registry
  template:
    metadata:
      labels:
        app: rivven-schema-registry
    spec:
      containers:
        - name: schema-registry
          image: ghcr.io/hupe1980/rivven-schema:0.0.9
          ports:
            - containerPort: 8081
          env:
            - name: RIVVEN_SCHEMA_PORT
              value: "8081"
            - name: RIVVEN_SCHEMA_STORAGE_MODE
              value: "broker"
            - name: RIVVEN_BROKERS
              value: "rivven-headless:9092"
          livenessProbe:
            httpGet:
              path: /health
              port: 8081
          readinessProbe:
            httpGet:
              path: /health
              port: 8081
---
apiVersion: v1
kind: Service
metadata:
  name: rivven-schema-registry
spec:
  selector:
    app: rivven-schema-registry
  ports:
    - port: 8081
      targetPort: 8081
```

See the [Kubernetes Deployment Guide](kubernetes.md#rivvenschemaregistry-crd) for more details.

---

## Disabled Mode (No Schema Registry)

For development or simple use cases, you can disable the schema registry entirely:

```yaml
# rivven-connect.yaml
schema_registry:
  mode: disabled
```

Events are sent as plain JSON without schema validation or the 5-byte schema ID header.

## External Mode

Connect to an external schema registry (including rivven-schema) for multi-cluster environments or when integrating with existing infrastructure.

### Authentication Options

rivven-connect supports multiple authentication methods when connecting to external registries:

```yaml
schema_registry:
  mode: external
  external:
    url: "https://schema-registry.example.com:8081"
    
    # Option 1: Basic Auth (username/password)
    auth:
      type: basic
      username: "${SCHEMA_REGISTRY_USER}"
      password: "${SCHEMA_REGISTRY_PASSWORD}"
    
    # Option 2: OAuth2/OIDC
    # auth:
    #   type: oauth2
    #   client_id: "${OAUTH_CLIENT_ID}"
    #   client_secret: "${OAUTH_CLIENT_SECRET}"
    #   token_url: "https://auth.example.com/oauth/token"
    #   scope: "registry:read registry:write"
    
    # Option 3: mTLS (Enterprise)
    # auth:
    #   type: mtls
    #   client_cert_path: /etc/rivven/client.crt
    #   client_key_path: /etc/rivven/client.key
    #   ca_cert_path: /etc/rivven/ca.crt
    
    # Option 4: API Key
    # auth:
    #   type: api_key
    #   key: "${API_KEY}"
    #   header_name: "X-API-Key"
    
    # Option 5: Bearer Token
    # auth:
    #   type: bearer
    #   token: "${BEARER_TOKEN}"
    
    timeout_secs: 30
    max_retries: 3
    retry_backoff_ms: 100
```

```rust
use rivven_connect::schema::{SchemaRegistryClient, SchemaRegistryConfig};

let config = SchemaRegistryConfig::external("http://schema-registry:8081");
let registry = SchemaRegistryClient::from_config_async(&config, None).await?;
```

## AWS Glue Mode

Connect to AWS Glue Schema Registry for AWS-native deployments. Supports all schema types (Avro, JSON Schema, Protobuf) and integrates with AWS IAM for authentication.

```yaml
schema_registry:
  mode: glue
  glue:
    region: us-east-1
    registry_name: my-registry  # optional, defaults to "default-registry"
    cache_ttl_secs: 300         # schema cache TTL
```

**Features:**
- Full AWS Glue Schema Registry API support
- IAM authentication (uses default AWS credential chain)
- Schema versioning and compatibility checking
- Automatic UUID-to-integer ID mapping for standard wire format compatibility
- Schema caching for performance

**Environment Variables:**
```bash
# AWS credentials (if not using IAM roles)
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

**Example (Rust):**
```rust
use rivven_connect::schema::{SchemaRegistryClient, SchemaRegistryConfig, SchemaType, Subject};

// Create Glue registry client
let config = SchemaRegistryConfig::glue("us-east-1");
let registry = SchemaRegistryClient::from_config_async(&config, None).await?;

// Register a schema
let schema_id = registry.register(
    &Subject::new("users-value"),
    SchemaType::Avro,
    r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#
).await?;

// Get schema by ID
let schema = registry.get_by_id(schema_id).await?;
```

---

## Supported Schema Formats

| Format | Status | Features |
|--------|--------|----------|
| **JSON Schema** | ✅ Full | Draft-07 support, validation, inference |
| **Apache Avro** | ✅ Full | Encoding/decoding, fingerprinting, standard wire format |
| **Protocol Buffers** | ✅ Full | Dynamic parsing, encoding/decoding, standard wire format |

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

### Standard Wire Format

The standard wire format prefixes Avro data with a magic byte and 4-byte schema ID:

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
let wire_bytes = codec.encode_wire_format(&json, schema_id)?;

// Decode and retrieve schema ID
let (schema_id, decoded) = codec.decode_wire_format(&wire_bytes)?;
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

### Standard Wire Format

The standard wire format for Protobuf includes a message index:

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
let wire_bytes = codec.encode_wire_format(&json, schema_id)?;

// Decode and retrieve schema ID
let (schema_id, decoded) = codec.decode_wire_format(&wire_bytes)?;
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

## Schema-Aware Sinks

Wrap any sink with automatic schema registration and Avro serialization:

```rust
use rivven_connect::schema::{SchemaAwareSink, SchemaAwareConfig, SubjectStrategy};
use rivven_connect::connectors::stdout::StdoutSink;

// Create a schema-aware wrapper around any sink
let registry = SchemaRegistryClient::from_config_async(&config, None).await?;
let schema_sink = SchemaAwareSink::with_config(
    StdoutSink::new(),
    Arc::new(registry),
    SchemaAwareConfig {
        subject_strategy: SubjectStrategy::TopicName,  // "{stream}-value"
        auto_register: true,                           // auto-register inferred schemas
        wire_format: true,                             // 5-byte header
        schema_type: SchemaType::Avro,
    }
);

// Events will have schema_id and schema_subject added to metadata
// Data is serialized with Avro before writing
```

### SchemaAwareSource

Wrap any source with automatic Avro serialization and schema registration:

```rust
use rivven_connect::schema::{SchemaAwareSource, SchemaAwareConfig, SubjectStrategy};
use rivven_connect::connectors::postgres::PostgresCdcSource;

// Create a schema-aware wrapper around any source
let registry = SchemaRegistryClient::from_config_async(&config, None).await?;
let schema_source = SchemaAwareSource::with_config(
    PostgresCdcSource::new(),
    Arc::new(registry),
    SchemaAwareConfig {
        subject_strategy: SubjectStrategy::TopicName,  // "{stream}-value"
        auto_register: true,                           // auto-register inferred schemas
        wire_format: true,                             // 5-byte header
        schema_type: SchemaType::Avro,
    }
);

// Events from the source will have:
// - Data serialized as Avro bytes (standard wire format)
// - Schema registered in the schema registry
// - schema_id and schema_subject in event metadata
let stream = schema_source.read(&config, &catalog, state).await?;
```

### Subject Naming Strategies

| Strategy | Pattern | Example |
|----------|---------|---------|
| `TopicName` | `{stream}-value` | `users-value` |
| `RecordName` | `{type}` | `User` (from `__type` field) |
| `TopicRecordName` | `{stream}-{type}` | `users-Created` |
| `Custom` | User-defined | `fn(&str, &Value) -> String` |

### Stdout Sink with Avro Output

The stdout sink supports Avro output formats for debugging:

```yaml
sink:
  type: stdout
  config:
    format: avrobinary  # avrojson, avrobinary, or avrohex
    avro_schema: |
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"}
        ]
      }
    wire_format: true  # include 5-byte header
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
        mode: external
        external:
          url: "http://schema-registry:8081"
        format: avro
        compatibility: backward
```

### External Queue Bridge with Protobuf

```yaml
connectors:
  - name: external-orders
    type: external-source
    config:
      brokers: ["broker:9092"]
      topic: "orders"
      schema_registry:
        mode: external
        url: "http://schema-registry:8081"
        format: protobuf
```

### AWS Glue with CDC

```yaml
connectors:
  - name: mysql-orders
    type: mysql-cdc
    config:
      host: "mysql.example.com"
      database: "orders"
      schema_registry:
        mode: glue
        glue:
          region: us-east-1
          registry_name: orders-registry
        format: avro
```

---

## Advanced Features

### Schema Contexts (Multi-Tenancy)

Schema contexts provide namespace isolation for multi-tenant deployments. Each context acts as a separate namespace for subjects, enabling:

- **Tenant isolation** — Different teams/applications can use the same subject names
- **Environment separation** — dev/staging/prod contexts without name conflicts
- **Access control** — Per-context permissions

```rust
use rivven_schema::{SchemaRegistry, SchemaContext, RegistryConfig};

// Create a registry
let registry = SchemaRegistry::new(RegistryConfig::memory()).await?;

// Create a tenant context
let tenant_ctx = SchemaContext::new("tenant-acme")
    .with_description("ACME Corp schemas");
registry.create_context(tenant_ctx)?;

// Register schema in context using qualified subject name
// Format: :.context:subject
let schema_id = registry.register(
    ":.tenant-acme:user-value",
    SchemaType::Avro,
    r#"{"type": "record", "name": "User", "fields": [...]}"#
).await?;

// List subjects in context
let subjects = registry.list_subjects_in_context("tenant-acme");

// List all contexts
let contexts = registry.list_contexts();
```

**REST API:**

```bash
# Create context
POST /contexts {"name": "tenant-acme", "description": "ACME Corp"}

# List contexts
GET /contexts

# List subjects in context
GET /contexts/tenant-acme/subjects

# Delete context (must be empty)
DELETE /contexts/tenant-acme
```

### Version States (Schema Lifecycle)

Version states enable schema lifecycle management:

| State | Description | Behavior |
|-------|-------------|----------|
| **Enabled** | Active, fully usable | Default state for new schemas |
| **Deprecated** | Discouraged but usable | Returns warning in API response |
| **Disabled** | Blocked from use | Returns 403 Forbidden |

```rust
use rivven_schema::{SchemaVersion, VersionState};

// Deprecate a version (warns clients to migrate)
registry.deprecate_version("user-value", SchemaVersion::new(1)).await?;

// Disable a version (blocks usage entirely)
registry.disable_version("user-value", SchemaVersion::new(1)).await?;

// Re-enable a version
registry.enable_version("user-value", SchemaVersion::new(1)).await?;

// Check version state
let state = registry.get_version_state("user-value", SchemaVersion::new(1)).await?;
match state {
    VersionState::Enabled => println!("Active"),
    VersionState::Deprecated => println!("Deprecated - migrate soon"),
    VersionState::Disabled => println!("Blocked"),
}
```

**REST API:**

```bash
# Get version state
GET /subjects/user-value/versions/1/state

# Set version state
PUT /subjects/user-value/versions/1/state
Content-Type: application/json
{"state": "DEPRECATED"}

# Convenience endpoints
POST /subjects/user-value/versions/1/deprecate
POST /subjects/user-value/versions/1/disable
POST /subjects/user-value/versions/1/enable
```

### Content Validation Rules

Beyond compatibility checking, the validation engine enforces content rules on schemas:

| Rule Type | Description | Example Config |
|-----------|-------------|----------------|
| `MaxSize` | Maximum schema size in bytes | `{"max_bytes": 102400}` |
| `NamingConvention` | Name pattern matching | `{"pattern": "^[A-Z][a-zA-Z0-9]*$"}` |
| `FieldRequired` | Required fields in records | `{"fields": ["id", "timestamp"]}` |
| `FieldType` | Field type constraints | `{"field": "id", "types": ["long", "string"]}` |
| `Regex` | Custom regex validation | `{"pattern": ".*Event$", "target": "name"}` |
| `JsonSchema` | Validate against JSON Schema | `{"schema": {...}}` |

```rust
use rivven_schema::{ValidationRule, ValidationRuleType, ValidationLevel};

// Create a validation rule
let rule = ValidationRule::new(
    "max-schema-size",
    ValidationRuleType::MaxSize,
    r#"{"max_bytes": 102400}"#
)
.with_level(ValidationLevel::Error)
.with_description("Schemas must be under 100KB")
.for_subjects(vec!["user-*".to_string()]);  // Only apply to user-* subjects

// Add to registry
registry.add_validation_rule(rule);

// Validate a schema without registering
let report = registry.validate_schema(
    SchemaType::Avro,
    "user-value",
    large_schema
)?;

if !report.is_valid() {
    for error in report.error_messages() {
        println!("Validation error: {}", error);
    }
}
```

**Pre-built Rule Presets:**

```rust
use rivven_schema::validation::presets;

// Production-ready ruleset
let engine = presets::production_ruleset();

// Rules included:
// - max_bytes: 100KB limit
// - naming: PascalCase for record names
// - required fields: id, timestamp
```

**REST API:**

```bash
# List validation rules
GET /config/validation/rules

# Add a validation rule
POST /config/validation/rules
Content-Type: application/json
{
  "name": "max-size",
  "rule_type": "MaxSize",
  "config": {"max_bytes": 102400},
  "level": "ERROR",
  "description": "Schemas must be under 100KB"
}

# Validate schema without registering
POST /subjects/user-value/validate
Content-Type: application/json
{"schema": "{...}", "schemaType": "AVRO"}

# Delete a rule
DELETE /config/validation/rules/max-size
```

### Prometheus Metrics

Enable Prometheus metrics for monitoring:

```bash
# Build with metrics support
cargo build -p rivven-schema --features metrics
```

```rust
use rivven_schema::{SchemaRegistry, RegistryConfig, MetricsConfig};

// Create registry with metrics
let config = RegistryConfig::memory();
let metrics_config = MetricsConfig::default()
    .with_prefix("rivven_schema")
    .with_buckets(vec![0.001, 0.01, 0.1, 0.5, 1.0]);

let registry = SchemaRegistry::with_metrics(config, metrics_config).await?;

// Access metrics for custom integration
if let Some(metrics) = registry.metrics() {
    // Metrics are automatically recorded
    // Access Prometheus registry if needed
}
```

**Available Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `schemas_registered_total` | Counter | Total schemas registered |
| `schemas_lookups_total` | Counter | Total schema lookups (by_id/by_version) |
| `compatibility_checks_total` | Counter | Compatibility checks (pass/fail) |
| `validation_checks_total` | Counter | Validation rule checks |
| `errors_total` | Counter | Errors by type |
| `version_state_changes_total` | Counter | State transitions |
| `schemas_count` | Gauge | Current schema count |
| `subjects_count` | Gauge | Current subject count |
| `versions_count` | Gauge | Current version count |
| `operation_duration_seconds` | Histogram | Operation latency |
| `schema_size_bytes` | Histogram | Schema sizes |

**Prometheus Endpoint:**

```bash
# Start server with metrics
rivven-schema serve --port 8081 --metrics-port 9090

# Scrape metrics
curl http://localhost:9090/metrics
```

**Grafana Dashboard:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'rivven-schema'
    static_configs:
      - targets: ['localhost:9090']
```

---

## REST API Reference

The Schema Registry provides an industry-standard REST API with additional enterprise endpoints.

### Core Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information |
| `GET` | `/health` | Health check |
| `GET` | `/ready` | Readiness check |
| `GET` | `/live` | Liveness check |
| `GET` | `/subjects` | List all subjects |
| `GET` | `/subjects/:subject` | Get subject info |
| `DELETE` | `/subjects/:subject` | Delete subject (soft delete) |
| `GET` | `/subjects/:subject/versions` | List versions for subject |
| `POST` | `/subjects/:subject/versions` | Register new schema |
| `GET` | `/subjects/:subject/versions/:version` | Get schema by version |
| `DELETE` | `/subjects/:subject/versions/:version` | Delete specific version |
| `GET` | `/subjects/:subject/versions/:version/schema` | Get raw schema string |
| `GET` | `/subjects/:subject/versions/:version/referencedby` | Get referencing schemas |
| `POST` | `/subjects/:subject` | Check if schema exists |
| `GET` | `/schemas` | List all schemas |
| `GET` | `/schemas/ids/:id` | Get schema by ID |
| `GET` | `/schemas/ids/:id/schema` | Get raw schema by ID |
| `GET` | `/schemas/ids/:id/subjects` | Get subjects using schema |
| `GET` | `/schemas/ids/:id/versions` | Get all versions of schema |
| `GET` | `/schemas/types` | List supported schema types |
| `GET` | `/config` | Get global config |
| `PUT` | `/config` | Set global compatibility |
| `GET` | `/config/:subject` | Get subject config |
| `PUT` | `/config/:subject` | Set subject compatibility |
| `DELETE` | `/config/:subject` | Reset subject config |
| `POST` | `/compatibility/subjects/:subject/versions/:version` | Check compatibility |
| `POST` | `/compatibility/subjects/:subject/versions/latest` | Check vs latest |
| `GET` | `/mode` | Get global mode |
| `PUT` | `/mode` | Set global mode |

### Version State Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/subjects/:subject/versions/:version/state` | Get version state |
| `PUT` | `/subjects/:subject/versions/:version/state` | Set version state |
| `POST` | `/subjects/:subject/versions/:version/deprecate` | Mark as deprecated |
| `POST` | `/subjects/:subject/versions/:version/disable` | Disable version |
| `POST` | `/subjects/:subject/versions/:version/enable` | Re-enable version |

### Content Validation Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/subjects/:subject/validate` | Validate schema without registering |
| `GET` | `/config/validation/rules` | List all validation rules |
| `POST` | `/config/validation/rules` | Add a validation rule |
| `DELETE` | `/config/validation/rules/:name` | Delete a validation rule |

### Schema Context Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/contexts` | List all contexts |
| `POST` | `/contexts` | Create a new context |
| `GET` | `/contexts/:context` | Get context details |
| `DELETE` | `/contexts/:context` | Delete context (must be empty) |
| `GET` | `/contexts/:context/subjects` | List subjects in context |

### Statistics Endpoint

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stats` | Get registry statistics |

**Example Stats Response:**
```json
{
  "total_schemas": 42,
  "total_subjects": 15,
  "total_versions": 87,
  "schemas_by_type": {
    "AVRO": 35,
    "JSON": 5,
    "PROTOBUF": 2
  }
}
```

---

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
| `encode_wire_format(json, id)` | Encode with standard wire format |
| `decode_wire_format(bytes)` | Decode standard wire format |

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
| `encode_wire_format(json, id)` | Encode with standard wire format |
| `decode_wire_format(bytes)` | Decode standard wire format |

---

*For more information, see the [Architecture](architecture.md) and [Connector Development](connector-development.md) guides.*

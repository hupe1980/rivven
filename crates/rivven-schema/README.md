# rivven-schema

> High-performance Schema Registry for the Rivven event streaming platform.

## Overview

`rivven-schema` provides schema management with Avro, JSON Schema, and Protobuf support. It offers an industry-standard REST API for drop-in compatibility.

## Features

| Category | Features |
|:---------|:---------|
| **Formats** | Avro, JSON Schema, Protobuf |
| **Evolution** | Forward, backward, full, and transitive compatibility |
| **Storage** | In-memory, broker-backed, AWS Glue |
| **API** | Industry-standard REST API |
| **Auth** | Basic, Bearer, JWT/OIDC, API Keys |
| **K8s** | Health checks (`/health`, `/health/live`, `/health/ready`) |

> **Note**: The Schema Registry stores, versions, and validates schemas. It does **not**
> encode/decode message data — that's the job of producers and consumers.
> Use `rivven-connect` for Avro/Protobuf/JSON codecs.

## Deployment Modes

Rivven supports **3 schema modes** for maximum flexibility:

| Mode | Description | Use Case |
|------|-------------|----------|
| **Broker-backed** | Store schemas in rivven broker topics | Production (self-hosted) |
| **External** | Connect to an external compatible registry | Production, multi-cluster |
| **External (AWS Glue)** | Connect to AWS Glue Schema Registry | AWS-native deployments |
| **In-memory** | Fast, volatile storage | Development, testing |

> **Note**: The broker (rivvend) is schema-agnostic. It only handles raw bytes. All schema operations are handled by rivven-schema or external registries.

## Quick Start

### As a Library

```rust
use rivven_schema::{SchemaRegistry, RegistryConfig, SchemaType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create an in-memory registry
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await?;

    // Register a schema
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

    // Check compatibility (for evolving schemas)
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

### Broker-Backed Storage (Production)

Enable durable storage by using rivven broker topics (using `_schemas` topic):

```bash
cargo build -p rivven-schema --features broker
```

```rust,ignore
use rivven_schema::{RegistryConfig, BrokerStorageConfig, SchemaRegistry};

// Configure broker-backed storage
let broker_config = BrokerStorageConfig::new("localhost:9092")
    .with_topic("_schemas")            // Custom topic name (default: "_schemas")
    .with_replication_factor(3);       // Replication for durability

let config = RegistryConfig::broker(broker_config);
let registry = SchemaRegistry::new(config).await?;
```

Benefits:
- **Durability**: Schemas survive registry restarts
- **Replication**: Schemas replicated across broker nodes  
- **No external dependencies**: Uses rivven broker itself
- **Compaction**: Only latest schema versions retained

### As a Standalone Server

```bash
# Start with in-memory storage
rivven-schema serve --port 8081
```

## Authentication (Optional)

Enable authentication by building with the `auth` feature:

```bash
cargo build -p rivven-schema --features auth
```

The schema registry supports enterprise-grade authentication:

| Method | Header | Use Case |
|--------|--------|----------|
| **HTTP Basic Auth** | `Authorization: Basic base64(user:pass)` | Simple deployments |
| **Bearer Token** | `Authorization: Bearer <session-id>` | Session-based auth |
| **JWT/OIDC** | `Authorization: Bearer <jwt>` | Enterprise SSO (requires `jwt` feature) |
| **API Keys** | `X-API-Key: <key>` | Service-to-service auth |

### JWT/OIDC Support

For JWT/OIDC token validation, enable the `jwt` feature:

```bash
cargo build -p rivven-schema --features jwt
```

Supports:
- HS256, RS256, ES256 algorithms
- Configurable issuer and audience validation
- JWKS endpoint support for key rotation
- Custom claims mapping (groups, roles)

Authentication integrates with rivven-core's RBAC system, supporting:
- Per-subject access control (read/write/admin permissions)
- Anonymous read access (configurable)
- Rate limiting and lockout protection

### Cedar Policy-Based Authorization (Optional)

For fine-grained, policy-as-code authorization, use the `cedar` feature:

```bash
cargo build -p rivven-schema --features cedar
```

Cedar provides powerful policy expressions with fine-grained access control:

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

// Deny deletions outside maintenance windows
forbid(
  principal,
  action == Rivven::Action::"delete",
  resource is Rivven::Schema
) unless {
  context.timestamp.hour >= 2 && context.timestamp.hour <= 6
};
```

```rust,ignore
use rivven_schema::{SchemaServer, ServerConfig, AuthConfig, CedarAuthorizer};
use rivven_core::AuthManager;
use std::sync::Arc;

// Create Cedar authorizer with policies
let authorizer = Arc::new(CedarAuthorizer::new()?);
authorizer.add_policy("schema-admin", r#"
permit(
  principal in Rivven::Group::"schema-admins",
  action,
  resource is Rivven::Schema
);
"#)?;

// Configure server with Cedar
let config = ServerConfig::default()
    .with_auth(AuthConfig::required().with_cedar());

let server = SchemaServer::with_cedar(registry, config, auth_manager, authorizer);
```

### Programmatic Authentication Setup

```rust,ignore
use rivven_schema::{SchemaServer, ServerConfig, AuthConfig};
use rivven_core::AuthManager;
use std::sync::Arc;

// Create auth manager with users
let auth_manager = Arc::new(AuthManager::new());
auth_manager.create_principal("admin", "Secret@123", PrincipalType::User, ["admin"])?;

// Configure server with authentication
let config = ServerConfig::default()
    .with_auth(AuthConfig::required());

let server = SchemaServer::with_auth(registry, config, auth_manager);
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
rivven-schema compat --url http://localhost:8081 --subject user-value --schema new-schema.avsc
```

## REST API

The server implements a standard Schema Registry REST API plus enterprise extensions:

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Get server info |
| `/subjects` | GET | List all subjects |
| `/subjects/{subject}/versions` | GET | List versions for a subject |
| `/subjects/{subject}/versions` | POST | Register a new schema (with optional references) |
| `/subjects/{subject}/versions/{version}` | GET | Get schema by subject and version |
| `/subjects/{subject}/versions/{version}/referencedby` | GET | Get schemas referencing this version |
| `/schemas/ids/{id}` | GET | Get schema by global ID |
| `/compatibility/subjects/{subject}/versions/{version}` | POST | Check compatibility |
| `/config` | GET/PUT | Get/set global compatibility config |
| `/config/{subject}` | GET/PUT | Get/set subject compatibility config |

### Version State Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/subjects/{subject}/versions/{version}/state` | GET/PUT | Get/set version state |
| `/subjects/{subject}/versions/{version}/deprecate` | POST | Mark version as deprecated |
| `/subjects/{subject}/versions/{version}/disable` | POST | Disable version |
| `/subjects/{subject}/versions/{version}/enable` | POST | Re-enable version |

### Subject Recovery Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/subjects/deleted` | GET | List soft-deleted subjects |
| `/subjects/{subject}/undelete` | POST | Restore a soft-deleted subject |

### Content Validation Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/subjects/{subject}/validate` | POST | Validate schema without registering |
| `/config/validation/rules` | GET/POST | List/add validation rules |
| `/config/validation/rules/{name}` | DELETE | Delete validation rule |

### Schema Context Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/contexts` | GET/POST | List/create contexts |
| `/contexts/{context}` | GET/DELETE | Get/delete context |
| `/contexts/{context}/subjects` | GET | List subjects in context |

### Monitoring Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/stats` | GET | Get registry statistics |
| `/health` | GET | Health check |
| `/health/live` | GET | Liveness probe |
| `/health/ready` | GET | Readiness probe |

## Compatibility Modes

| Mode | Description |
|------|-------------|
| `BACKWARD` | New schema can read old data (default) |
| `BACKWARD_TRANSITIVE` | New schema can read all previous data |
| `FORWARD` | Old schema can read new data |
| `FORWARD_TRANSITIVE` | All previous schemas can read new data |
| `FULL` | Both backward and forward compatible |
| `FULL_TRANSITIVE` | Both backward and forward compatible with all versions |
| `NONE` | No compatibility checking |

## Schema Formats

### Avro (Recommended)

```json
{
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["null", "string"], "default": null}
    ]
}
```

### JSON Schema

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"}
    },
    "required": ["id", "name"]
}
```

## Wire Format

The registry uses a standard wire format for encoded messages:

```
+--------+----------------+------------------+
| Magic  | Schema ID      | Avro Payload     |
| (1 B)  | (4 B BE)       | (variable)       |
+--------+----------------+------------------+
| 0x00   | [schema_id]    | [avro_bytes]     |
+--------+----------------+------------------+
```

This allows consumers to look up the schema by ID before deserializing.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `server` | ✅ | HTTP server with industry-standard REST API |
| `cli` | ✅ | Command-line interface |
| `avro` | ✅ | Avro schema parsing and compatibility checking |
| `json-schema` | ✅ | JSON Schema support with validation |
| `protobuf` | ✅ | Protobuf schema parsing and compatibility checking |
| `external` | ❌ | External Schema Registry client |
| `glue` | ❌ | AWS Glue Schema Registry client |
| `metrics` | ❌ | Prometheus metrics for monitoring |

> **Note**: For encoding/decoding data with Avro/Protobuf codecs, use `rivven-connect`.

## Advanced Features

### Schema Contexts (Multi-Tenancy)

Schema contexts provide namespace isolation for multi-tenant deployments:

```rust
use rivven_schema::{SchemaRegistry, SchemaContext, RegistryConfig};

// Create a tenant context
let tenant_ctx = SchemaContext::new("tenant-acme")
    .with_description("ACME Corp schemas");
registry.create_context(tenant_ctx)?;

// Register schema in context using qualified subject name
// Format: :.context:subject
let schema_id = registry.register(
    ":.tenant-acme:user-value",
    SchemaType::Avro,
    schema
).await?;

// List subjects in context
let subjects = registry.list_subjects_in_context("tenant-acme");
```

### Version States (Schema Lifecycle)

Manage schema version lifecycle with states:

```rust
use rivven_schema::{SchemaVersion, VersionState};

// Deprecate a version (warns clients)
registry.deprecate_version("user-value", SchemaVersion::new(1)).await?;

// Disable a version (blocks usage)
registry.disable_version("user-value", SchemaVersion::new(1)).await?;

// Re-enable a version
registry.enable_version("user-value", SchemaVersion::new(1)).await?;
```

| State | Description | Behavior |
|-------|-------------|----------|
| **Enabled** | Active, fully usable | Default state |
| **Deprecated** | Discouraged but usable | Returns warning |
| **Disabled** | Blocked from use | Returns 403 |

### Subject Recovery (Undelete)

Soft-deleted subjects can be recovered within a configurable retention period:

```rust
use rivven_schema::SchemaRegistry;

// Soft delete a subject (default)
let deleted_versions = registry.delete_subject("user-value", false).await?;
println!("Deleted versions: {:?}", deleted_versions);

// List deleted subjects available for recovery
let deleted = registry.list_deleted_subjects().await?;
for subject in &deleted {
    println!("Can recover: {}", subject);
}

// Recover a deleted subject
let restored_versions = registry.undelete_subject("user-value").await?;
println!("Restored versions: {:?}", restored_versions);

// Permanent delete (cannot be recovered)
registry.delete_subject("user-value", true).await?;
```

**Note:** Soft-deleted subjects are moved to a recoverable state. Permanent deletes cannot be undone.

### Content Validation Rules

Enforce content rules beyond compatibility checking:

```rust
use rivven_schema::{ValidationRule, ValidationRuleType, ValidationLevel};

// Add a max size rule
registry.add_validation_rule(
    ValidationRule::new("max-size", ValidationRuleType::MaxSize, r#"{"max_bytes": 102400}"#)
        .with_level(ValidationLevel::Error)
);

// Validate before registering
let report = registry.validate_schema(SchemaType::Avro, "subject", schema)?;
if !report.is_valid() {
    println!("Errors: {:?}", report.error_messages());
}
```

Available rule types: `MaxSize`, `NamingConvention`, `FieldRequired`, `FieldType`, `Regex`, `JsonSchema`.

### Schema References (Cross-Schema Dependencies)

Schema references allow schemas to reference types defined in other schemas. This is essential for:
- Sharing common types (e.g., `Address`, `Money`) across multiple schemas
- Managing complex domain models with reusable building blocks
- Protobuf imports and JSON Schema `$ref` support

```rust
use rivven_schema::{SchemaRegistry, SchemaType, SchemaReference, RegistryConfig};

// Register a base schema first
let address_schema = r#"{
    "type": "record",
    "name": "Address",
    "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"}
    ]
}"#;
let address_id = registry.register("address-value", SchemaType::Avro, address_schema).await?;

// Register a schema that references Address
let user_schema = r#"{
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "address": {"$ref": "Address"}
    }
}"#;
let refs = vec![
    SchemaReference {
        name: "Address".to_string(),
        subject: "address-value".to_string(),
        version: 1,
    }
];
let user_id = registry.register_with_references(
    "user-value",
    SchemaType::Json,
    user_schema,
    refs
).await?;

// Find all schemas that reference a given schema
let referencing = registry.get_schemas_referencing("address-value", SchemaVersion::new(1)).await?;
```

**API Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/subjects/{subject}/versions` | POST | Register schema with `references` array |
| `/subjects/{subject}/versions/{version}/referencedby` | GET | Get schemas referencing this version |

**Request body for registration with references:**
```json
{
    "schema": "{...}",
    "schemaType": "JSON",
    "references": [
        {"name": "Address", "subject": "address-value", "version": 1}
    ]
}
```

### Prometheus Metrics

Enable monitoring with the `metrics` feature:

```bash
cargo build -p rivven-schema --features metrics
```

```rust
use rivven_schema::{SchemaRegistry, RegistryConfig, MetricsConfig};

let registry = SchemaRegistry::with_metrics(
    RegistryConfig::memory(),
    MetricsConfig::default()
).await?;
```

Metrics include: `schemas_registered_total`, `schemas_lookups_total`, `compatibility_checks_total`, `operation_duration_seconds`, etc.

## Standard Wire Format

### Avro Wire Format

When using Avro with Schema Registry, data is encoded with a 5-byte header:

```
+--------+----------------+------------------+
| Magic  | Schema ID      | Avro Payload     |
| (1 B)  | (4 B BE)       | (variable)       |
+--------+----------------+------------------+
| 0x00   | [schema_id]    | [avro_bytes]     |
+--------+----------------+------------------+
```

### Protobuf Wire Format

For Protobuf, the format includes a message index (varint):

```
+--------+----------------+-------------+------------------+
| Magic  | Schema ID      | Msg Index   | Protobuf Payload |
| (1 B)  | (4 B BE)       | (varint)    | (variable)       |
+--------+----------------+-------------+------------------+
| 0x00   | [schema_id]    | 0x00        | [proto_bytes]    |
+--------+----------------+-------------+------------------+
```

This format is compatible with standard producers/consumers using common serializers.

## Best Practices

1. **Use Avro in Production**: Schema evolution with compatibility checking
2. **Subject Naming**: Use `{topic}-key` and `{topic}-value` convention
3. **Compatibility Level**: Start with `BACKWARD` for safe evolution
4. **Versioning**: Never delete schemas, only deprecate
5. **Deduplication**: Same schema content gets the same ID across subjects

## Integration with Rivven Connect

```rust
use rivven_connect::schema::{SchemaRegistryClient, SchemaRegistryConfig, SchemaType, Subject};

// Create registry client (multiple modes available)
let config = SchemaRegistryConfig::external("http://localhost:8081");
let registry = SchemaRegistryClient::from_config_async(&config, None).await?;

// Register a schema
let schema_id = registry.register(
    &Subject::value("users"),
    SchemaType::Avro,
    r#"{"type":"record","name":"User","fields":[...]}"#
).await?;
```

## Documentation

- [Schema Registry](https://rivven.hupe1980.github.io/rivven/docs/schema-registry)
- [Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)

## License

Apache-2.0. See [LICENSE](../../LICENSE).

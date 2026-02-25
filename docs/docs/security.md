---
layout: default
title: Security
nav_order: 6
---

# Security
{: .no_toc }

Enterprise-grade security features.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven implements **31 security features** to protect your data. Authentication is **wired in by default** — all high-level clients (`Producer`, `ResilientClient`, `PipelinedClient`) perform SCRAM-SHA-256 authentication automatically when credentials are configured, including on reconnect.

| Category | Features |
|:---------|:---------|
| **Transport** | TLS 1.3, mTLS, QUIC encryption |
| **Broker Auth** | mTLS client certs, API tokens, SCRAM-SHA-256 |
| **Schema Registry Auth** | Basic Auth, Bearer Token, JWT/OIDC, API Keys |
| **Authorization** | RBAC, ACLs, Cedar policy-as-code |
| **Data Protection** | Encryption at rest, field-level masking |
| **Input Validation** | Size limits, rate limiting, injection prevention |

---

## Transport Security

### TLS Configuration

Enable TLS for client connections:

```yaml
# rivven.yaml
server:
  tls:
    enabled: true
    cert_path: /etc/rivven/tls/server.crt
    key_path: /etc/rivven/tls/server.key
    ca_path: /etc/rivven/tls/ca.crt
    client_auth: required  # none, optional, required
    min_version: "1.3"
```

### Automatic Certificate Hot-Reload

Rivven automatically reloads TLS certificate and key files **without restarting the broker or dropping existing connections**. This enables zero-downtime certificate rotation:

- A background task periodically re-reads `cert_path`, `key_path`, and `ca_path`
- New connections use the updated certificates immediately
- Existing connections continue with their original certificates until they reconnect
- No extra configuration — hot-reload is enabled automatically when TLS is active

This is ideal for:

- **cert-manager** (Kubernetes) automatic certificate renewal
- **Let's Encrypt** / ACME certificate rotation
- **Short-lived certificates** in zero-trust environments

### Certificate Generation

Generate certificates with OpenSSL:

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -out ca.crt -subj "/CN=Rivven CA"

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=rivven.example.com"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 365 -sha256

# Generate client certificate (for mTLS)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/CN=client-app"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt -days 365 -sha256
```

### Client Connection with TLS

```rust
use rivven_client::{ResilientClient, ResilientClientConfig};

let config = ResilientClientConfig::builder()
    .servers(vec!["rivven.example.com:9292".into()])
    .tls_config(TlsConfig {
        ca_cert: include_bytes!("../ca.crt").to_vec(),
        // For mTLS:
        client_cert: Some(include_bytes!("../client.crt").to_vec()),
        client_key: Some(include_bytes!("../client.key").to_vec()),
    })
    .build()?;

let client = ResilientClient::new(config).await?;
```

### Cluster Transport

Inter-node communication supports two transport modes:

| Transport | Encryption | Feature Flag |
|-----------|-----------|---------------|
| `quic` | ✅ Automatic TLS 1.3 | `quic` |
| `tcp` | ❌ Plaintext | (default) |

> **Warning:** TCP transport is **plaintext**. For encrypted inter-node communication, use `transport: quic` which requires the `quic` feature flag.

```yaml
cluster:
  transport: quic  # or tcp (plaintext)
  quic:
    keep_alive_interval_secs: 15
    max_idle_timeout_secs: 30
```

---

## Authentication

### mTLS Client Authentication

Require client certificates:

```yaml
server:
  tls:
    client_auth: required
    ca_path: /etc/rivven/tls/ca.crt
```

Map certificates to identities:

```yaml
auth:
  mtls_mapping:
    # CN=service-a -> principal "service-a"
    type: common_name
    # Or use SAN extension
    # type: san_dns
    # type: san_uri
```

### API Tokens

Generate API tokens for services:

```bash
# Generate token
rivven auth token create --name service-a --ttl 365d
# Output: rvn_a1b2c3d4e5f6...

# List tokens
rivven auth token list

# Revoke token
rivven auth token revoke service-a
```

Use tokens in client:

```rust
use rivven_client::{ResilientClient, ResilientClientConfig};

let config = ResilientClientConfig::builder()
    .servers(vec!["rivven.example.com:9292".into()])
    .auth_token("rvn_a1b2c3d4e5f6...")
    .build()?;

let client = ResilientClient::new(config).await?;
```

### SCRAM-SHA-256

Username/password authentication using RFC 5802 / RFC 7677 SCRAM-SHA-256:

```yaml
auth:
  mechanism: scram-sha-256
  users_file: /etc/rivven/users.yaml
```

```yaml
# users.yaml
users:
  admin:
    password_hash: "$scram-sha-256$..."  # Use rivven auth hash-password
    roles: [admin]
  
  producer-app:
    password_hash: "$scram-sha-256$..."
    roles: [producer]
```

#### SCRAM Protocol Flow

1. **Client → Server**: Client-first-message (`n,,n=<user>,r=<client-nonce>`)
2. **Server → Client**: Server-first-message (`r=<combined-nonce>,s=<salt>,i=<iterations>`)  
3. **Client → Server**: Client-final-message (`c=biws,r=<nonce>,p=<proof>`)
4. **Server → Client**: Server-final-message (`v=<verifier>`)

#### Security Features

- **PBKDF2-HMAC-SHA256**: 600,000 iterations for key derivation (OWASP recommendation)
- **32-byte random salt**: Per-user unique salt prevents rainbow tables
- **Constant-time comparison**: Prevents timing attacks
- **User enumeration prevention**: Fake salt/iterations for unknown users
- **Mutual authentication**: Server proves it knows the password too
- **Password complexity**: Minimum 8 characters, must include uppercase, lowercase, digit, and special character

#### Client Example

```rust
use rivven_client::Client;

// Low-level: authenticate an existing client connection
let mut client = Client::connect("localhost:9092").await?;
client.authenticate_scram("admin", "secure-password").await?;
```

#### Auto-Authentication in High-Level Clients

`Producer`, `ResilientClient`, and `PipelinedClient` perform SCRAM-SHA-256 authentication **automatically** on connection when credentials are configured:

```rust
use rivven_client::{Producer, ProducerConfig};

let config = ProducerConfig::builder()
    .bootstrap_servers(vec!["localhost:9092".to_string()])
    .auth("producer-app", "secure-password")
    .build();

let producer = Producer::new(config).await?;
// Connection is authenticated — no manual auth call needed
```

```rust
use rivven_client::{ResilientClient, ResilientClientConfig};

let config = ResilientClientConfig::builder()
    .servers(vec!["node1:9092".to_string(), "node2:9092".to_string()])
    .auth("my-service", "service-password")
    .build();

let client = ResilientClient::new(config).await?;
// All pooled connections are authenticated on creation and re-authenticated on reconnect
```

---

## Authorization

### Role-Based Access Control (RBAC)

RBAC is enforced on all **data-plane operations** — produce, consume, and admin requests are all subject to authorization checks. Authentication is required by default; unauthenticated requests are rejected before reaching any handler.

Define roles with permissions:

```yaml
# rbac.yaml
roles:
  admin:
    permissions:
      - resource: "*"
        actions: ["*"]
  
  producer:
    permissions:
      - resource: "topic:events-*"
        actions: [produce]
      - resource: "topic:logs-*"
        actions: [produce]
  
  consumer:
    permissions:
      - resource: "topic:events-*"
        actions: [consume]
      - resource: "group:*"
        actions: [join, describe]
  
  readonly:
    permissions:
      - resource: "*"
        actions: [describe, list]
```

### ACL Rules

Fine-grained topic ACLs with indexed lookups:

> **Performance note:** ACL entries are stored in an `AclIndex` keyed by
> principal name. Lookups scan only the entries for the requesting principal
> plus wildcard rules — O(W + P) average instead of O(N). Wildcard (`*`)
> principals are kept in a separate list to preserve deny-takes-precedence
> semantics.

```bash
# Allow user to produce to specific topics
rivven acl add --principal user:producer-app \
  --resource topic:orders \
  --action produce \
  --allow

# Allow group to consume
rivven acl add --principal group:analytics-team \
  --resource topic:events-* \
  --action consume \
  --allow

# Deny access
rivven acl add --principal user:suspicious \
  --resource topic:* \
  --action "*" \
  --deny

# List ACLs
rivven acl list
```

### Permission Matrix

| Action | Description |
|:-------|:------------|
| `produce` | Publish messages |
| `consume` | Read messages |
| `create` | Create topics/groups |
| `delete` | Delete topics/groups |
| `describe` | View metadata |
| `alter` | Modify configurations |
| `admin` | Full control |

---

## Data Protection

### Encryption at Rest

Enable storage encryption:

```yaml
storage:
  encryption:
    enabled: true
    algorithm: aes-256-gcm  # or chacha20-poly1305
    key_rotation_days: 90   # automatic key rotation
    key_source: kms  # file, kms, vault
    kms:
      provider: aws
      key_id: alias/rivven-data-key
      region: us-east-1
```

**Nonce construction**: AES-256-GCM uses fully random 96-bit nonces generated via `SystemRandom`. The birthday bound allows approximately $2^{48}$ encryptions per key before collision probability becomes non-negligible — far beyond any realistic WAL lifetime. Nonce uniqueness does not depend on monotonic counters, making the scheme safe across crash recovery and WAL replay scenarios.

### Field-Level Masking

Mask sensitive data in transit:

```yaml
sources:
  orders:
    connector: postgres-cdc
    transforms:
      - type: MaskField
        config:
          fields:
            - credit_card
            - ssn
            - email
          mask_patterns:
            credit_card: "****-****-****-{last4}"
            ssn: "***-**-{last4}"
            email: "{first}***@{domain}"
```

### PII Detection

Automatic PII scanning:

```yaml
data_governance:
  pii_detection:
    enabled: true
    actions:
      - type: warn  # warn, mask, reject
        severity: high
      - type: mask
        severity: medium
    patterns:
      - name: credit_card
        regex: '\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}'
        severity: high
      - name: ssn
        regex: '\d{3}-\d{2}-\d{4}'
        severity: high
```

---

## Input Validation

### Message Size Limits

```yaml
limits:
  max_message_size: 10485760  # 10 MB
  max_batch_size: 104857600   # 100 MB
  max_key_size: 1048576       # 1 MB
```

### Rate Limiting

```yaml
rate_limiting:
  enabled: true
  global:
    requests_per_second: 100000
    burst: 10000
  
  per_client:
    requests_per_second: 10000
    burst: 1000
  
  per_topic:
    requests_per_second: 50000
    burst: 5000
```

### Request Validation

```yaml
validation:
  topic_name:
    pattern: "^[a-zA-Z0-9._-]+$"
    max_length: 249
  
  key:
    max_size: 1048576
    required: false
  
  headers:
    max_count: 100
    max_key_size: 1024
    max_value_size: 32768
```

### TLS Enforcement for CDC Connections

{: .warning }
> CDC connectors default to `tls.mode: prefer`, which silently falls back to unencrypted connections. For production deployments, set `tls.mode: verify-full` (PostgreSQL) or `tls.mode: verify-identity` (MySQL) to enforce encrypted and authenticated connections.

### TLS Enforcement for Plaintext Auth

{: .warning }
> When using plaintext authentication (simple username/password), TLS is enforced. The broker rejects plaintext auth attempts over unencrypted connections to prevent credential leakage. Use SCRAM-SHA-256 or enable TLS for password-based authentication.

### HTTP LLM Provider Security

LLM providers (OpenAI, Bedrock) require HTTPS by default. If you need to connect to an HTTP endpoint (e.g., a local proxy or development server), set `allow_insecure: true` explicitly in the provider configuration. This opt-in prevents accidental credential transmission over unencrypted connections.

### Operator Environment Variable Validation

The Rivven operator validates environment variable specifications in CRDs, including `value_from` references. Dangerous environment variables (e.g., `LD_PRELOAD`, `LD_LIBRARY_PATH`) are blocked to prevent injection attacks that could compromise the container runtime.

### Cluster Secret for Raft RPC

Raft RPC communication between cluster nodes is authenticated using a shared cluster secret. This prevents unauthorized nodes from joining the cluster or injecting log entries. Configure the secret via `cluster.secret` or `RIVVEN_CLUSTER_SECRET` environment variable.

### CDC Identifier Validation

All CDC table, schema, and column identifiers are validated at construction time via `Validator::validate_identifier()`, which enforces:

- Pattern: `^[a-zA-Z_][a-zA-Z0-9_]{0,254}$`
- Maximum length: 255 characters
- No SQL injection characters (quotes, semicolons, comments, etc.)

`TableSpec::new()` returns `Result` — invalid identifiers are rejected before any SQL query is constructed.

**Defense-in-depth:** Snapshot queries additionally escape identifiers at the query site:

| Database | Escaping |
|----------|----------|
| MySQL | Backtick-doubling (`` ` `` → ` `` ``) |
| PostgreSQL | Double-quote-doubling (`"` → `""`) |
| SQL Server | Bracket-escaping (`]` → `]]`) |

This two-layer approach (validation + escaping) prevents SQL injection even if validation is bypassed via internal API misuse.

### Protocol Numeric Safety

Wire protocol conversions use validated and saturating casts to prevent silent truncation:

| Field | Conversion | Safety |
|-------|-----------|--------|
| `producer_epoch` | `u32 → u16` | `safe_producer_epoch()` — returns `ProtocolError` on overflow |
| `max_messages` | `usize → u32` | `u32::try_from().unwrap_or(u32::MAX)` — saturates instead of truncating |
| `expires_in` | `u64 → u32` | `u32::try_from().unwrap_or(u32::MAX)` — saturates instead of truncating |
| `port` | `u32 → u16` | `u16::try_from()` — logs warning and uses 0 on overflow |

---

## Audit Logging

### Enable Audit Trail

```yaml
audit:
  enabled: true
  log_path: /var/log/rivven/audit.log
  events:
    - authentication
    - authorization
    - topic_create
    - topic_delete
    - acl_change
    - config_change
  format: json
```

### Audit Log Format

```json
{
  "timestamp": "2026-01-25T10:30:00.123Z",
  "event_type": "authorization",
  "principal": "user:producer-app",
  "action": "produce",
  "resource": "topic:orders",
  "result": "allowed",
  "client_ip": "10.0.1.50",
  "request_id": "req-abc123"
}
```

### Log Shipping

Send audit logs to external systems:

```yaml
audit:
  output:
    - type: file
      path: /var/log/rivven/audit.log
    
    - type: stream
      bootstrap_servers: rivven:9292
      topic: security-audit
    
    - type: syslog
      address: syslog.example.com:514
      facility: auth
```

---

## Network Security

### Listener Bindings

```yaml
listeners:
  - name: internal
    bind: 0.0.0.0:9292
    protocol: plaintext
    # Restrict to internal network
    allowed_cidrs:
      - 10.0.0.0/8
      - 172.16.0.0/12
  
  - name: external
    bind: 0.0.0.0:9293
    protocol: tls
    tls:
      cert_path: /etc/rivven/tls/server.crt
      key_path: /etc/rivven/tls/server.key
```

### IP Allowlists

```yaml
security:
  ip_allowlist:
    enabled: true
    cidrs:
      - 10.0.0.0/8
      - 192.168.1.0/24
      - 203.0.113.50/32  # Specific IP
```

### Connection Limits

```yaml
limits:
  max_connections: 10000
  max_connections_per_ip: 100
  rate_limit_per_ip: 1000        # Max requests per second per IP
  connection_timeout_secs: 30
  idle_timeout_secs: 300
```

Rate limiting is enforced on both the cluster server (inter-node) and the secure server (client-facing TLS). When a client exceeds the per-IP rate limit, requests are rejected with a `RATE_LIMIT_EXCEEDED` error. Connections from IPs exceeding `max_connections_per_ip` are refused at the accept level.

---

## Kubernetes Security

### Pod Security

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: rivven
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    fsGroup: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: rivven
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
```

### Secret Management

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: rivven-tls
type: kubernetes.io/tls
data:
  tls.crt: <base64-encoded-cert>
  tls.key: <base64-encoded-key>
  ca.crt: <base64-encoded-ca>
---
apiVersion: apps/v1
kind: StatefulSet
spec:
  template:
    spec:
      containers:
        - name: rivven
          volumeMounts:
            - name: tls
              mountPath: /etc/rivven/tls
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: rivven-tls
```

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: rivven-network-policy
spec:
  podSelector:
    matchLabels:
      app: rivven
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: producer
      ports:
        - port: 9292
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: rivven
      ports:
        - port: 9393  # Inter-node
```

---

## Security Checklist

### Production Hardening

- [ ] Enable TLS for all client connections
- [ ] Set up automatic certificate rotation (cert-manager, ACME)
- [ ] Use mTLS for service-to-service auth
- [ ] Configure RBAC with least privilege
- [ ] Enable audit logging
- [ ] Set message size limits
- [ ] Configure rate limiting
- [ ] Enable encryption at rest
- [ ] Use Kubernetes secrets for credentials
- [ ] Apply network policies
- [ ] Regular security audits

---

## Next Steps

- [Architecture](architecture) — Security design details
- [Kubernetes](kubernetes) — Secure deployment
- [CDC](cdc) — Database security configuration

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

Rivven implements **31 security features** to protect your data:

| Category | Features |
|:---------|:---------|
| **Transport** | TLS 1.3, mTLS, QUIC encryption |
| **Authentication** | mTLS client certs, API tokens, SCRAM-SHA-256 |
| **Authorization** | RBAC, ACLs, topic-level permissions |
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
use rivven_client::RivvenClient;

let client = RivvenClient::builder()
    .addresses(["rivven.example.com:9292"])
    .tls(TlsConfig {
        ca_cert: include_bytes!("../ca.crt").to_vec(),
        // For mTLS:
        client_cert: Some(include_bytes!("../client.crt").to_vec()),
        client_key: Some(include_bytes!("../client.key").to_vec()),
    })
    .build()
    .await?;
```

### QUIC Transport

Inter-node communication uses QUIC with automatic encryption:

```yaml
cluster:
  transport: quic  # or tcp
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
rivvenctl auth token create --name service-a --ttl 365d
# Output: rvn_a1b2c3d4e5f6...

# List tokens
rivvenctl auth token list

# Revoke token
rivvenctl auth token revoke service-a
```

Use tokens in client:

```rust
let client = RivvenClient::builder()
    .addresses(["rivven.example.com:9292"])
    .auth_token("rvn_a1b2c3d4e5f6...")
    .build()
    .await?;
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

- **PBKDF2-HMAC-SHA256**: 4096 iterations for key derivation
- **32-byte random salt**: Per-user unique salt prevents rainbow tables
- **Constant-time comparison**: Prevents timing attacks
- **User enumeration prevention**: Fake salt/iterations for unknown users
- **Mutual authentication**: Server proves it knows the password too

#### Client Example

```rust
use rivven_client::{Request, Response};

// Step 1: Send client-first message
let client_first = format!("n,,n={},r={}", username, client_nonce);
let response = client.send(Request::ScramClientFirst { 
    message: Bytes::from(client_first) 
}).await?;

// Step 2: Process server-first, compute proof, send client-final
let Response::ScramServerFirst { message } = response else { ... };
// Parse r=<nonce>,s=<salt>,i=<iterations>
// Compute client proof and send client-final

// Step 3: Receive session on success
let Response::ScramServerFinal { session_id, expires_in, .. } = response else { ... };
```

---

## Authorization

### Role-Based Access Control (RBAC)

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

Fine-grained topic ACLs:

```bash
# Allow user to produce to specific topics
rivvenctl acl add --principal user:producer-app \
  --resource topic:orders \
  --action produce \
  --allow

# Allow group to consume
rivvenctl acl add --principal group:analytics-team \
  --resource topic:events-* \
  --action consume \
  --allow

# Deny access
rivvenctl acl add --principal user:suspicious \
  --resource topic:* \
  --action "*" \
  --deny

# List ACLs
rivvenctl acl list
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
    algorithm: aes-256-gcm
    key_source: kms  # file, kms, vault
    kms:
      provider: aws
      key_id: alias/rivven-data-key
      region: us-east-1
```

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
    
    - type: kafka
      bootstrap_servers: kafka:9092
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
  connection_timeout_secs: 30
  idle_timeout_secs: 300
```

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

- [Architecture](/rivven/docs/architecture) - Security design details
- [Kubernetes](/rivven/docs/kubernetes) - Secure deployment
- [CDC](/rivven/docs/cdc) - Database security configuration

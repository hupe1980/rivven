---
layout: default
title: OIDC Authentication
nav_order: 14
---

# OIDC Authentication
{: .no_toc }

Integrate with enterprise identity providers using OpenID Connect.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven supports **OpenID Connect (OIDC)** authentication, allowing seamless integration with enterprise identity providers:

- **Keycloak** — Open source identity management
- **Auth0** — Identity-as-a-service
- **Okta** — Enterprise identity platform
- **Azure AD** — Microsoft Entra ID
- **Google Workspace** — Google identity
- **Any OIDC-compliant provider**

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        OIDC AUTHENTICATION FLOW                          │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Client obtains JWT from Identity Provider                            │
│     ┌─────────┐         ┌─────────────────┐                              │
│     │ Client  │ ──────► │ Identity        │                              │
│     │         │ ◄────── │ Provider (IdP)  │  ← Returns JWT               │
│     └─────────┘         └─────────────────┘                              │
│                                                                          │
│  2. Client sends JWT to Rivven                                           │
│     ┌─────────┐         ┌─────────────────┐                              │
│     │ Client  │ ──JWT──►│ Rivven Server   │                              │
│     └─────────┘         └─────────────────┘                              │
│                                 │                                        │
│  3. Rivven validates JWT        │                                        │
│                                 ▼                                        │
│     ┌─────────────────────────────────────────────────────────────┐      │
│     │ a) Fetch JWKS from IdP (cached)                             │      │
│     │ b) Verify signature with public key                         │      │
│     │ c) Validate claims (iss, aud, exp, nbf)                     │      │
│     │ d) Extract username, groups, roles                          │      │
│     │ e) Create Rivven Principal for authorization                │      │
│     └─────────────────────────────────────────────────────────────┘      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Basic Setup

```yaml
# rivvend.yaml
authentication:
  method: oidc
  
  oidc:
    providers:
      - name: primary
        issuer: https://auth.example.com/realms/rivven
        audience: rivven-api
```

### Multi-Provider Configuration

Support multiple identity providers simultaneously:

```yaml
authentication:
  oidc:
    providers:
      # Keycloak for internal users
      - name: keycloak
        issuer: https://auth.example.com/realms/rivven
        audience: rivven-api
        username_claim: preferred_username
        groups_claim: groups
        roles_claim: realm_access.roles
      
      # Azure AD for external partners
      - name: azure-ad
        issuer: https://login.microsoftonline.com/{tenant-id}/v2.0
        audience: api://rivven
        username_claim: upn
        groups_claim: groups
      
      # Auth0 for SaaS customers
      - name: auth0
        issuer: https://your-tenant.auth0.com/
        audience: https://api.rivven.example.com
        username_claim: sub
        groups_claim: https://rivven.example.com/groups
```

### Provider Configuration Options

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `name` | string | required | Unique provider identifier |
| `issuer` | string | required | OIDC issuer URL (must match token `iss` claim) |
| `audience` | string | required | Expected audience (client_id or API identifier) |
| `username_claim` | string | `sub` | Claim to extract username from |
| `groups_claim` | string | — | Claim containing group memberships |
| `roles_claim` | string | — | Claim containing role assignments |
| `required_claims` | list | `[]` | Additional claims that must be present |
| `clock_skew_secs` | int | `60` | Tolerance for clock differences |
| `jwks_cache_ttl_secs` | int | `3600` | How long to cache JWKS (1 hour) |
| `algorithms` | list | `[RS256]` | Allowed signing algorithms |

---

## Identity Provider Setup

### Keycloak

1. **Create a Client**:
   - Client ID: `rivven-api`
   - Client Protocol: `openid-connect`
   - Access Type: `confidential` (for service accounts) or `public` (for user auth)

2. **Configure Client Scopes**:
   - Add `groups` scope to include group memberships
   - Map roles to token claims

3. **Rivven Configuration**:

```yaml
authentication:
  oidc:
    providers:
      - name: keycloak
        issuer: https://keycloak.example.com/realms/rivven
        audience: rivven-api
        username_claim: preferred_username
        groups_claim: groups
        roles_claim: realm_access.roles
```

### Azure AD / Microsoft Entra ID

1. **Register Application**:
   - App Registration → New Registration
   - Supported account types: Choose based on requirements
   - Redirect URI: Not needed for API access

2. **Configure API Permissions**:
   - Add `openid`, `profile`, `email` scopes
   - Configure group claims in Token Configuration

3. **Rivven Configuration**:

```yaml
authentication:
  oidc:
    providers:
      - name: azure
        issuer: https://login.microsoftonline.com/{tenant-id}/v2.0
        audience: api://rivven  # Or application client_id
        username_claim: upn     # Or 'email' or 'preferred_username'
        groups_claim: groups    # Requires group claims configuration
```

### Auth0

1. **Create API**:
   - APIs → Create API
   - Identifier: `https://api.rivven.example.com`

2. **Configure Application**:
   - Applications → Create Application
   - Add Rules/Actions for custom claims

3. **Rivven Configuration**:

```yaml
authentication:
  oidc:
    providers:
      - name: auth0
        issuer: https://your-tenant.auth0.com/
        audience: https://api.rivven.example.com
        username_claim: sub
        # Auth0 requires namespaced custom claims
        groups_claim: https://rivven.example.com/groups
```

### Okta

1. **Create Application**:
   - Applications → Create App Integration
   - OIDC → API Services (for machine-to-machine)

2. **Configure Authorization Server**:
   - Security → API → Authorization Servers
   - Add custom claims for groups

3. **Rivven Configuration**:

```yaml
authentication:
  oidc:
    providers:
      - name: okta
        issuer: https://your-org.okta.com/oauth2/default
        audience: api://rivven
        username_claim: sub
        groups_claim: groups
```

### Google Workspace

```yaml
authentication:
  oidc:
    providers:
      - name: google
        issuer: https://accounts.google.com
        audience: your-client-id.apps.googleusercontent.com
        username_claim: email
        # Google doesn't include groups by default
```

---

## Client Authentication

### Using Bearer Tokens

Clients authenticate by including the JWT in the `Authorization` header:

```bash
# Obtain token from IdP (example with Keycloak)
TOKEN=$(curl -s -X POST \
  "https://keycloak.example.com/realms/rivven/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=my-app" \
  -d "client_secret=my-secret" | jq -r '.access_token')

# Use with Rivven CLI
rivven --auth-token \"$TOKEN\" topic list

# Use with Rivven client library
rivven-client --bootstrap-servers rivven:9092 --auth-token "$TOKEN"
```

### SASL/OAUTHBEARER

For Kafka-compatible clients, Rivven supports SASL/OAUTHBEARER:

```properties
# Kafka client properties
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  oauth.token.endpoint.uri="https://keycloak.example.com/realms/rivven/protocol/openid-connect/token" \
  oauth.client.id="kafka-client" \
  oauth.client.secret="secret";
```

### Rust Client Example

```rust
use rivven_client::{ClientConfig, RivvenClient};

let config = ClientConfig::builder()
    .bootstrap_servers(vec!["rivven:9092".into()])
    .auth_token(token)  // JWT from IdP
    .build()?;

let client = RivvenClient::connect(config).await?;
```

---

## Claims Mapping

### Standard Claims

| JWT Claim | Rivven Mapping | Usage |
|:----------|:---------------|:------|
| `sub` | Principal ID | Unique user identifier |
| `iss` | Provider | Identifies which IdP issued token |
| `aud` | — | Validated against configured audience |
| `exp` | — | Token expiration check |
| `groups` | Groups | Authorization group memberships |

### Custom Claims

Map custom claims to Rivven attributes:

```yaml
authentication:
  oidc:
    providers:
      - name: custom
        issuer: https://idp.example.com
        audience: rivven
        # Nested claim access with dot notation
        roles_claim: realm_access.roles
        # Or use custom claim names
        groups_claim: https://example.com/claims/groups
```

### Claims Transformation

The extracted claims create a Rivven identity:

```rust
// Internal representation
RivvenIdentity {
    principal: "user@example.com",  // from username_claim
    groups: ["producers", "admins"], // from groups_claim
    roles: ["admin", "reader"],      // from roles_claim
    provider: "keycloak",
    raw_claims: { ... },
}
```

---

## Security Best Practices

### 1. Always Verify Audience

Ensure tokens are intended for Rivven:

```yaml
authentication:
  oidc:
    providers:
      - name: prod
        audience: rivven-production-api  # Must match exactly
```

### 2. Use Short-Lived Tokens

Configure IdP for short token lifetimes (5-15 minutes) with refresh tokens.

### 3. Require HTTPS

Always use TLS for OIDC communication:

```yaml
tls:
  enabled: true
  cert_file: /etc/rivven/certs/server.crt
  key_file: /etc/rivven/certs/server.key
```

### 4. Restrict Algorithms

Only allow secure algorithms:

```yaml
authentication:
  oidc:
    providers:
      - name: secure
        algorithms:
          - RS256
          - RS384
          - RS512
          - ES256
          - ES384
        # Never allow: HS256 (symmetric), none
```

### 5. Validate Required Claims

Ensure critical claims are present:

```yaml
authentication:
  oidc:
    providers:
      - name: strict
        required_claims:
          - email
          - email_verified
```

---

## Troubleshooting

### Token Validation Failures

**Error**: `Invalid issuer: expected X, got Y`

**Cause**: Token's `iss` claim doesn't match configured issuer

**Solution**: Verify issuer URL exactly matches (including trailing slashes)

---

**Error**: `Invalid audience`

**Cause**: Token's `aud` claim doesn't include configured audience

**Solution**: 
1. Check IdP configuration for audience
2. Ensure client requests correct audience/scope

---

**Error**: `Token expired`

**Cause**: Token's `exp` claim is in the past

**Solution**:
1. Obtain fresh token
2. Check clock sync between client and server
3. Increase `clock_skew_secs` if needed (not recommended for production)

---

### JWKS Fetch Errors

**Error**: `JWKS fetch error: connection refused`

**Cause**: Can't reach IdP's JWKS endpoint

**Solution**:
1. Verify network connectivity to IdP
2. Check firewall rules
3. Verify IdP is running

---

### Missing Claims

**Error**: `Missing claim: groups`

**Cause**: Token doesn't contain expected claim

**Solution**:
1. Configure IdP to include claim in tokens
2. Check client scopes request the claim
3. Verify claim mapping in Rivven config

---

## Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_auth_oidc_validations_total` | Total token validations |
| `rivven_auth_oidc_validation_errors_total` | Failed validations by error type |
| `rivven_auth_oidc_jwks_fetches_total` | JWKS fetch operations |
| `rivven_auth_oidc_jwks_cache_hits_total` | JWKS cache hits |
| `rivven_auth_oidc_token_expiry_seconds` | Token remaining lifetime histogram |

```yaml
# Alert on high authentication failures
- alert: RivvenOIDCAuthFailures
  expr: rate(rivven_auth_oidc_validation_errors_total[5m]) > 10
  labels:
    severity: warning
  annotations:
    summary: "High OIDC authentication failure rate"
```

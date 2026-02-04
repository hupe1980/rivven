---
layout: default
title: Cedar Authorization
nav_order: 15
---

# Cedar Authorization
{: .no_toc }

Policy-as-code authorization using AWS Cedar for fine-grained access control.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven supports **Cedar**, the policy language from AWS, for fine-grained authorization:

- **Formal verification** — Policies can be mathematically proven correct
- **Separation of concerns** — Policy changes without code changes  
- **Audit trail** — Every decision is explainable
- **Attribute-based** — Context-aware decisions (time, IP, resource attributes)

Cedar provides more expressive policies than traditional ACLs while maintaining high performance for real-time authorization decisions.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     CEDAR AUTHORIZATION FLOW                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐         ┌──────────────┐         ┌─────────────┐ │
│  │   Request    │────────►│    Cedar     │────────►│  Decision   │ │
│  │              │         │   Engine     │         │ Allow/Deny  │ │
│  │ • Principal  │         │              │         └─────────────┘ │
│  │ • Action     │         │ ┌──────────┐ │                         │
│  │ • Resource   │         │ │ Policies │ │                         │
│  │ • Context    │         │ └──────────┘ │                         │
│  └──────────────┘         │ ┌──────────┐ │                         │
│                           │ │ Entities │ │                         │
│                           │ └──────────┘ │                         │
│                           └──────────────┘                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Rivven Entity Model

Cedar uses entities (principals, resources) and actions. Rivven defines the following schema:

```cedar
namespace Rivven {
  // Principals
  entity User in [Group] {
    email?: String,
    roles: Set<String>,
    service_account: Bool,
  };
  
  entity Group in [Group];
  
  // Resources
  entity Topic {
    owner?: User,
    partitions: Long,
    replication_factor: Long,
    retention_ms: Long,
    name: String,
  };
  
  entity ConsumerGroup {
    name: String,
  };

  entity Schema {
    name: String,
    version: Long,
  };
  
  entity Cluster;
  
  // Topic actions
  action produce, consume, create, delete, alter, describe
    appliesTo { principal: [User, Group], resource: [Topic] };
  
  // Consumer group actions  
  action join, leave, commit, fetch_offsets
    appliesTo { principal: [User, Group], resource: [ConsumerGroup] };

  // Schema actions (for rivven-schema)
  action create, delete, alter, describe
    appliesTo { principal: [User, Group], resource: [Schema] };
  
  // Cluster actions
  action admin, alter_configs, describe_configs
    appliesTo { principal: [User, Group], resource: [Cluster] };
}
```

---

## Configuration

### Enable Cedar Authorization

```yaml
# rivvend.yaml
authorization:
  method: cedar
  
  cedar:
    # Policy files (loaded in order)
    policy_files:
      - /etc/rivven/policies/base.cedar
      - /etc/rivven/policies/team-specific.cedar
    
    # Schema file (optional but recommended)
    schema_file: /etc/rivven/policies/schema.cedarschema
    
    # Entity store (users, groups, resources)
    entities_file: /etc/rivven/policies/entities.json
    
    # Default decision when no policy matches
    default_decision: deny
    
    # Enable policy validation at startup
    validate_policies: true
```

### Policy Reload

Policies can be reloaded without restart:

```bash
# Send SIGHUP to reload policies
kill -HUP $(pidof rivvend)

# Or via admin API
rivven admin reload-policies
```

---

## Writing Policies

### Basic Policy Structure

```cedar
// permit or forbid
permit(
  principal,           // Who (User or Group)
  action,              // What (produce, consume, etc.)
  resource             // Where (Topic, ConsumerGroup, etc.)
) when {
  // Conditions (optional)
};
```

### Example Policies

#### Allow Producers Group

```cedar
// Members of "producers" group can produce to any topic
permit(
  principal in Rivven::Group::"producers",
  action == Rivven::Action::"produce",
  resource
);
```

#### Topic Ownership

```cedar
// Users can do anything to topics they own
permit(
  principal,
  action,
  resource
) when {
  resource has owner && resource.owner == principal
};
```

#### Prefix-Based Access

```cedar
// Team can access topics with their prefix
permit(
  principal in Rivven::Group::"team-orders",
  action in [Rivven::Action::"produce", Rivven::Action::"consume"],
  resource
) when {
  resource.name like "orders-*"
};
```

#### Consumer Group Membership

```cedar
// Only group members can commit offsets
permit(
  principal,
  action == Rivven::Action::"commit",
  resource
) when {
  principal in resource.members
};
```

#### IP-Based Restrictions

```cedar
// Deny access from untrusted networks
forbid(
  principal,
  action,
  resource
) when {
  !(context.ip_address.isInRange(ip("10.0.0.0/8"))) &&
  !(context.ip_address.isInRange(ip("192.168.0.0/16")))
};
```

#### Time-Based Access

```cedar
// Allow production access only during business hours
permit(
  principal in Rivven::Group::"operators",
  action == Rivven::Action::"delete",
  resource
) when {
  context.time.hour >= 9 && context.time.hour < 17 &&
  context.time.dayOfWeek >= 1 && context.time.dayOfWeek <= 5
};
```

#### Service Account Restrictions

```cedar
// Service accounts can only produce (not consume)
forbid(
  principal,
  action == Rivven::Action::"consume",
  resource
) when {
  principal.service_account == true
};
```

---

## Entity Management

### Entity File Format

Define users, groups, and their relationships:

```json
[
  {
    "uid": { "type": "Rivven::User", "id": "alice" },
    "attrs": {
      "email": "alice@example.com",
      "roles": ["admin", "developer"],
      "service_account": false
    },
    "parents": [
      { "type": "Rivven::Group", "id": "developers" },
      { "type": "Rivven::Group", "id": "admins" }
    ]
  },
  {
    "uid": { "type": "Rivven::User", "id": "order-service" },
    "attrs": {
      "service_account": true,
      "roles": []
    },
    "parents": [
      { "type": "Rivven::Group", "id": "services" }
    ]
  },
  {
    "uid": { "type": "Rivven::Group", "id": "developers" },
    "attrs": {},
    "parents": []
  },
  {
    "uid": { "type": "Rivven::Group", "id": "admins" },
    "attrs": {},
    "parents": [
      { "type": "Rivven::Group", "id": "developers" }
    ]
  },
  {
    "uid": { "type": "Rivven::Topic", "id": "orders" },
    "attrs": {
      "partitions": 12,
      "replication_factor": 3,
      "retention_ms": 604800000
    },
    "parents": []
  }
]
```

### Dynamic Entity Updates

Update entities via admin API:

```bash
# Add user to group
rivven admin cedar add-entity \
  --type User \
  --id bob \
  --parent Group::developers

# Update topic attributes
rivven admin cedar update-entity \
  --type Topic \
  --id orders \
  --attr partitions=24
```

---

## Context Variables

Cedar policies can access request context:

| Variable | Type | Description |
|:---------|:-----|:------------|
| `context.ip_address` | IP | Client IP address |
| `context.time` | DateTime | Request timestamp |
| `context.time.hour` | Long | Hour (0-23) |
| `context.time.dayOfWeek` | Long | Day (1=Mon, 7=Sun) |
| `context.tls_verified` | Bool | mTLS client verified |
| `context.client_id` | String | Client ID |
| `context.correlation_id` | String | Request correlation ID |

### Context Example

```cedar
// Require mTLS for sensitive topics
forbid(
  principal,
  action,
  resource
) when {
  resource.name like "pii-*" &&
  context.tls_verified == false
};
```

---

## Policy Organization

### Recommended Structure

```
/etc/rivven/policies/
├── schema.cedarschema     # Entity schema
├── entities.json          # Users, groups, topics
├── base.cedar             # Default deny, admin access
├── topics/
│   ├── production.cedar   # Production topic policies
│   └── development.cedar  # Dev topic policies
├── teams/
│   ├── orders.cedar       # Orders team policies
│   └── payments.cedar     # Payments team policies
└── security/
    ├── ip-restrictions.cedar
    └── time-restrictions.cedar
```

### Base Policy Template

```cedar
// base.cedar - Default policies for all Rivven deployments

// Default deny (defense in depth)
forbid(principal, action, resource);

// Admins can do everything
permit(
  principal in Rivven::Group::"admins",
  action,
  resource
);

// All authenticated users can describe topics
permit(
  principal,
  action == Rivven::Action::"describe",
  resource is Rivven::Topic
);

// All authenticated users can fetch their own offsets
permit(
  principal,
  action == Rivven::Action::"fetch_offsets",
  resource
) when {
  principal in resource.members
};
```

---

## Policy Validation

### Validate Before Deployment

```bash
# Validate policies against schema
cedar validate \
  --schema /etc/rivven/policies/schema.cedarschema \
  --policies /etc/rivven/policies/

# Test specific authorization request
cedar authorize \
  --schema schema.cedarschema \
  --policies policies/ \
  --entities entities.json \
  --principal 'Rivven::User::"alice"' \
  --action 'Rivven::Action::"produce"' \
  --resource 'Rivven::Topic::"orders"'
```

### Policy Analysis

```bash
# Find which policies apply to a request
cedar explain \
  --policies policies/ \
  --principal 'Rivven::User::"alice"' \
  --action 'Rivven::Action::"produce"' \
  --resource 'Rivven::Topic::"orders"'
```

---

## Migration from ACLs

### Map ACLs to Cedar Policies

| ACL Rule | Cedar Policy |
|:---------|:-------------|
| `User:alice ALLOW PRODUCE Topic:orders` | `permit(principal == Rivven::User::"alice", action == Rivven::Action::"produce", resource == Rivven::Topic::"orders");` |
| `Group:producers ALLOW PRODUCE Topic:*` | `permit(principal in Rivven::Group::"producers", action == Rivven::Action::"produce", resource is Rivven::Topic);` |
| `User:* ALLOW DESCRIBE Topic:*` | `permit(principal, action == Rivven::Action::"describe", resource is Rivven::Topic);` |

### Gradual Migration

1. **Audit mode**: Log Cedar decisions without enforcing
2. **Shadow mode**: Run both ACLs and Cedar, compare results
3. **Cutover**: Switch to Cedar enforcement
4. **Cleanup**: Remove ACL configuration

```yaml
authorization:
  method: cedar
  cedar:
    # Audit mode - log but don't enforce
    audit_mode: true
    
    # Compare with ACLs (for migration)
    compare_with_acls: true
```

---

## Audit Logging

Cedar decisions are logged for compliance:

```json
{
  "timestamp": "2026-01-27T10:30:00Z",
  "principal": "Rivven::User::alice",
  "action": "Rivven::Action::produce",
  "resource": "Rivven::Topic::orders",
  "decision": "allow",
  "policies_evaluated": ["team-orders.cedar:12"],
  "context": {
    "ip_address": "10.0.1.50",
    "client_id": "order-producer"
  }
}
```

---

## Optimization Tips

1. **Use specific policies**: More specific policies evaluate faster
2. **Limit entity hierarchy depth**: Deep group nesting impacts performance
3. **Cache entity lookups**: Configure entity caching in Rivven

```yaml
authorization:
  cedar:
    entity_cache_size: 10000
    entity_cache_ttl_secs: 300
```

---

## Schema Registry Integration

The Schema Registry (`rivven-schema`) supports Cedar authorization for schema operations.

### Enable Cedar for Schema Registry

Build with the `cedar` feature:

```bash
cargo build -p rivven-schema --features cedar
```

### Schema Actions

| Action | Description |
|:-------|:------------|
| `describe` | Get schemas, list subjects, check compatibility |
| `create` | Register new schemas |
| `alter` | Update subject configuration |
| `delete` | Delete schemas or subjects |

### Schema Policy Examples

```cedar
// Schema admins can do anything
permit(
  principal in Rivven::Group::"schema-admins",
  action,
  resource is Rivven::Schema
);

// Developers can read and register schemas for their team
permit(
  principal in Rivven::Group::"team-orders",
  action in [Rivven::Action::"describe", Rivven::Action::"create"],
  resource is Rivven::Schema
) when {
  resource.name like "orders-*"
};

// Prevent deleting production schemas
forbid(
  principal,
  action == Rivven::Action::"delete",
  resource is Rivven::Schema
) when {
  resource.name like "*-prod-*"
};
```

### Programmatic Setup

```rust
use rivven_schema::{SchemaServer, ServerConfig, AuthConfig, CedarAuthorizer};
use rivven_core::AuthManager;
use std::sync::Arc;

// Create Cedar authorizer
let authorizer = Arc::new(CedarAuthorizer::new()?);

// Add schema policies
authorizer.add_policies(r#"
permit(
  principal in Rivven::Group::"schema-admins",
  action,
  resource is Rivven::Schema
);

permit(
  principal,
  action == Rivven::Action::"describe",
  resource is Rivven::Schema
);
"#)?;

// Add users and groups
authorizer.add_group("schema-admins", &[])?;
authorizer.add_user("alice", Some("alice@example.com"), &["admin"], &["schema-admins"], false)?;

// Configure server with Cedar
let config = ServerConfig::default()
    .with_auth(AuthConfig::required().with_cedar());

let server = SchemaServer::with_cedar(registry, config, auth_manager, authorizer);
```

---

## Troubleshooting

### Debug Authorization Decisions

```bash
# Enable debug logging
export RUST_LOG=rivven::cedar=debug

# Check why a request was denied
rivven admin cedar explain \
  --principal alice \
  --action produce \
  --resource orders
```

### Common Issues

**Problem**: Policy not matching expected requests

**Solution**: Check entity hierarchy and group memberships:

```bash
rivven admin cedar show-entity --type User --id alice
```

---

**Problem**: Performance degradation with many policies

**Solution**: 
1. Consolidate overlapping policies
2. Use policy templates
3. Enable policy compilation cache

---

## Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_authz_cedar_decisions_total` | Total authorization decisions |
| `rivven_authz_cedar_decision_duration_seconds` | Decision latency histogram |
| `rivven_authz_cedar_policies_loaded` | Number of policies loaded |
| `rivven_authz_cedar_entities_loaded` | Number of entities loaded |

```yaml
# Alert on authorization denials spike
- alert: RivvenCedarDenialsHigh
  expr: rate(rivven_authz_cedar_decisions_total{decision="deny"}[5m]) > 100
  labels:
    severity: warning
```

//! Cedar-based Authorization Engine for Rivven
//!
//! This module provides fine-grained, policy-as-code authorization using
//! [Cedar](https://www.cedarpolicy.com/), the policy language from AWS.
//!
//! ## Why Cedar?
//!
//! - **Formal verification**: Policies can be mathematically proven correct
//! - **Separation of concerns**: Policy changes without code changes
//! - **Audit trail**: Every decision is explainable
//! - **Attribute-based**: Context-aware decisions (time, IP, resource attributes)
//!
//! ## Rivven Entity Model
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                     RIVVEN CEDAR SCHEMA                             │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                     │
//! │  namespace Rivven {                                                 │
//! │    entity User in [Group] {                                         │
//! │      email?: String,                                               │
//! │      roles: Set<String>,                                           │
//! │      service_account: Bool,                                        │
//! │    };                                                              │
//! │                                                                     │
//! │    entity Group in [Group];                                        │
//! │                                                                     │
//! │    entity Topic {                                                  │
//! │      owner?: User,                                                 │
//! │      partitions: Long,                                             │
//! │      replication_factor: Long,                                     │
//! │      retention_ms: Long,                                           │
//! │    };                                                              │
//! │                                                                     │
//! │    entity ConsumerGroup {                                          │
//! │      members: Set<User>,                                           │
//! │    };                                                              │
//! │                                                                     │
//! │    entity Cluster;                                                 │
//! │                                                                     │
//! │    action produce, consume, create, delete, alter, describe        │
//! │      appliesTo { principal: [User, Group], resource: [Topic] };    │
//! │                                                                     │
//! │    action join, leave, commit, fetch_offsets                       │
//! │      appliesTo { principal: [User, Group], resource: [ConsumerGroup] };│
//! │                                                                     │
//! │    action admin                                                     │
//! │      appliesTo { principal: [User, Group], resource: [Cluster] };  │
//! │  }                                                                  │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example Policies
//!
//! ```cedar
//! // Allow users to produce to topics they own
//! permit(
//!   principal,
//!   action == Rivven::Action::"produce",
//!   resource
//! ) when {
//!   resource.owner == principal
//! };
//!
//! // Allow members of "producers" group to produce to any topic starting with "events-"
//! permit(
//!   principal in Rivven::Group::"producers",
//!   action == Rivven::Action::"produce",
//!   resource
//! ) when {
//!   resource.name.startsWith("events-")
//! };
//!
//! // Deny access from untrusted IPs
//! forbid(
//!   principal,
//!   action,
//!   resource
//! ) when {
//!   context.ip_address.isLoopback() == false &&
//!   context.ip_address.isInRange(ip("10.0.0.0/8")) == false
//! };
//! ```

#[cfg(feature = "cedar")]
mod cedar_impl {
    use cedar_policy::{
        Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid,
        Policy, PolicySet, Request, Schema, ValidationMode,
    };
    use parking_lot::RwLock;
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};
    use std::str::FromStr;
    use thiserror::Error;
    use tracing::{debug, info};

    // ============================================================================
    // Error Types
    // ============================================================================

    #[derive(Error, Debug)]
    pub enum CedarError {
        #[error("Policy parse error: {0}")]
        PolicyParse(String),

        #[error("Schema error: {0}")]
        Schema(String),

        #[error("Validation error: {0}")]
        Validation(String),

        #[error("Entity error: {0}")]
        Entity(String),

        #[error("Request error: {0}")]
        Request(String),

        #[error("Authorization denied: {principal} cannot {action} on {resource}")]
        Denied {
            principal: String,
            action: String,
            resource: String,
        },

        #[error("Internal error: {0}")]
        Internal(String),
    }

    pub type CedarResult<T> = Result<T, CedarError>;

    // ============================================================================
    // Rivven Actions
    // ============================================================================

    /// Actions that can be performed in Rivven
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum RivvenAction {
        // Topic actions
        Produce,
        Consume,
        Create,
        Delete,
        Alter,
        Describe,

        // Consumer group actions
        Join,
        Leave,
        Commit,
        FetchOffsets,

        // Cluster actions
        Admin,
        AlterConfigs,
        DescribeConfigs,
    }

    impl RivvenAction {
        pub fn as_str(&self) -> &'static str {
            match self {
                Self::Produce => "produce",
                Self::Consume => "consume",
                Self::Create => "create",
                Self::Delete => "delete",
                Self::Alter => "alter",
                Self::Describe => "describe",
                Self::Join => "join",
                Self::Leave => "leave",
                Self::Commit => "commit",
                Self::FetchOffsets => "fetch_offsets",
                Self::Admin => "admin",
                Self::AlterConfigs => "alter_configs",
                Self::DescribeConfigs => "describe_configs",
            }
        }

        fn to_entity_uid(self) -> EntityUid {
            EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::Action").unwrap(),
                EntityId::from_str(self.as_str()).unwrap(),
            )
        }
    }

    // ============================================================================
    // Resource Types
    // ============================================================================

    /// Types of resources in Rivven
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(tag = "type", content = "name")]
    pub enum RivvenResource {
        Topic(String),
        ConsumerGroup(String),
        Schema(String),
        Cluster,
    }

    impl RivvenResource {
        fn type_name(&self) -> &'static str {
            match self {
                Self::Topic(_) => "Rivven::Topic",
                Self::ConsumerGroup(_) => "Rivven::ConsumerGroup",
                Self::Schema(_) => "Rivven::Schema",
                Self::Cluster => "Rivven::Cluster",
            }
        }

        fn id(&self) -> &str {
            match self {
                Self::Topic(name) => name,
                Self::ConsumerGroup(name) => name,
                Self::Schema(name) => name,
                Self::Cluster => "default",
            }
        }

        fn to_entity_uid(&self) -> EntityUid {
            EntityUid::from_type_name_and_id(
                EntityTypeName::from_str(self.type_name()).unwrap(),
                EntityId::from_str(self.id()).unwrap(),
            )
        }
    }

    // ============================================================================
    // Authorization Context
    // ============================================================================

    /// Context for authorization decisions
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct AuthzContext {
        /// Client IP address
        pub ip_address: Option<String>,

        /// Request timestamp (ISO 8601)
        pub timestamp: String,

        /// TLS client certificate subject (if mTLS)
        pub tls_subject: Option<String>,

        /// Additional context attributes
        #[serde(flatten)]
        pub extra: HashMap<String, serde_json::Value>,
    }

    impl Default for AuthzContext {
        fn default() -> Self {
            Self {
                ip_address: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
                tls_subject: None,
                extra: HashMap::new(),
            }
        }
    }

    impl AuthzContext {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_ip(mut self, ip: impl Into<String>) -> Self {
            self.ip_address = Some(ip.into());
            self
        }

        pub fn with_tls_subject(mut self, subject: impl Into<String>) -> Self {
            self.tls_subject = Some(subject.into());
            self
        }

        pub fn with_attr(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
            self.extra.insert(key.into(), value);
            self
        }

        fn to_cedar_context(&self) -> CedarResult<Context> {
            // Cedar does not allow null values, so we must create a JSON object
            // with only the non-null values
            let mut context_map = serde_json::Map::new();

            // Add timestamp (always present)
            context_map.insert("timestamp".to_string(), serde_json::json!(self.timestamp));

            // Add optional fields only if they have values
            if let Some(ip) = &self.ip_address {
                context_map.insert("ip_address".to_string(), serde_json::json!(ip));
            }

            if let Some(tls) = &self.tls_subject {
                context_map.insert("tls_subject".to_string(), serde_json::json!(tls));
            }

            // Add extra context, filtering out null values
            for (key, value) in &self.extra {
                if !value.is_null() {
                    context_map.insert(key.clone(), value.clone());
                }
            }

            let json = serde_json::Value::Object(context_map);

            Context::from_json_value(json, None)
                .map_err(|e| CedarError::Request(format!("Invalid context: {}", e)))
        }
    }

    // ============================================================================
    // Authorization Decision
    // ============================================================================

    /// Result of an authorization check
    #[derive(Debug, Clone)]
    pub struct AuthzDecision {
        /// Whether access is allowed
        pub allowed: bool,

        /// Policies that permitted the action
        pub satisfied_policies: Vec<String>,

        /// Policies that denied the action
        pub denied_policies: Vec<String>,

        /// Errors during evaluation
        pub errors: Vec<String>,

        /// Diagnostics for debugging
        pub diagnostics: Option<String>,
    }

    impl AuthzDecision {
        pub fn allowed(satisfied: Vec<String>) -> Self {
            Self {
                allowed: true,
                satisfied_policies: satisfied,
                denied_policies: vec![],
                errors: vec![],
                diagnostics: None,
            }
        }

        pub fn denied(denied: Vec<String>) -> Self {
            Self {
                allowed: false,
                satisfied_policies: vec![],
                denied_policies: denied,
                errors: vec![],
                diagnostics: None,
            }
        }
    }

    // ============================================================================
    // Cedar Authorizer
    // ============================================================================

    /// Cedar-based authorization engine
    pub struct CedarAuthorizer {
        /// Cedar schema for validation
        schema: Option<Schema>,

        /// Policy set
        policies: RwLock<PolicySet>,

        /// Entity store
        entities: RwLock<Entities>,

        /// Whether to validate policies against schema
        validate_policies: bool,
    }

    impl CedarAuthorizer {
        /// Create a new authorizer with default schema
        pub fn new() -> CedarResult<Self> {
            let schema = Self::default_schema()?;

            Ok(Self {
                schema: Some(schema),
                policies: RwLock::new(PolicySet::new()),
                entities: RwLock::new(Entities::empty()),
                validate_policies: true,
            })
        }

        /// Create without schema validation (for testing)
        pub fn new_without_schema() -> Self {
            Self {
                schema: None,
                policies: RwLock::new(PolicySet::new()),
                entities: RwLock::new(Entities::empty()),
                validate_policies: false,
            }
        }

        /// Default Rivven Cedar schema
        fn default_schema() -> CedarResult<Schema> {
            let schema_src = r#"
{
  "Rivven": {
    "entityTypes": {
      "User": {
        "memberOfTypes": ["Group"],
        "shape": {
          "type": "Record",
          "attributes": {
            "email": { "type": "String", "required": false },
            "roles": { "type": "Set", "element": { "type": "String" } },
            "service_account": { "type": "Boolean" }
          }
        }
      },
      "Group": {
        "memberOfTypes": ["Group"]
      },
      "Topic": {
        "shape": {
          "type": "Record",
          "attributes": {
            "owner": { "type": "Entity", "name": "Rivven::User", "required": false },
            "partitions": { "type": "Long" },
            "replication_factor": { "type": "Long" },
            "retention_ms": { "type": "Long" },
            "name": { "type": "String" }
          }
        }
      },
      "ConsumerGroup": {
        "shape": {
          "type": "Record",
          "attributes": {
            "name": { "type": "String" }
          }
        }
      },
      "Schema": {
        "shape": {
          "type": "Record",
          "attributes": {
            "name": { "type": "String" },
            "version": { "type": "Long" }
          }
        }
      },
      "Cluster": {}
    },
    "actions": {
      "produce": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Topic"]
        }
      },
      "consume": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Topic"]
        }
      },
      "create": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Topic", "Schema"]
        }
      },
      "delete": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Topic", "ConsumerGroup", "Schema"]
        }
      },
      "alter": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Topic", "Schema"]
        }
      },
      "describe": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Topic", "ConsumerGroup", "Schema", "Cluster"]
        }
      },
      "join": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["ConsumerGroup"]
        }
      },
      "leave": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["ConsumerGroup"]
        }
      },
      "commit": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["ConsumerGroup"]
        }
      },
      "fetch_offsets": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["ConsumerGroup"]
        }
      },
      "admin": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Cluster"]
        }
      },
      "alter_configs": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Cluster"]
        }
      },
      "describe_configs": {
        "appliesTo": {
          "principalTypes": ["User", "Group"],
          "resourceTypes": ["Cluster"]
        }
      }
    }
  }
}
"#;

            Schema::from_json_str(schema_src)
                .map_err(|e| CedarError::Schema(format!("Invalid schema: {:?}", e)))
        }

        /// Add a policy from Cedar source
        pub fn add_policy(&self, id: &str, policy_src: &str) -> CedarResult<()> {
            let policy = Policy::parse(Some(cedar_policy::PolicyId::new(id)), policy_src)
                .map_err(|e| CedarError::PolicyParse(format!("{:?}", e)))?;

            // Validate against schema if enabled
            if self.validate_policies {
                if let Some(schema) = &self.schema {
                    let mut temp_set = PolicySet::new();
                    temp_set.add(policy.clone()).map_err(|e| {
                        CedarError::PolicyParse(format!("Duplicate policy ID: {:?}", e))
                    })?;

                    let validator = cedar_policy::Validator::new(schema.clone());
                    let result = validator.validate(&temp_set, ValidationMode::Strict);

                    if !result.validation_passed() {
                        let errors: Vec<String> = result
                            .validation_errors()
                            .map(|e| format!("{:?}", e))
                            .collect();
                        return Err(CedarError::Validation(errors.join("; ")));
                    }
                }
            }

            let mut policies = self.policies.write();
            policies
                .add(policy)
                .map_err(|e| CedarError::PolicyParse(format!("Failed to add policy: {:?}", e)))?;

            info!("Added Cedar policy: {}", id);
            Ok(())
        }

        /// Add multiple policies from Cedar source
        pub fn add_policies(&self, policies_src: &str) -> CedarResult<()> {
            let parsed = PolicySet::from_str(policies_src)
                .map_err(|e| CedarError::PolicyParse(format!("{:?}", e)))?;

            // Validate against schema if enabled
            if self.validate_policies {
                if let Some(schema) = &self.schema {
                    let validator = cedar_policy::Validator::new(schema.clone());
                    let result = validator.validate(&parsed, ValidationMode::Strict);

                    if !result.validation_passed() {
                        let errors: Vec<String> = result
                            .validation_errors()
                            .map(|e| format!("{:?}", e))
                            .collect();
                        return Err(CedarError::Validation(errors.join("; ")));
                    }
                }
            }

            let mut policies = self.policies.write();
            for policy in parsed.policies() {
                policies.add(policy.clone()).map_err(|e| {
                    CedarError::PolicyParse(format!("Failed to add policy: {:?}", e))
                })?;
            }

            Ok(())
        }

        /// Remove a policy by ID
        pub fn remove_policy(&self, id: &str) -> CedarResult<()> {
            // Cedar PolicySet doesn't support removal directly
            // We need to rebuild without the policy
            let mut policies = self.policies.write();
            let policy_id = cedar_policy::PolicyId::new(id);

            // Create a new PolicySet without the specified policy
            let new_policies = policies
                .policies()
                .filter(|p| p.id() != &policy_id)
                .cloned()
                .collect::<Vec<_>>();

            let mut new_set = PolicySet::new();
            for policy in new_policies {
                new_set.add(policy).ok();
            }

            *policies = new_set;
            info!("Removed Cedar policy: {}", id);
            Ok(())
        }

        /// F-041 fix: Upsert a single entity incrementally without cloning all existing entities.
        /// Uses Cedar's `upsert_entities` API which re-computes TC but avoids O(n) clone.
        fn upsert_entity(&self, entity: Entity) -> CedarResult<()> {
            let mut guard = self.entities.write();
            let current = std::mem::replace(&mut *guard, Entities::empty());
            match current.upsert_entities([entity], None) {
                Ok(updated) => {
                    *guard = updated;
                    Ok(())
                }
                Err(e) => Err(CedarError::Entity(format!(
                    "Failed to upsert entity: {:?}",
                    e
                ))),
            }
        }

        /// Add a user entity
        pub fn add_user(
            &self,
            username: &str,
            email: Option<&str>,
            roles: &[&str],
            groups: &[&str],
            is_service_account: bool,
        ) -> CedarResult<()> {
            let uid = EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::User").unwrap(),
                EntityId::from_str(username).unwrap(),
            );

            // Build parent groups
            let parents: HashSet<EntityUid> = groups
                .iter()
                .map(|g| {
                    EntityUid::from_type_name_and_id(
                        EntityTypeName::from_str("Rivven::Group").unwrap(),
                        EntityId::from_str(g).unwrap(),
                    )
                })
                .collect();

            // Build attributes
            let mut attrs = HashMap::new();
            if let Some(e) = email {
                attrs.insert(
                    "email".to_string(),
                    cedar_policy::RestrictedExpression::new_string(e.to_string()),
                );
            }

            let roles_set: Vec<_> = roles
                .iter()
                .map(|r| cedar_policy::RestrictedExpression::new_string(r.to_string()))
                .collect();
            attrs.insert(
                "roles".to_string(),
                cedar_policy::RestrictedExpression::new_set(roles_set),
            );

            attrs.insert(
                "service_account".to_string(),
                cedar_policy::RestrictedExpression::new_bool(is_service_account),
            );

            let entity = Entity::new(uid, attrs, parents)
                .map_err(|e| CedarError::Entity(format!("Invalid entity: {:?}", e)))?;

            self.upsert_entity(entity)?;

            debug!("Added user entity: {}", username);
            Ok(())
        }

        /// Add a group entity
        pub fn add_group(&self, name: &str, parent_groups: &[&str]) -> CedarResult<()> {
            let uid = EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::Group").unwrap(),
                EntityId::from_str(name).unwrap(),
            );

            let parents: HashSet<EntityUid> = parent_groups
                .iter()
                .map(|g| {
                    EntityUid::from_type_name_and_id(
                        EntityTypeName::from_str("Rivven::Group").unwrap(),
                        EntityId::from_str(g).unwrap(),
                    )
                })
                .collect();

            let entity = Entity::new_no_attrs(uid, parents);

            self.upsert_entity(entity)?;

            debug!("Added group entity: {}", name);
            Ok(())
        }

        /// Add a topic entity
        pub fn add_topic(
            &self,
            name: &str,
            owner: Option<&str>,
            partitions: i64,
            replication_factor: i64,
            retention_ms: i64,
        ) -> CedarResult<()> {
            let uid = EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::Topic").unwrap(),
                EntityId::from_str(name).unwrap(),
            );

            let mut attrs = HashMap::new();
            attrs.insert(
                "name".to_string(),
                cedar_policy::RestrictedExpression::new_string(name.to_string()),
            );

            if let Some(o) = owner {
                let owner_uid = EntityUid::from_type_name_and_id(
                    EntityTypeName::from_str("Rivven::User").unwrap(),
                    EntityId::from_str(o).unwrap(),
                );
                attrs.insert(
                    "owner".to_string(),
                    cedar_policy::RestrictedExpression::new_entity_uid(owner_uid),
                );
            }

            attrs.insert(
                "partitions".to_string(),
                cedar_policy::RestrictedExpression::new_long(partitions),
            );
            attrs.insert(
                "replication_factor".to_string(),
                cedar_policy::RestrictedExpression::new_long(replication_factor),
            );
            attrs.insert(
                "retention_ms".to_string(),
                cedar_policy::RestrictedExpression::new_long(retention_ms),
            );

            let entity = Entity::new(uid, attrs, HashSet::new())
                .map_err(|e| CedarError::Entity(format!("Invalid entity: {:?}", e)))?;

            self.upsert_entity(entity)?;

            debug!("Added topic entity: {}", name);
            Ok(())
        }

        /// Add a consumer group entity
        pub fn add_consumer_group(&self, name: &str) -> CedarResult<()> {
            let uid = EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::ConsumerGroup").unwrap(),
                EntityId::from_str(name).unwrap(),
            );

            let mut attrs = HashMap::new();
            attrs.insert(
                "name".to_string(),
                cedar_policy::RestrictedExpression::new_string(name.to_string()),
            );

            let entity = Entity::new(uid, attrs, HashSet::new())
                .map_err(|e| CedarError::Entity(format!("Invalid entity: {:?}", e)))?;

            self.upsert_entity(entity)?;

            debug!("Added consumer group entity: {}", name);
            Ok(())
        }

        /// Add a schema entity
        pub fn add_schema(&self, name: &str, version: i64) -> CedarResult<()> {
            let uid = EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::Schema").unwrap(),
                EntityId::from_str(name).unwrap(),
            );

            let mut attrs = HashMap::new();
            attrs.insert(
                "name".to_string(),
                cedar_policy::RestrictedExpression::new_string(name.to_string()),
            );
            attrs.insert(
                "version".to_string(),
                cedar_policy::RestrictedExpression::new_long(version),
            );

            let entity = Entity::new(uid, attrs, HashSet::new())
                .map_err(|e| CedarError::Entity(format!("Invalid entity: {:?}", e)))?;

            self.upsert_entity(entity)?;

            debug!("Added schema entity: {} (version {})", name, version);
            Ok(())
        }

        /// Check if an action is authorized
        pub fn is_authorized(
            &self,
            principal: &str,
            action: RivvenAction,
            resource: &RivvenResource,
            context: &AuthzContext,
        ) -> CedarResult<AuthzDecision> {
            let principal_uid = EntityUid::from_type_name_and_id(
                EntityTypeName::from_str("Rivven::User").unwrap(),
                EntityId::from_str(principal).unwrap(),
            );

            let action_uid = action.to_entity_uid();
            let resource_uid = resource.to_entity_uid();
            let cedar_context = context.to_cedar_context()?;

            let request = Request::new(
                principal_uid.clone(),
                action_uid.clone(),
                resource_uid.clone(),
                cedar_context,
                None,
            )
            .map_err(|e| CedarError::Request(format!("Invalid request: {:?}", e)))?;

            let authorizer = Authorizer::new();
            let policies = self.policies.read();
            let entities = self.entities.read();

            let response = authorizer.is_authorized(&request, &policies, &entities);

            let decision = match response.decision() {
                Decision::Allow => {
                    let satisfied: Vec<String> = response
                        .diagnostics()
                        .reason()
                        .map(|id| id.to_string())
                        .collect();
                    AuthzDecision::allowed(satisfied)
                }
                Decision::Deny => {
                    let denied: Vec<String> = response
                        .diagnostics()
                        .reason()
                        .map(|id| id.to_string())
                        .collect();

                    let errors: Vec<String> = response
                        .diagnostics()
                        .errors()
                        .map(|e| format!("{:?}", e))
                        .collect();

                    AuthzDecision {
                        allowed: false,
                        satisfied_policies: vec![],
                        denied_policies: denied,
                        errors,
                        diagnostics: None,
                    }
                }
            };

            debug!(
                "Authorization: {} {} {} -> {}",
                principal,
                action.as_str(),
                resource.id(),
                if decision.allowed { "ALLOW" } else { "DENY" }
            );

            Ok(decision)
        }

        /// Check authorization and return error if denied
        pub fn authorize(
            &self,
            principal: &str,
            action: RivvenAction,
            resource: &RivvenResource,
            context: &AuthzContext,
        ) -> CedarResult<()> {
            let decision = self.is_authorized(principal, action, resource, context)?;

            if decision.allowed {
                Ok(())
            } else {
                Err(CedarError::Denied {
                    principal: principal.to_string(),
                    action: action.as_str().to_string(),
                    resource: format!("{:?}", resource),
                })
            }
        }

        /// Load default policies for common use cases
        pub fn load_default_policies(&self) -> CedarResult<()> {
            // Super admin has all permissions
            self.add_policy(
                "super-admin",
                r#"
permit(
  principal in Rivven::Group::"admins",
  action,
  resource
);
"#,
            )?;

            // All users can describe topics
            self.add_policy(
                "describe-topics",
                r#"
permit(
  principal,
  action == Rivven::Action::"describe",
  resource is Rivven::Topic
);
"#,
            )?;

            // All users can describe consumer groups
            self.add_policy(
                "describe-consumer-groups",
                r#"
permit(
  principal,
  action == Rivven::Action::"describe",
  resource is Rivven::ConsumerGroup
);
"#,
            )?;

            info!("Loaded default Cedar policies");
            Ok(())
        }
    }

    impl Default for CedarAuthorizer {
        fn default() -> Self {
            Self::new().expect("Failed to create default authorizer")
        }
    }

    // ============================================================================
    // Tests
    // ============================================================================

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_create_authorizer() {
            let authz = CedarAuthorizer::new().unwrap();
            assert!(authz.schema.is_some());
        }

        #[test]
        fn test_add_simple_policy() {
            let authz = CedarAuthorizer::new_without_schema();

            authz
                .add_policy(
                    "test-policy",
                    r#"
permit(
  principal == Rivven::User::"alice",
  action == Rivven::Action::"produce",
  resource == Rivven::Topic::"orders"
);
"#,
                )
                .unwrap();
        }

        #[test]
        fn test_add_user_and_authorize() {
            let authz = CedarAuthorizer::new_without_schema();

            // Add admin policy
            authz
                .add_policy(
                    "admin-all",
                    r#"
permit(
  principal in Rivven::Group::"admins",
  action,
  resource
);
"#,
                )
                .unwrap();

            // Add group and user
            authz.add_group("admins", &[]).unwrap();
            authz
                .add_user(
                    "alice",
                    Some("alice@example.com"),
                    &["admin"],
                    &["admins"],
                    false,
                )
                .unwrap();

            // Add topic
            authz
                .add_topic("orders", Some("alice"), 3, 2, 604800000)
                .unwrap();

            // Check authorization
            let ctx = AuthzContext::new().with_ip("127.0.0.1");
            let decision = authz
                .is_authorized(
                    "alice",
                    RivvenAction::Produce,
                    &RivvenResource::Topic("orders".to_string()),
                    &ctx,
                )
                .unwrap();

            assert!(decision.allowed);
        }

        #[test]
        fn test_deny_unauthorized() {
            let authz = CedarAuthorizer::new_without_schema();

            // Add a restrictive policy - only admins can produce
            authz
                .add_policy(
                    "only-admins-produce",
                    r#"
permit(
  principal in Rivven::Group::"admins",
  action == Rivven::Action::"produce",
  resource is Rivven::Topic
);
"#,
                )
                .unwrap();

            // Add a non-admin user
            authz
                .add_user("bob", Some("bob@example.com"), &["user"], &[], false)
                .unwrap();

            // Add topic
            authz.add_topic("orders", None, 3, 2, 604800000).unwrap();

            // Check authorization - should be denied
            let ctx = AuthzContext::new();
            let decision = authz
                .is_authorized(
                    "bob",
                    RivvenAction::Produce,
                    &RivvenResource::Topic("orders".to_string()),
                    &ctx,
                )
                .unwrap();

            assert!(!decision.allowed);
        }

        #[test]
        fn test_context_attributes() {
            let ctx = AuthzContext::new()
                .with_ip("192.168.1.100")
                .with_tls_subject("CN=client,O=Rivven")
                .with_attr("custom_field", serde_json::json!("custom_value"));

            assert_eq!(ctx.ip_address, Some("192.168.1.100".to_string()));
            assert_eq!(ctx.tls_subject, Some("CN=client,O=Rivven".to_string()));
            assert!(ctx.extra.contains_key("custom_field"));
        }
    }
}

#[cfg(feature = "cedar")]
pub use cedar_impl::*;

#[cfg(not(feature = "cedar"))]
mod no_cedar {
    //! Stub module when Cedar feature is disabled

    use std::collections::HashMap;
    use thiserror::Error;

    #[derive(Error, Debug)]
    pub enum CedarError {
        #[error("Cedar authorization not enabled. Build with 'cedar' feature.")]
        NotEnabled,
    }

    pub type CedarResult<T> = Result<T, CedarError>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum RivvenAction {
        Produce,
        Consume,
        Create,
        Delete,
        Alter,
        Describe,
        Join,
        Leave,
        Commit,
        FetchOffsets,
        Admin,
        AlterConfigs,
        DescribeConfigs,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub enum RivvenResource {
        Topic(String),
        ConsumerGroup(String),
        Schema(String),
        Cluster,
    }

    #[derive(Debug, Clone, Default)]
    pub struct AuthzContext {
        pub ip_address: Option<String>,
        pub timestamp: String,
        pub tls_subject: Option<String>,
        pub extra: HashMap<String, serde_json::Value>,
    }

    impl AuthzContext {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_ip(self, _ip: impl Into<String>) -> Self {
            self
        }

        pub fn with_tls_subject(self, _subject: impl Into<String>) -> Self {
            self
        }
    }

    pub struct CedarAuthorizer;

    impl CedarAuthorizer {
        pub fn new() -> CedarResult<Self> {
            Err(CedarError::NotEnabled)
        }

        pub fn authorize(
            &self,
            _principal: &str,
            _action: RivvenAction,
            _resource: &RivvenResource,
            _context: &AuthzContext,
        ) -> CedarResult<()> {
            Err(CedarError::NotEnabled)
        }
    }
}

#[cfg(not(feature = "cedar"))]
pub use no_cedar::*;

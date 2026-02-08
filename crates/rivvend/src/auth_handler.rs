//! Authenticated Request Handler
//!
//! This module provides an authentication-aware request handler that:
//! - Manages authentication for connections
//! - Enforces ACL/RBAC permissions on each request
//! - Integrates with the base RequestHandler for actual operations
//!
//! ## Security Design
//!
//! - All requests require authentication when `require_authentication` is enabled
//! - Each operation is checked against the session's permissions
//! - Sessions are validated on every request (expiration, principal enabled)
//! - Audit logging for all access attempts

use crate::handler::RequestHandler;
use crate::protocol::{Request, Response};
use bytes::Bytes;
use rivven_core::{
    AuthManager, AuthSession, Permission, ResourceType, SaslPlainAuth, SaslScramAuth, ScramState,
};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Connection authentication state
#[derive(Debug, Clone)]
pub enum ConnectionAuth {
    /// Not yet authenticated
    Unauthenticated,
    /// SCRAM-SHA-256 handshake in progress
    ScramInProgress(ScramState),
    /// Authenticated with a session
    Authenticated(AuthSession),
    /// Anonymous access (when auth not required)
    Anonymous,
}

/// Authenticated request handler that enforces permissions
pub struct AuthenticatedHandler {
    /// Base request handler
    handler: RequestHandler,
    /// Authentication manager
    auth_manager: Arc<AuthManager>,
    /// SASL/PLAIN handler
    sasl_plain: SaslPlainAuth,
    /// SASL/SCRAM-SHA-256 handler
    sasl_scram: SaslScramAuth,
    /// Whether authentication is required
    require_auth: bool,
}

impl AuthenticatedHandler {
    /// Create a new authenticated handler
    pub fn new(
        handler: RequestHandler,
        auth_manager: Arc<AuthManager>,
        require_auth: bool,
    ) -> Self {
        let sasl_plain = SaslPlainAuth::new(auth_manager.clone());
        let sasl_scram = SaslScramAuth::new(auth_manager.clone());
        Self {
            handler,
            auth_manager,
            sasl_plain,
            sasl_scram,
            require_auth,
        }
    }

    /// Handle a request with authentication and authorization
    pub async fn handle(
        &self,
        request: Request,
        connection_auth: &mut ConnectionAuth,
        client_ip: &str,
    ) -> Response {
        // Handle authentication requests first (don't require existing auth)
        match &request {
            Request::Authenticate { username, password } => {
                return self
                    .handle_authenticate(username, password, client_ip, connection_auth)
                    .await;
            }
            Request::SaslAuthenticate {
                mechanism,
                auth_bytes,
            } => {
                return self
                    .handle_sasl_authenticate(mechanism, auth_bytes, client_ip, connection_auth)
                    .await;
            }
            Request::ScramClientFirst { message } => {
                return self
                    .handle_scram_client_first(message, client_ip, connection_auth)
                    .await;
            }
            Request::ScramClientFinal { message } => {
                return self
                    .handle_scram_client_final(message, client_ip, connection_auth)
                    .await;
            }
            Request::Ping => {
                // Ping is always allowed (for health checks)
                return Response::Pong;
            }
            _ => {}
        }

        // Check authentication requirement
        if self.require_auth {
            match connection_auth {
                ConnectionAuth::Unauthenticated => {
                    warn!("Unauthenticated request from {}", client_ip);
                    return Response::Error {
                        message: "AUTHENTICATION_REQUIRED: Please authenticate first".to_string(),
                    };
                }
                ConnectionAuth::ScramInProgress(_) => {
                    // SCRAM handshake incomplete - only ScramClientFinal allowed
                    warn!("Non-auth request during SCRAM handshake from {}", client_ip);
                    return Response::Error {
                        message: "AUTHENTICATION_REQUIRED: Complete SCRAM handshake first"
                            .to_string(),
                    };
                }
                ConnectionAuth::Authenticated(session) => {
                    // Check session expiration
                    if session.is_expired() {
                        warn!(
                            "Expired session for {} from {}",
                            session.principal_name, client_ip
                        );
                        *connection_auth = ConnectionAuth::Unauthenticated;
                        return Response::Error {
                            message: "SESSION_EXPIRED: Please re-authenticate".to_string(),
                        };
                    }

                    // Validate session still exists (principal not deleted)
                    if self.auth_manager.get_session(&session.id).is_none() {
                        warn!("Invalid session {} from {}", session.id, client_ip);
                        *connection_auth = ConnectionAuth::Unauthenticated;
                        return Response::Error {
                            message: "INVALID_SESSION: Please re-authenticate".to_string(),
                        };
                    }
                }
                ConnectionAuth::Anonymous => {
                    // This shouldn't happen if require_auth is true
                    warn!(
                        "Anonymous access attempted when auth required from {}",
                        client_ip
                    );
                    return Response::Error {
                        message: "AUTHENTICATION_REQUIRED: Anonymous access not allowed"
                            .to_string(),
                    };
                }
            }
        }

        // Get session for authorization (if authenticated)
        let session = match connection_auth {
            ConnectionAuth::Authenticated(s) => Some(s.clone()),
            _ => None,
        };

        // Authorize and handle the request
        self.authorize_and_handle(request, session.as_ref(), client_ip)
            .await
    }

    /// Handle username/password authentication
    async fn handle_authenticate(
        &self,
        username: &str,
        password: &str,
        client_ip: &str,
        connection_auth: &mut ConnectionAuth,
    ) -> Response {
        match self
            .auth_manager
            .authenticate(username, password, client_ip)
        {
            Ok(session) => {
                info!("User '{}' authenticated from {}", username, client_ip);
                let expires_in = session
                    .expires_at
                    .duration_since(session.created_at)
                    .as_secs();
                let session_id = session.id.clone();
                *connection_auth = ConnectionAuth::Authenticated(session);
                Response::Authenticated {
                    session_id,
                    expires_in,
                }
            }
            Err(e) => {
                warn!(
                    "Authentication failed for '{}' from {}: {}",
                    username, client_ip, e
                );
                Response::Error {
                    message: format!("AUTHENTICATION_FAILED: {}", e),
                }
            }
        }
    }

    /// Handle SASL/PLAIN authentication (Kafka client compatible)
    async fn handle_sasl_authenticate(
        &self,
        mechanism: &[u8],
        auth_bytes: &[u8],
        client_ip: &str,
        connection_auth: &mut ConnectionAuth,
    ) -> Response {
        // Check mechanism
        let mechanism_str = String::from_utf8_lossy(mechanism);
        if mechanism_str != "PLAIN" {
            return Response::Error {
                message: format!(
                    "UNSUPPORTED_MECHANISM: Only PLAIN is supported, got {}",
                    mechanism_str
                ),
            };
        }

        match self.sasl_plain.authenticate(auth_bytes, client_ip) {
            Ok(session) => {
                info!(
                    "SASL authentication successful for '{}' from {}",
                    session.principal_name, client_ip
                );
                let expires_in = session
                    .expires_at
                    .duration_since(session.created_at)
                    .as_secs();
                let session_id = session.id.clone();
                *connection_auth = ConnectionAuth::Authenticated(session);
                Response::Authenticated {
                    session_id,
                    expires_in,
                }
            }
            Err(e) => {
                warn!("SASL authentication failed from {}: {}", client_ip, e);
                Response::Error {
                    message: format!("AUTHENTICATION_FAILED: {}", e),
                }
            }
        }
    }

    /// Handle SCRAM-SHA-256 client-first message
    async fn handle_scram_client_first(
        &self,
        message: &Bytes,
        client_ip: &str,
        connection_auth: &mut ConnectionAuth,
    ) -> Response {
        match self.sasl_scram.process_client_first(message, client_ip) {
            Ok((state, server_first)) => {
                debug!("SCRAM client-first processed, sent server-first");
                *connection_auth = ConnectionAuth::ScramInProgress(state);
                Response::ScramServerFirst {
                    message: Bytes::from(server_first),
                }
            }
            Err(e) => {
                warn!("SCRAM client-first failed from {}: {}", client_ip, e);
                Response::Error {
                    message: format!("SCRAM_FAILED: {}", e),
                }
            }
        }
    }

    /// Handle SCRAM-SHA-256 client-final message
    async fn handle_scram_client_final(
        &self,
        message: &Bytes,
        client_ip: &str,
        connection_auth: &mut ConnectionAuth,
    ) -> Response {
        // Must be in SCRAM handshake state
        let state = match connection_auth {
            ConnectionAuth::ScramInProgress(state) => state.clone(),
            _ => {
                warn!("SCRAM client-final without client-first from {}", client_ip);
                return Response::Error {
                    message: "SCRAM_FAILED: Invalid state - send client-first first".to_string(),
                };
            }
        };

        match self
            .sasl_scram
            .process_client_final(&state, message, client_ip)
        {
            Ok((session, server_final)) => {
                info!(
                    "SCRAM authentication successful for '{}' from {}",
                    session.principal_name, client_ip
                );
                let expires_in = session
                    .expires_at
                    .duration_since(session.created_at)
                    .as_secs();
                let session_id = session.id.clone();
                *connection_auth = ConnectionAuth::Authenticated(session);
                Response::ScramServerFinal {
                    message: Bytes::from(server_final),
                    session_id: Some(session_id),
                    expires_in: Some(expires_in),
                }
            }
            Err(e) => {
                warn!("SCRAM authentication failed from {}: {}", client_ip, e);
                // Reset to unauthenticated on failure
                *connection_auth = ConnectionAuth::Unauthenticated;
                // Return server-final with error
                let error_msg = format!("e={}", e);
                Response::ScramServerFinal {
                    message: Bytes::from(error_msg),
                    session_id: None,
                    expires_in: None,
                }
            }
        }
    }

    /// Authorize and handle a request
    async fn authorize_and_handle(
        &self,
        request: Request,
        session: Option<&AuthSession>,
        client_ip: &str,
    ) -> Response {
        // Determine required permission for this request
        let (resource, permission) = match &request {
            Request::Publish { topic, .. } => {
                (ResourceType::Topic(topic.clone()), Permission::Write)
            }
            Request::Consume { topic, .. } => {
                (ResourceType::Topic(topic.clone()), Permission::Read)
            }
            Request::CreateTopic { name, .. } => {
                (ResourceType::Topic(name.clone()), Permission::Create)
            }
            Request::DeleteTopic { name } => {
                (ResourceType::Topic(name.clone()), Permission::Delete)
            }
            Request::ListTopics => (ResourceType::Cluster, Permission::Describe),
            Request::GetMetadata { topic } => {
                (ResourceType::Topic(topic.clone()), Permission::Describe)
            }
            Request::GetOffsetBounds { topic, .. } => {
                (ResourceType::Topic(topic.clone()), Permission::Describe)
            }
            Request::GetOffsetForTimestamp { topic, .. } => {
                (ResourceType::Topic(topic.clone()), Permission::Describe)
            }
            Request::GetClusterMetadata { .. } => (ResourceType::Cluster, Permission::Describe),
            Request::CommitOffset {
                consumer_group,
                topic,
                ..
            } => {
                // Commit offset requires both consumer group Write and topic Read permissions
                // We check consumer group here, topic is checked below for dual-resource ops
                if let Some(session) = session {
                    // First check consumer group permission
                    if let Err(e) = self.auth_manager.authorize(
                        session,
                        &ResourceType::ConsumerGroup(consumer_group.clone()),
                        Permission::Write,
                        client_ip,
                    ) {
                        warn!(
                            "Authorization denied for '{}' on consumer group '{}': {}",
                            session.principal_name, consumer_group, e
                        );
                        return Response::Error {
                            message: format!(
                                "AUTHORIZATION_FAILED: Consumer group access denied: {}",
                                e
                            ),
                        };
                    }
                }
                // Then check topic Read permission
                (ResourceType::Topic(topic.clone()), Permission::Read)
            }
            Request::GetOffset {
                consumer_group,
                topic,
                ..
            } => {
                // Get offset requires consumer group Read and topic Read
                if let Some(session) = session {
                    if let Err(e) = self.auth_manager.authorize(
                        session,
                        &ResourceType::ConsumerGroup(consumer_group.clone()),
                        Permission::Read,
                        client_ip,
                    ) {
                        warn!(
                            "Authorization denied for '{}' on consumer group '{}': {}",
                            session.principal_name, consumer_group, e
                        );
                        return Response::Error {
                            message: format!(
                                "AUTHORIZATION_FAILED: Consumer group access denied: {}",
                                e
                            ),
                        };
                    }
                }
                (ResourceType::Topic(topic.clone()), Permission::Read)
            }
            Request::ListGroups => (ResourceType::Cluster, Permission::Describe),
            Request::DescribeGroup { consumer_group, .. } => (
                ResourceType::ConsumerGroup(consumer_group.clone()),
                Permission::Describe,
            ),
            Request::DeleteGroup { consumer_group, .. } => (
                ResourceType::ConsumerGroup(consumer_group.clone()),
                Permission::Delete,
            ),
            // Idempotent Producer
            Request::InitProducerId { .. } => {
                // InitProducerId requires IdempotentWrite on Cluster
                (ResourceType::Cluster, Permission::IdempotentWrite)
            }
            Request::IdempotentPublish { topic, .. } => {
                // Idempotent publish requires both IdempotentWrite on Cluster and Write on Topic
                if let Some(session) = session {
                    // First check cluster-level IdempotentWrite permission
                    if let Err(e) = self.auth_manager.authorize(
                        session,
                        &ResourceType::Cluster,
                        Permission::IdempotentWrite,
                        client_ip,
                    ) {
                        warn!(
                            "IdempotentWrite denied for '{}' on cluster: {}",
                            session.principal_name, e
                        );
                        return Response::Error {
                            message: format!(
                                "AUTHORIZATION_FAILED: IdempotentWrite permission denied: {}",
                                e
                            ),
                        };
                    }
                }
                // Then check topic Write permission
                (ResourceType::Topic(topic.clone()), Permission::Write)
            }
            // Native Transactions
            Request::BeginTransaction { .. }
            | Request::AddPartitionsToTxn { .. }
            | Request::AddOffsetsToTxn { .. }
            | Request::CommitTransaction { .. }
            | Request::AbortTransaction { .. } => {
                // Transaction operations require IdempotentWrite on Cluster
                (ResourceType::Cluster, Permission::IdempotentWrite)
            }
            Request::TransactionalPublish { topic, .. } => {
                // Transactional publish requires IdempotentWrite on Cluster and Write on Topic
                if let Some(session) = session {
                    if let Err(e) = self.auth_manager.authorize(
                        session,
                        &ResourceType::Cluster,
                        Permission::IdempotentWrite,
                        client_ip,
                    ) {
                        warn!(
                            "IdempotentWrite denied for '{}' on cluster: {}",
                            session.principal_name, e
                        );
                        return Response::Error {
                            message: format!(
                                "AUTHORIZATION_FAILED: IdempotentWrite permission denied: {}",
                                e
                            ),
                        };
                    }
                }
                (ResourceType::Topic(topic.clone()), Permission::Write)
            }
            // Per-Principal Quotas (Kafka Parity)
            Request::DescribeQuotas { .. } => {
                // DescribeQuotas requires Describe on Cluster
                (ResourceType::Cluster, Permission::Describe)
            }
            Request::AlterQuotas { .. } => {
                // AlterQuotas requires Alter on Cluster (admin operation)
                (ResourceType::Cluster, Permission::Alter)
            }
            // Admin API (Kafka Parity)
            Request::AlterTopicConfig { topic, .. } => {
                // AlterTopicConfig requires Alter on Topic
                (ResourceType::Topic(topic.clone()), Permission::Alter)
            }
            Request::CreatePartitions { topic, .. } => {
                // CreatePartitions requires Alter on Topic
                (ResourceType::Topic(topic.clone()), Permission::Alter)
            }
            Request::DeleteRecords { topic, .. } => {
                // DeleteRecords requires Delete on Topic
                (ResourceType::Topic(topic.clone()), Permission::Delete)
            }
            Request::DescribeTopicConfigs { .. } => {
                // DescribeTopicConfigs requires Describe on Cluster
                (ResourceType::Cluster, Permission::Describe)
            }
            // Auth requests handled earlier
            Request::Authenticate { .. }
            | Request::SaslAuthenticate { .. }
            | Request::ScramClientFirst { .. }
            | Request::ScramClientFinal { .. }
            | Request::Ping => {
                // Already handled
                return Response::Error {
                    message: "INTERNAL_ERROR: Unexpected request type".to_string(),
                };
            }
        };

        // Authorize if we have a session
        if let Some(session) = session {
            if let Err(e) = self
                .auth_manager
                .authorize(session, &resource, permission, client_ip)
            {
                warn!(
                    "Authorization denied for '{}' on {:?}: {}",
                    session.principal_name, resource, e
                );
                return Response::Error {
                    message: format!("AUTHORIZATION_FAILED: {}", e),
                };
            }
            debug!(
                "Authorized '{}' for {:?} on {:?}",
                session.principal_name, permission, resource
            );
        } else if self.require_auth {
            // No session and auth required - shouldn't reach here but safety check
            return Response::Error {
                message: "AUTHENTICATION_REQUIRED".to_string(),
            };
        }

        // ======== Per-principal quota enforcement ========
        // Delegate to base handler with principal context.
        // handle_with_principal() checks request, produce, and consume quotas
        // with the real user identity — no double-counting.
        let user = session.map(|s| s.principal_name.as_str());
        self.handler
            .handle_with_principal(request, user, None)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivven_core::{
        AuthConfig, AuthManager, Config, OffsetManager, PrincipalType, TopicManager,
    };
    use std::collections::HashSet;
    use std::time::Duration;
    use tempfile::tempdir;

    async fn setup_test_handler(require_auth: bool) -> (AuthenticatedHandler, Arc<AuthManager>) {
        let temp_dir = tempdir().unwrap();
        let config = Config::new().with_data_dir(temp_dir.path().to_string_lossy().to_string());
        let topic_manager = TopicManager::new(config.clone());
        let offset_manager = OffsetManager::new();

        let base_handler = RequestHandler::new(topic_manager, offset_manager);

        let auth_config = AuthConfig {
            require_authentication: require_auth,
            enable_acls: require_auth,
            session_timeout: Duration::from_secs(3600),
            ..Default::default()
        };
        let auth_manager = Arc::new(AuthManager::new(auth_config));

        // Create test users
        let mut producer_roles = HashSet::new();
        producer_roles.insert("producer".to_string());
        auth_manager
            .create_principal(
                "producer",
                "Prod@Pass123",
                PrincipalType::User,
                producer_roles,
            )
            .unwrap();

        let mut admin_roles = HashSet::new();
        admin_roles.insert("admin".to_string());
        auth_manager
            .create_principal("admin", "Admin@Pass1", PrincipalType::User, admin_roles)
            .unwrap();

        let handler = AuthenticatedHandler::new(base_handler, auth_manager.clone(), require_auth);
        (handler, auth_manager)
    }

    #[tokio::test]
    async fn test_unauthenticated_request_denied() {
        let (handler, _) = setup_test_handler(true).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        let response = handler
            .handle(Request::ListTopics, &mut conn_auth, "127.0.0.1")
            .await;

        match response {
            Response::Error { message } => {
                assert!(message.contains("AUTHENTICATION_REQUIRED"));
            }
            _ => panic!("Expected error response"),
        }
    }

    #[tokio::test]
    async fn test_authentication_success() {
        let (handler, _) = setup_test_handler(true).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        let response = handler
            .handle(
                Request::Authenticate {
                    username: "producer".to_string(),
                    password: "Prod@Pass123".to_string(),
                },
                &mut conn_auth,
                "127.0.0.1",
            )
            .await;

        match response {
            Response::Authenticated { session_id, .. } => {
                assert!(!session_id.is_empty());
                assert!(matches!(conn_auth, ConnectionAuth::Authenticated(_)));
            }
            _ => panic!("Expected authenticated response"),
        }
    }

    #[tokio::test]
    async fn test_authentication_failure() {
        let (handler, _) = setup_test_handler(true).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        let response = handler
            .handle(
                Request::Authenticate {
                    username: "producer".to_string(),
                    password: "Wrong@Pass1".to_string(),
                },
                &mut conn_auth,
                "127.0.0.1",
            )
            .await;

        match response {
            Response::Error { message } => {
                assert!(message.contains("AUTHENTICATION_FAILED"));
            }
            _ => panic!("Expected error response"),
        }
    }

    #[tokio::test]
    async fn test_authorized_request() {
        let (handler, _) = setup_test_handler(true).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        // First authenticate
        handler
            .handle(
                Request::Authenticate {
                    username: "producer".to_string(),
                    password: "Prod@Pass123".to_string(),
                },
                &mut conn_auth,
                "127.0.0.1",
            )
            .await;

        // Now try to publish (producer role should allow this)
        let _response = handler
            .handle(
                Request::CreateTopic {
                    name: "test-topic".to_string(),
                    partitions: Some(1),
                },
                &mut conn_auth,
                "127.0.0.1",
            )
            .await;

        // Producer role has Write on topics, but not Create - should fail
        // Let's test with admin instead
    }

    #[tokio::test]
    async fn test_admin_can_create_topic() {
        let (handler, _) = setup_test_handler(true).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        // Authenticate as admin
        handler
            .handle(
                Request::Authenticate {
                    username: "admin".to_string(),
                    password: "Admin@Pass1".to_string(),
                },
                &mut conn_auth,
                "127.0.0.1",
            )
            .await;

        // Admin should be able to create topics
        let response = handler
            .handle(
                Request::CreateTopic {
                    name: "admin-topic".to_string(),
                    partitions: Some(1),
                },
                &mut conn_auth,
                "127.0.0.1",
            )
            .await;

        match response {
            Response::TopicCreated { name, .. } => {
                assert_eq!(name, "admin-topic");
            }
            Response::Error { message } => {
                panic!("Expected topic created, got error: {}", message);
            }
            _ => panic!("Expected topic created response"),
        }
    }

    #[tokio::test]
    async fn test_ping_without_auth() {
        let (handler, _) = setup_test_handler(true).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        // Ping should always work
        let response = handler
            .handle(Request::Ping, &mut conn_auth, "127.0.0.1")
            .await;

        assert!(matches!(response, Response::Pong));
    }

    #[tokio::test]
    async fn test_no_auth_required() {
        let (handler, _) = setup_test_handler(false).await;
        let mut conn_auth = ConnectionAuth::Unauthenticated;

        // Should be able to list topics without auth when not required
        let response = handler
            .handle(Request::ListTopics, &mut conn_auth, "127.0.0.1")
            .await;

        assert!(matches!(response, Response::Topics { .. }));
    }
}

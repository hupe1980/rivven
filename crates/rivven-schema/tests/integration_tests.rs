//! Integration tests for rivven-schema
//!
//! These tests verify the Schema Registry works correctly end-to-end.

use rivven_schema::{
    CompatibilityChecker, CompatibilityLevel, RegistryConfig, SchemaRegistry, SchemaType,
};

/// Test full schema registration workflow
#[tokio::test]
async fn test_full_registration_workflow() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Register first version
    let schema_v1 =
        r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
    let id1 = registry
        .register("user-value", SchemaType::Avro, schema_v1)
        .await
        .unwrap();
    assert!(id1.0 > 0);

    // Register compatible second version (add optional field)
    let schema_v2 = r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": ["null", "string"], "default": null}]}"#;
    let id2 = registry
        .register("user-value", SchemaType::Avro, schema_v2)
        .await
        .unwrap();
    assert!(id2.0 > 0);

    // Verify versions
    let versions = registry.list_versions("user-value").await.unwrap();
    assert_eq!(versions.len(), 2);
    assert!(versions.contains(&1));
    assert!(versions.contains(&2));

    // Get by version - schema may be normalized so just check it parses
    let sv1 = registry
        .get_by_version("user-value", rivven_schema::SchemaVersion::new(1))
        .await
        .unwrap();
    assert!(sv1.schema.contains("User"));
    assert!(sv1.schema.contains("id"));

    let sv2 = registry
        .get_by_version("user-value", rivven_schema::SchemaVersion::new(2))
        .await
        .unwrap();
    assert!(sv2.schema.contains("name"));

    // Get latest
    let latest = registry.get_latest("user-value").await.unwrap();
    assert_eq!(latest.version.0, 2);
}

/// Test schema deduplication
#[tokio::test]
async fn test_schema_deduplication() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    let schema =
        r#"{"type": "record", "name": "Event", "fields": [{"name": "id", "type": "string"}]}"#;

    // Register same schema to different subjects
    let id1 = registry
        .register("events-value", SchemaType::Avro, schema)
        .await
        .unwrap();
    let id2 = registry
        .register("notifications-value", SchemaType::Avro, schema)
        .await
        .unwrap();

    // Same schema content should get same ID (deduplication)
    assert_eq!(id1, id2);

    // But subjects should be independent
    let subjects = registry.list_subjects().await.unwrap();
    assert_eq!(subjects.len(), 2);
}

/// Test multi-subject management
#[tokio::test]
async fn test_multi_subject_management() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Register schemas for different subjects
    let user_schema =
        r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
    let order_schema =
        r#"{"type": "record", "name": "Order", "fields": [{"name": "orderId", "type": "string"}]}"#;
    let product_schema =
        r#"{"type": "record", "name": "Product", "fields": [{"name": "sku", "type": "string"}]}"#;

    registry
        .register("users-value", SchemaType::Avro, user_schema)
        .await
        .unwrap();
    registry
        .register("orders-value", SchemaType::Avro, order_schema)
        .await
        .unwrap();
    registry
        .register("products-value", SchemaType::Avro, product_schema)
        .await
        .unwrap();

    // List all subjects
    let subjects = registry.list_subjects().await.unwrap();
    assert_eq!(subjects.len(), 3);

    // Delete one subject
    registry
        .delete_subject("orders-value", false)
        .await
        .unwrap();

    let subjects = registry.list_subjects().await.unwrap();
    assert_eq!(subjects.len(), 2);
}

/// Test compatibility checker
#[tokio::test]
async fn test_compatibility_checker() {
    let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

    let old_schema =
        r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;

    // Adding optional field is backward compatible
    let new_schema_compatible = r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": ["null", "string"], "default": null}]}"#;

    let result = checker
        .check(SchemaType::Avro, new_schema_compatible, &[old_schema])
        .unwrap();
    assert!(result.is_compatible);
}

/// Test JSON schema support
#[tokio::test]
async fn test_json_schema_registration() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    let json_schema = r#"{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        },
        "required": ["id"]
    }"#;

    let id = registry
        .register("users-json-value", SchemaType::Json, json_schema)
        .await
        .unwrap();
    assert!(id.0 > 0);

    // Retrieve and verify
    let schema = registry.get_by_id(id).await.unwrap();
    assert_eq!(schema.schema_type, SchemaType::Json);
}

/// Test subject compatibility configuration
#[tokio::test]
async fn test_subject_compatibility_config() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Default compatibility
    let default = registry.get_default_compatibility();
    assert_eq!(default, CompatibilityLevel::Backward);

    // Set subject-specific compatibility
    registry.set_subject_compatibility("strict-subject", CompatibilityLevel::Full);

    let subject_level =
        registry.get_subject_compatibility(&rivven_schema::Subject::new("strict-subject"));
    assert_eq!(subject_level, CompatibilityLevel::Full);
}

/// Test caching behavior
#[tokio::test]
async fn test_caching_behavior() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    let schema =
        r#"{"type": "record", "name": "CacheTest", "fields": [{"name": "id", "type": "long"}]}"#;

    let id = registry
        .register("cache-test-value", SchemaType::Avro, schema)
        .await
        .unwrap();

    // First fetch should populate cache
    let fetched1 = registry.get_by_id(id).await.unwrap();
    assert!(fetched1.schema.contains("CacheTest"));
    assert!(fetched1.schema.contains("id"));

    // Second fetch should use cache
    let fetched2 = registry.get_by_id(id).await.unwrap();
    assert_eq!(fetched1.schema, fetched2.schema);
}

/// Test error handling for non-existent schemas
#[tokio::test]
async fn test_error_handling() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Get non-existent schema by ID
    let result = registry
        .get_by_id(rivven_schema::SchemaId::new(99999))
        .await;
    assert!(result.is_err());

    // Get non-existent subject version
    let result = registry
        .get_by_version("non-existent", rivven_schema::SchemaVersion::new(1))
        .await;
    assert!(result.is_err());

    // Get latest from non-existent subject
    let result = registry.get_latest("non-existent").await;
    assert!(result.is_err());
}

/// Test schema contexts (multi-tenancy)
#[tokio::test]
async fn test_schema_contexts() {
    use rivven_schema::SchemaContext;

    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Create a tenant context
    let tenant_ctx = SchemaContext::new("tenant-acme").with_description("ACME Corp schemas");
    registry.create_context(tenant_ctx.clone()).unwrap();

    // List contexts should include the new one
    let contexts = registry.list_contexts();
    assert!(contexts.iter().any(|c| c.name() == "tenant-acme"));

    // Register schema in context
    let schema =
        r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
    let id = registry
        .register(":.tenant-acme:users-value", SchemaType::Avro, schema)
        .await
        .unwrap();
    assert!(id.0 > 0);

    // List subjects in context
    let subjects = registry.list_subjects_in_context("tenant-acme");
    assert!(subjects.contains(&"users-value".to_string()));

    // Get context
    let ctx = registry.get_context("tenant-acme").unwrap();
    assert_eq!(ctx.description(), Some("ACME Corp schemas"));
}

/// Test version states (lifecycle management)
#[tokio::test]
async fn test_version_states() {
    use rivven_schema::{SchemaVersion, VersionState};

    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Register a schema
    let schema =
        r#"{"type": "record", "name": "Event", "fields": [{"name": "id", "type": "string"}]}"#;
    registry
        .register("events-value", SchemaType::Avro, schema)
        .await
        .unwrap();

    // Default state should be Enabled
    let state = registry
        .get_version_state("events-value", SchemaVersion::new(1))
        .await
        .unwrap();
    assert_eq!(state, VersionState::Enabled);

    // Deprecate the version
    registry
        .deprecate_version("events-value", SchemaVersion::new(1))
        .await
        .unwrap();
    let state = registry
        .get_version_state("events-value", SchemaVersion::new(1))
        .await
        .unwrap();
    assert_eq!(state, VersionState::Deprecated);

    // Re-enable the version
    registry
        .enable_version("events-value", SchemaVersion::new(1))
        .await
        .unwrap();
    let state = registry
        .get_version_state("events-value", SchemaVersion::new(1))
        .await
        .unwrap();
    assert_eq!(state, VersionState::Enabled);
}

/// Test content validation rules
#[tokio::test]
async fn test_validation_rules() {
    use rivven_schema::{ValidationLevel, ValidationRule, ValidationRuleType};

    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Add a max size validation rule
    let rule = ValidationRule::new(
        "max-size",
        ValidationRuleType::MaxSize,
        r#"{"max_bytes": 1000}"#,
    )
    .with_level(ValidationLevel::Error)
    .with_description("Schemas must be under 1KB");

    registry.add_validation_rule(rule);

    // Small schema should pass
    let small_schema =
        r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
    let report = registry
        .validate_schema(SchemaType::Avro, "users-value", small_schema)
        .unwrap();
    assert!(report.is_valid());

    // Large schema should fail
    let large_schema = format!(
        r#"{{"type": "record", "name": "BigRecord", "fields": [{}]}}"#,
        (0..100)
            .map(|i| format!(r#"{{"name": "field{}", "type": "string"}}"#, i))
            .collect::<Vec<_>>()
            .join(",")
    );
    let report = registry
        .validate_schema(SchemaType::Avro, "big-value", &large_schema)
        .unwrap();
    assert!(!report.is_valid());
    assert!(report.error_messages().iter().any(|m| m.contains("size")));
}

/// Test registry statistics
#[tokio::test]
async fn test_registry_stats() {
    let config = RegistryConfig::memory();
    let registry = SchemaRegistry::new(config).await.unwrap();

    // Register some schemas
    let user_schema =
        r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
    let order_schema =
        r#"{"type": "record", "name": "Order", "fields": [{"name": "orderId", "type": "string"}]}"#;

    registry
        .register("users-value", SchemaType::Avro, user_schema)
        .await
        .unwrap();
    registry
        .register("orders-value", SchemaType::Avro, order_schema)
        .await
        .unwrap();

    // Check stats
    let stats = registry.stats().await;
    assert!(stats.schema_count >= 2);
    assert_eq!(stats.subject_count, 2);
}

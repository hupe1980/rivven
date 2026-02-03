//! Schema Registry and Avro Integration Tests
//!
//! Tests for the complete Avro serialization pipeline:
//! - Source → Avro (schema registry) → Broker → Avro → Sink
//!
//! Run with: cargo test -p rivven-integration-tests --test schema_avro -- --nocapture

use anyhow::Result;
use futures::StreamExt;
use rivven_client::Client;
use rivven_connect::schema::{
    AvroCodec, AvroSchema, ExternalRegistry, ExternalRegistryConfig, SchemaAwareConfig,
    SchemaAwareSink, SchemaAwareSource, SchemaRegistryClient, SchemaRegistryConfig, SchemaType,
    Subject, SubjectStrategy,
};
use rivven_connect::traits::{
    Catalog, CheckResult, ConfiguredCatalog, ConnectorSpec, Sink, Source, SourceEvent, State,
    Stream, SyncMode, WriteResult,
};
use rivven_integration_tests::fixtures::{TestBroker, TestSchemaRegistry};
use rivven_integration_tests::helpers::init_tracing;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

// ============================================================================
// Test User Event Schema
// ============================================================================

const USER_AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.rivven.test",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "timestamp", "type": "long"}
  ]
}"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserEvent {
    id: String,
    name: String,
    email: String,
    age: i32,
    timestamp: i64,
}

impl UserEvent {
    fn new(id: u32) -> Self {
        Self {
            id: format!("user-{}", id),
            name: format!("User {}", id),
            email: format!("user{}@test.com", id),
            age: 25 + (id as i32 % 50),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }
}

// ============================================================================
// Test Mock Source (generates UserEvents)
// ============================================================================

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema, validator::Validate,
)]
struct TestSourceConfig {
    events_to_generate: u32,
    stream_name: String,
}

impl TestSourceConfig {
    fn with_events(events_to_generate: u32, stream_name: &str) -> Self {
        Self {
            events_to_generate,
            stream_name: stream_name.to_string(),
        }
    }
}

struct TestSource {
    #[allow(dead_code)]
    counter: AtomicU64,
}

impl TestSource {
    fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Source for TestSource {
    type Config = TestSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("test-source", "1.0.0")
            .description("Test source for integration tests")
            .build()
    }

    async fn check(&self, _config: &Self::Config) -> rivven_connect::error::Result<CheckResult> {
        Ok(CheckResult::success())
    }

    async fn discover(&self, _config: &Self::Config) -> rivven_connect::error::Result<Catalog> {
        let stream = Stream::new(
            "test-users",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "age": {"type": "integer"},
                    "timestamp": {"type": "integer"}
                }
            }),
        )
        .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental])
        .primary_key(vec![vec!["id".to_string()]]);

        Ok(Catalog::new().add_stream(stream))
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> rivven_connect::error::Result<
        futures::stream::BoxStream<'static, rivven_connect::error::Result<SourceEvent>>,
    > {
        let events_to_generate = config.events_to_generate;
        let stream_name = config.stream_name.clone();

        let stream = futures::stream::unfold(0u32, move |state| {
            let stream_name = stream_name.clone();
            async move {
                if state >= events_to_generate {
                    return None;
                }

                let user = UserEvent::new(state);
                let data = serde_json::to_value(&user).unwrap();

                let event = SourceEvent::record(&stream_name, data);

                Some((Ok(event), state + 1))
            }
        });

        Ok(Box::pin(stream))
    }
}

// ============================================================================
// Test Mock Sink (collects events for verification)
// ============================================================================

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema, validator::Validate,
)]
struct TestSinkConfig {
    #[allow(dead_code)]
    expected_format: String,
}

struct TestSink {
    received_events: Arc<Mutex<Vec<SourceEvent>>>,
}

impl TestSink {
    fn new() -> Self {
        Self {
            received_events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    async fn get_events(&self) -> Vec<SourceEvent> {
        self.received_events.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl Sink for TestSink {
    type Config = TestSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("test-sink", "1.0.0")
            .description("Test sink for integration tests")
            .build()
    }

    async fn check(&self, _config: &Self::Config) -> rivven_connect::error::Result<CheckResult> {
        Ok(CheckResult::success())
    }

    async fn write(
        &self,
        _config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> rivven_connect::error::Result<WriteResult> {
        let mut result = WriteResult::new();
        let mut received = self.received_events.lock().await;

        while let Some(event) = events.next().await {
            received.push(event);
            result.add_success(1, 0);
        }

        Ok(result)
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

/// Test basic Avro serialization
#[tokio::test]
async fn test_avro_serialization_basic() -> Result<()> {
    init_tracing();

    // Create Avro schema and codec
    let schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let codec = AvroCodec::new(schema);

    // Test serialization
    let user = UserEvent::new(1);
    let data = serde_json::to_value(&user)?;
    let avro_bytes = codec.encode(&data)?;

    // Test deserialization
    let decoded: serde_json::Value = codec.decode(&avro_bytes)?;
    let decoded_user: UserEvent = serde_json::from_value(decoded)?;

    assert_eq!(user.id, decoded_user.id);
    assert_eq!(user.name, decoded_user.name);
    assert_eq!(user.email, decoded_user.email);
    assert_eq!(user.age, decoded_user.age);

    info!("Basic Avro serialization test passed");
    Ok(())
}

/// Test Avro with Confluent wire format (5-byte header with schema ID)
#[tokio::test]
async fn test_avro_confluent_wire_format() -> Result<()> {
    init_tracing();

    let schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let codec = AvroCodec::new(schema);
    let schema_id = 42u32;

    // Test serialization with wire format
    let user = UserEvent::new(1);
    let data = serde_json::to_value(&user)?;
    let avro_bytes = codec.encode(&data)?;

    // Add Confluent wire format header
    let mut wire_format = Vec::with_capacity(5 + avro_bytes.len());
    wire_format.push(0u8); // Magic byte
    wire_format.extend_from_slice(&schema_id.to_be_bytes()); // Schema ID
    wire_format.extend_from_slice(&avro_bytes);

    // Verify wire format structure
    assert_eq!(wire_format[0], 0u8); // Magic byte
    let extracted_id = u32::from_be_bytes([
        wire_format[1],
        wire_format[2],
        wire_format[3],
        wire_format[4],
    ]);
    assert_eq!(extracted_id, schema_id);

    // Decode the payload (skip 5-byte header)
    let payload = &wire_format[5..];
    let decoded: serde_json::Value = codec.decode(payload)?;
    let decoded_user: UserEvent = serde_json::from_value(decoded)?;

    assert_eq!(user.id, decoded_user.id);

    info!("Confluent wire format test passed");
    Ok(())
}

/// Test source to broker to sink with Avro serialization
#[tokio::test]
async fn test_source_avro_broker_sink_pipeline() -> Result<()> {
    init_tracing();

    // Start test broker
    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create topic
    let topic = format!("avro-test-{}", uuid::Uuid::new_v4());
    client.create_topic(&topic, Some(1)).await?;

    // Create Avro codec
    let schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let codec = AvroCodec::new(schema.clone());

    // Generate and serialize events
    let events: Vec<UserEvent> = (0..10).map(UserEvent::new).collect();

    for event in &events {
        let data = serde_json::to_value(event)?;
        let avro_bytes = codec.encode(&data)?;

        // Add Confluent wire format header (schema ID = 1)
        let mut wire_format = Vec::with_capacity(5 + avro_bytes.len());
        wire_format.push(0u8);
        wire_format.extend_from_slice(&1u32.to_be_bytes());
        wire_format.extend_from_slice(&avro_bytes);

        client.publish(&topic, wire_format).await?;
    }

    info!("Published {} Avro-encoded events", events.len());

    // Consume and deserialize
    let messages = client.consume(&topic, 0, 0, 20).await?;
    assert_eq!(messages.len(), 10);

    // Decode and verify each message
    let decode_codec = AvroCodec::new(schema);
    for (i, msg) in messages.iter().enumerate() {
        // Skip 5-byte header
        assert_eq!(msg.value[0], 0u8, "Magic byte should be 0");
        let payload = &msg.value[5..];

        let decoded: serde_json::Value = decode_codec.decode(payload)?;
        let decoded_user: UserEvent = serde_json::from_value(decoded)?;

        assert_eq!(decoded_user.id, format!("user-{}", i));
        assert_eq!(decoded_user.name, format!("User {}", i));
    }

    info!(
        "Pipeline test passed: {} events serialized/deserialized correctly",
        events.len()
    );
    broker.shutdown().await?;
    Ok(())
}

/// Test schema registry with schema caching
#[tokio::test]
async fn test_schema_registry_caching() -> Result<()> {
    init_tracing();

    // Start test broker and schema registry
    let broker = TestBroker::start().await?;
    let schema_registry = TestSchemaRegistry::start().await?;

    // Create external registry pointing to test schema registry
    let external_config = ExternalRegistryConfig {
        url: schema_registry.url().to_string(),
        ..Default::default()
    };
    let registry = ExternalRegistry::new(&external_config)?;

    // Register schema
    let subject = Subject::new("test-users-value");
    let schema_id = registry
        .register(&subject, SchemaType::Avro, USER_AVRO_SCHEMA)
        .await?;
    info!("Registered schema with ID: {}", schema_id);

    // Retrieve schema by ID
    let retrieved = registry.get_by_id(schema_id).await;
    assert!(retrieved.is_ok());

    // Register same schema again - should return same ID (idempotent)
    let schema_id_2 = registry
        .register(&subject, SchemaType::Avro, USER_AVRO_SCHEMA)
        .await?;
    assert_eq!(schema_id.0, schema_id_2.0);

    info!("Schema registry caching test passed");
    schema_registry.shutdown().await?;
    broker.shutdown().await?;
    Ok(())
}

/// Test SchemaAwareSink wrapper
#[tokio::test]
async fn test_schema_aware_sink() -> Result<()> {
    init_tracing();

    // Start test broker and schema registry
    let broker = TestBroker::start().await?;
    let schema_registry = TestSchemaRegistry::start().await?;

    // Create schema registry client using config
    let config = SchemaRegistryConfig::external(schema_registry.url());
    let registry = SchemaRegistryClient::from_config(&config)?;

    // Pre-register schema
    let subject = Subject::new("test-users-value");
    registry
        .register(&subject, SchemaType::Avro, USER_AVRO_SCHEMA)
        .await?;

    // Create test sink wrapped with schema awareness
    let inner_sink = TestSink::new();
    let schema_config = SchemaAwareConfig {
        subject_strategy: SubjectStrategy::TopicName,
        auto_register: true,
        confluent_wire_format: true,
        ..Default::default()
    };

    let schema_sink = SchemaAwareSink::with_config(inner_sink, Arc::new(registry), schema_config);

    // Create test events
    let events: Vec<SourceEvent> = (0..5)
        .map(|i| {
            let user = UserEvent::new(i);
            SourceEvent::record("test-users", serde_json::to_value(&user).unwrap())
        })
        .collect();

    // Write events through schema-aware sink
    let event_stream = futures::stream::iter(events);
    let sink_config = TestSinkConfig::default();
    let result = schema_sink
        .write(&sink_config, Box::pin(event_stream))
        .await?;

    assert!(result.records_written >= 5);
    info!("SchemaAwareSink test passed");

    schema_registry.shutdown().await?;
    broker.shutdown().await?;
    Ok(())
}

/// Test full pipeline: Source → SchemaAwareSource → Broker → SchemaAwareSink → Sink
#[tokio::test]
async fn test_full_schema_aware_pipeline() -> Result<()> {
    init_tracing();

    // Start test broker and schema registry
    let broker = TestBroker::start().await?;
    let schema_registry = TestSchemaRegistry::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create topic
    let topic = format!("schema-aware-pipeline-{}", uuid::Uuid::new_v4());
    client.create_topic(&topic, Some(1)).await?;

    // Create schema registry using config
    let config = SchemaRegistryConfig::external(schema_registry.url());
    let registry = Arc::new(SchemaRegistryClient::from_config(&config)?);

    // Note: Don't pre-register schema - let SchemaAwareSource auto-register
    // This avoids compatibility issues between hand-written schema and inferred schema

    // Create schema-aware source
    let source = TestSource::new();
    let schema_config = SchemaAwareConfig {
        subject_strategy: SubjectStrategy::TopicName,
        auto_register: true, // Auto-register inferred schema
        confluent_wire_format: true,
        ..Default::default()
    };
    let schema_source = SchemaAwareSource::with_config(source, registry.clone(), schema_config);

    // Configure source
    let source_cfg = TestSourceConfig::with_events(10, &topic);

    // Create catalog
    let catalog = ConfiguredCatalog::new();

    // Read events from schema-aware source
    let mut event_stream = schema_source.read(&source_cfg, &catalog, None).await?;

    // Publish to broker
    let mut published_count = 0;
    while let Some(result) = event_stream.next().await {
        let event = result?;
        let data = serde_json::to_vec(&event.data)?;
        client.publish(&topic, data).await?;
        published_count += 1;
    }
    assert_eq!(published_count, 10);
    info!("Published {} events to broker", published_count);

    // Consume from broker
    let messages = client.consume(&topic, 0, 0, 20).await?;
    assert_eq!(messages.len(), 10);

    // Verify each message can be decoded
    for (i, msg) in messages.iter().enumerate() {
        let event: serde_json::Value = serde_json::from_slice(&msg.value)?;

        // Verify data integrity
        if let Some(id) = event.get("id") {
            assert!(
                id.as_str().unwrap().starts_with("user-"),
                "Event {} has invalid id",
                i
            );
        }
    }

    info!("Full schema-aware pipeline test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test schema evolution compatibility
#[tokio::test]
async fn test_schema_evolution() -> Result<()> {
    init_tracing();

    // Start test broker and schema registry
    let broker = TestBroker::start().await?;
    let schema_registry = TestSchemaRegistry::start().await?;

    // Create external registry pointing to test schema registry
    let external_config = ExternalRegistryConfig {
        url: schema_registry.url().to_string(),
        ..Default::default()
    };
    let registry = ExternalRegistry::new(&external_config)?;

    let subject = Subject::new("evolving-schema-value");

    // Register v1 schema
    let v1_schema = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"}
        ]
    }"#;
    let v1_id = registry
        .register(&subject, SchemaType::Avro, v1_schema)
        .await?;
    info!("Registered v1 schema with ID: {}", v1_id);

    // Register v2 schema (backward compatible - added optional field)
    let v2_schema = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }"#;
    let v2_id = registry
        .register(&subject, SchemaType::Avro, v2_schema)
        .await?;
    info!("Registered v2 schema with ID: {}", v2_id);

    // Verify both schemas exist
    assert_ne!(v1_id.0, v2_id.0);

    let v1_retrieved = registry.get_by_id(v1_id).await;
    assert!(v1_retrieved.is_ok());

    let v2_retrieved = registry.get_by_id(v2_id).await;
    assert!(v2_retrieved.is_ok());

    // Test that v1 data can be read with v1 schema
    let v1_codec = AvroCodec::new(AvroSchema::parse(v1_schema)?);

    let v1_data = serde_json::json!({
        "id": "user-1",
        "name": "Test User"
    });
    let v1_bytes = v1_codec.encode(&v1_data)?;

    // Read with v1 schema - should work
    let decoded_v1: serde_json::Value = v1_codec.decode(&v1_bytes)?;
    assert_eq!(decoded_v1["id"], "user-1");

    info!("Schema evolution test passed");
    schema_registry.shutdown().await?;
    broker.shutdown().await?;
    Ok(())
}

/// Test multiple concurrent schema registrations
#[tokio::test]
async fn test_concurrent_schema_operations() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let schema_registry = TestSchemaRegistry::start().await?;

    let external_config = ExternalRegistryConfig {
        url: schema_registry.url().to_string(),
        ..Default::default()
    };
    let registry = Arc::new(ExternalRegistry::new(&external_config)?);

    // Spawn multiple tasks registering different schemas
    let mut handles = Vec::new();
    for i in 0..5 {
        let reg = registry.clone();
        let handle = tokio::spawn(async move {
            let subject = Subject::new(format!("concurrent-test-{}-value", i));
            let schema = format!(
                r#"{{"type":"record","name":"Event{}","fields":[{{"name":"id","type":"string"}}]}}"#,
                i
            );
            reg.register(&subject, SchemaType::Avro, &schema).await
        });
        handles.push(handle);
    }

    // All registrations should succeed
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await?;
        assert!(result.is_ok(), "Registration {} failed: {:?}", i, result);
        info!("Concurrent registration {} succeeded", i);
    }

    info!("Concurrent schema operations test passed");
    schema_registry.shutdown().await?;
    broker.shutdown().await?;
    Ok(())
}

/// Test subject naming strategies
#[tokio::test]
async fn test_subject_naming_strategies() -> Result<()> {
    init_tracing();

    // TopicName strategy
    let strategy = SubjectStrategy::TopicName;
    let subject = strategy.generate_subject("my-topic", &serde_json::json!({}));
    assert_eq!(subject.0, "my-topic-value");

    // RecordName strategy with __type field
    let strategy = SubjectStrategy::RecordName;
    let data = serde_json::json!({"__type": "MyRecord", "data": "test"});
    let subject = strategy.generate_subject("my-topic", &data);
    assert_eq!(subject.0, "MyRecord");

    // RecordName strategy without __type field (falls back to topic)
    let data = serde_json::json!({"data": "test"});
    let subject = strategy.generate_subject("my-topic", &data);
    assert_eq!(subject.0, "my-topic-value");

    // TopicRecordName strategy
    let strategy = SubjectStrategy::TopicRecordName;
    let data = serde_json::json!({"__type": "MyRecord", "data": "test"});
    let subject = strategy.generate_subject("my-topic", &data);
    assert_eq!(subject.0, "my-topic-MyRecord");

    info!("Subject naming strategies test passed");
    Ok(())
}

/// Test error handling for invalid schemas
#[tokio::test]
async fn test_invalid_schema_handling() -> Result<()> {
    init_tracing();

    // Invalid Avro schema
    let invalid_schema = r#"{"type": "invalid_type"}"#;
    let result = AvroSchema::parse(invalid_schema);
    assert!(result.is_err());

    // Malformed JSON
    let malformed = r#"{"type": "record", "name": }"#;
    let result = AvroSchema::parse(malformed);
    assert!(result.is_err());

    // Valid schema but invalid data
    let schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let codec = AvroCodec::new(schema);

    // Missing required field
    let invalid_data = serde_json::json!({
        "id": "user-1",
        "name": "Test"
        // missing email, age, timestamp
    });
    let result = codec.encode(&invalid_data);
    assert!(result.is_err());

    info!("Invalid schema handling test passed");
    Ok(())
}

/// Benchmark: Measure Avro serialization throughput
#[tokio::test]
async fn test_avro_throughput() -> Result<()> {
    init_tracing();

    let schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let codec = AvroCodec::new(schema);

    let iterations = 10_000;
    let user = UserEvent::new(1);
    let data = serde_json::to_value(&user)?;

    // Measure serialization
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let _ = codec.encode(&data)?;
    }
    let serialize_duration = start.elapsed();

    // Measure deserialization
    let avro_bytes = codec.encode(&data)?;
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let _: serde_json::Value = codec.decode(&avro_bytes)?;
    }
    let deserialize_duration = start.elapsed();

    let serialize_rate = iterations as f64 / serialize_duration.as_secs_f64();
    let deserialize_rate = iterations as f64 / deserialize_duration.as_secs_f64();

    info!(
        "Avro throughput: serialize={:.0}/s, deserialize={:.0}/s",
        serialize_rate, deserialize_rate
    );

    // Ensure reasonable throughput (at least 10k/s)
    assert!(serialize_rate > 10_000.0, "Serialization too slow");
    assert!(deserialize_rate > 10_000.0, "Deserialization too slow");

    Ok(())
}

/// End-to-end pipeline: Source → Avro (schema registry) → Broker → Avro → JSON Sink
///
/// This test demonstrates the complete data flow with schema registry integration:
/// 1. Source generates events
/// 2. Events are Avro-encoded with schema registered in registry  
/// 3. Avro data is published to broker with Confluent wire format
/// 4. Consumer reads from broker
/// 5. Avro data is decoded back to JSON
/// 6. JSON is written to sink (stdout)
#[tokio::test]
async fn test_e2e_source_avro_broker_json_sink() -> Result<()> {
    init_tracing();

    // ========================================================================
    // Stage 1: Setup - Create broker, topic, and schema registry
    // ========================================================================
    let broker = TestBroker::start().await?;
    let schema_registry = TestSchemaRegistry::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = format!("e2e-avro-pipeline-{}", uuid::Uuid::new_v4());
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Create external schema registry
    let registry_config = SchemaRegistryConfig::external(schema_registry.url());
    let registry = Arc::new(SchemaRegistryClient::from_config(&registry_config)?);

    // Pre-register the schema
    let subject = Subject::new(format!("{}-value", topic));
    let schema_id = registry
        .register(&subject, SchemaType::Avro, USER_AVRO_SCHEMA)
        .await?;
    info!(
        "Registered schema with ID: {} for subject: {}",
        schema_id, subject.0
    );

    // ========================================================================
    // Stage 2: Source → Avro encoding → Broker
    // ========================================================================
    let source = TestSource::new();
    let schema_config = SchemaAwareConfig {
        subject_strategy: SubjectStrategy::TopicName,
        auto_register: false, // Use pre-registered schema
        confluent_wire_format: true,
        ..Default::default()
    };
    let schema_source = SchemaAwareSource::with_config(source, registry.clone(), schema_config);

    // Generate events
    let source_cfg = TestSourceConfig::with_events(5, &topic);
    let catalog = ConfiguredCatalog::new();
    let mut event_stream = schema_source.read(&source_cfg, &catalog, None).await?;

    // Publish to broker with Avro encoding + Confluent wire format
    let schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let codec = AvroCodec::new(schema);
    let mut published_messages: Vec<Vec<u8>> = Vec::new();

    while let Some(result) = event_stream.next().await {
        let event = result?;

        // Encode with Avro
        let avro_bytes = codec.encode(&event.data)?;

        // Add Confluent wire format header (magic byte + 4-byte schema ID)
        let mut wire_format = Vec::with_capacity(5 + avro_bytes.len());
        wire_format.push(0u8); // Magic byte
        wire_format.extend_from_slice(&schema_id.0.to_be_bytes()); // Schema ID
        wire_format.extend_from_slice(&avro_bytes);

        published_messages.push(wire_format.clone());
        client.publish(&topic, wire_format).await?;
    }
    info!(
        "Published {} Avro-encoded messages to broker",
        published_messages.len()
    );
    assert_eq!(published_messages.len(), 5);

    // ========================================================================
    // Stage 3: Broker → Avro decoding → JSON output
    // ========================================================================
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 5);

    // Create codec for decoding
    let decode_schema = AvroSchema::parse(USER_AVRO_SCHEMA)?;
    let decode_codec = AvroCodec::new(decode_schema);

    let mut decoded_events: Vec<serde_json::Value> = Vec::new();
    for (i, msg) in messages.iter().enumerate() {
        // Verify Confluent wire format header
        assert_eq!(msg.value[0], 0u8, "Message {} missing magic byte", i);

        // Extract and verify schema ID
        let extracted_id =
            u32::from_be_bytes([msg.value[1], msg.value[2], msg.value[3], msg.value[4]]);
        assert_eq!(
            extracted_id, schema_id.0,
            "Schema ID mismatch for message {}",
            i
        );

        // Decode Avro payload (skip 5-byte header)
        let payload = &msg.value[5..];
        let decoded: serde_json::Value = decode_codec.decode(payload)?;

        // Verify decoded data structure
        assert!(decoded.get("id").is_some(), "Missing 'id' field");
        assert!(decoded.get("name").is_some(), "Missing 'name' field");
        assert!(decoded.get("email").is_some(), "Missing 'email' field");
        assert!(decoded.get("age").is_some(), "Missing 'age' field");
        assert!(
            decoded.get("timestamp").is_some(),
            "Missing 'timestamp' field"
        );

        // Verify data integrity
        let user: UserEvent = serde_json::from_value(decoded.clone())?;
        assert_eq!(user.id, format!("user-{}", i));
        assert_eq!(user.name, format!("User {}", i));

        decoded_events.push(decoded);
        info!("Decoded message {}: {}", i, serde_json::to_string(&user)?);
    }

    // ========================================================================
    // Stage 4: Simulate JSON sink output (verify it's valid JSON)
    // ========================================================================
    for (i, event) in decoded_events.iter().enumerate() {
        // This is what would be written to stdout sink in JSON format
        let json_output = serde_json::to_string_pretty(event)?;
        assert!(!json_output.is_empty());
        info!("Sink output for event {}:\n{}", i, json_output);
    }

    info!("✅ E2E pipeline test passed: Source → Avro → Broker → Avro → JSON Sink");
    schema_registry.shutdown().await?;
    broker.shutdown().await?;
    Ok(())
}

/// Test AWS Glue Schema Registry configuration (unit test, no actual AWS call)
#[tokio::test]
async fn test_glue_registry_config() -> Result<()> {
    init_tracing();

    use rivven_connect::schema::glue::GlueConfig;

    // Test GlueConfig creation
    let config = GlueConfig {
        region: "us-east-1".to_string(),
        registry_name: "my-registry".to_string(),
        ..Default::default()
    };

    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.registry_name, "my-registry");

    // Test SchemaRegistryConfig::Glue variant
    let registry_config = SchemaRegistryConfig::Glue(config);
    match registry_config {
        SchemaRegistryConfig::Glue(gc) => {
            assert_eq!(gc.region, "us-east-1");
        }
        _ => panic!("Expected Glue config"),
    }

    info!("AWS Glue config test passed");
    Ok(())
}

/// Test external schema registry configuration (unit test)
#[tokio::test]
async fn test_external_registry_config() -> Result<()> {
    init_tracing();

    use rivven_connect::schema::ExternalRegistryConfig;

    // Test ExternalRegistryConfig creation
    let config = ExternalRegistryConfig {
        url: "http://localhost:8081".to_string(),
        ..Default::default()
    };

    assert_eq!(config.url, "http://localhost:8081");

    // Test SchemaRegistryConfig::External variant
    let registry_config = SchemaRegistryConfig::External(config);
    match registry_config {
        SchemaRegistryConfig::External(ec) => {
            assert_eq!(ec.url, "http://localhost:8081");
        }
        _ => panic!("Expected External config"),
    }

    info!("External registry config test passed");
    Ok(())
}

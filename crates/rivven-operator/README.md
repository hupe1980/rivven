# rivven-operator

> Kubernetes operator for deploying and managing Rivven clusters and connectors.

## Overview

The Rivven Operator provides Custom Resource Definitions (CRDs) for declarative, GitOps-friendly cluster management.

## Features

| Resource | Description |
|:---------|:------------|
| **RivvenCluster** | Declarative cluster management with StatefulSets |
| **RivvenConnect** | Declarative connector pipeline management |
| **RivvenTopic** | GitOps-friendly topic management |
| **RivvenSchemaRegistry** | Declarative Schema Registry deployment |

Additional capabilities:
- Automated reconciliation with eventual consistency
- Ordered deployment, scaling, and rolling updates
- Secure credential handling for sources and sinks
- Prometheus-compatible operator metrics

## Quick Start

### Prerequisites

- Kubernetes 1.28+
- kubectl configured for your cluster
- Helm 3.x (optional, for Helm-based deployment)

### Install CRDs

```bash
kubectl apply -f deploy/crds/
```

### Deploy Operator

```bash
# Using kubectl
kubectl apply -f deploy/operator/

# Or using Helm
helm install rivven-operator charts/rivven-operator/
```

### Create a Rivven Cluster

```yaml
apiVersion: rivven.hupe1980.github.io/v1alpha1
kind: RivvenCluster
metadata:
  name: production
  namespace: default
spec:
  replicas: 3
  version: "0.0.18"
  
  storage:
    size: 100Gi
    storageClassName: fast-ssd
  
  resources:
    requests:
      cpu: "1"
      memory: 2Gi
    limits:
      cpu: "4"
      memory: 8Gi
  
  config:
    defaultPartitions: 3
    defaultReplicationFactor: 2
    logRetentionHours: 168
    
  tls:
    enabled: true
    certSecretName: rivven-tls
    
  metrics:
    enabled: true
    port: 9090
```

```bash
kubectl apply -f my-cluster.yaml
```

### Create a RivvenConnect Instance

RivvenConnect uses a Kafka Connect-style generic configuration approach, allowing any connector to be configured without CRD schema changes. All connector-specific parameters go in the `config` field and are validated at runtime by the controller.

```yaml
apiVersion: rivven.hupe1980.github.io/v1alpha1
kind: RivvenConnect
metadata:
  name: cdc-pipeline
  namespace: default
spec:
  clusterRef:
    name: production
  
  replicas: 2
  version: "0.0.18"
  
  # PostgreSQL CDC source with generic config
  sources:
    - name: postgres-cdc
      connector: postgres-cdc
      topic: cdc.events
      enabled: true
      configSecretRef: postgres-credentials  # Secret with host/port/user/pass
      # All connector-specific config goes here (Kafka Connect style)
      config:
        slotName: rivven_cdc_slot
        publication: rivven_pub
        snapshotMode: initial
        decodingPlugin: pgoutput
        heartbeatIntervalMs: 10000
        # Table selection with column filtering
        tables:
          - schema: public
            table: orders
            columns: [id, customer_id, total, status, created_at]
            excludeColumns: [internal_notes]
          - schema: public
            table: customers
            columnMasks:
              email: "***@***.***"
              phone: "***-***-****"
        # Advanced CDC features
        snapshot:
          batchSize: 20000
          parallelTables: 8
          queryTimeoutSecs: 600
        incrementalSnapshot:
          enabled: true
          chunkSize: 2048
          watermarkStrategy: update_and_insert
        heartbeat:
          enabled: true
          intervalSecs: 5
          maxLagSecs: 60
          emitEvents: true
        deduplication:
          enabled: true
          bloomExpectedInsertions: 500000
          windowSecs: 7200
        parallel:
          enabled: true
          concurrency: 8
          workStealing: true
        health:
          enabled: true
          checkIntervalSecs: 5
          maxLagMs: 10000
          autoRecovery: true
      topicConfig:
        partitions: 6
        replicationFactor: 2
  
  # S3 sink with generic config
  sinks:
    - name: s3-archive
      connector: s3
      topics:
        - "cdc.*"
      consumerGroup: s3-archiver
      enabled: true
      startOffset: earliest
      configSecretRef: s3-credentials  # Secret with AWS keys
      # All connector-specific config goes here
      config:
        bucket: my-data-lake
        region: us-east-1
        prefix: cdc/events
        format: parquet
        compression: zstd
        batchSize: 10000
        flushIntervalSeconds: 60
      rateLimit:
        eventsPerSecond: 10000
        burstCapacity: 1000
    
    # Apache Iceberg lakehouse sink
    - name: iceberg-lakehouse
      connector: iceberg
      topics:
        - "cdc.events"
      consumerGroup: iceberg-writer
      enabled: true
      startOffset: earliest
      configSecretRef: iceberg-credentials  # Secret with S3/catalog keys
      config:
        catalog:
          type: rest
          rest:
            uri: http://polaris:8181
          warehouse: s3://my-lakehouse/warehouse
        namespace: analytics
        table: cdc_events
        partitioning: day
        partitionFields: [event_date]
        commitMode: append
        batchSize: 50000
        flushIntervalSeconds: 300
        targetFileSizeMb: 128
        compression: zstd
        storage:
          s3:
            region: us-east-1
    
    # Debug stdout sink
    - name: debug-output
      connector: stdout
      topics:
        - "cdc.events"
      consumerGroup: debug
      enabled: true
      config:
        format: json
        pretty: true
        includeMetadata: true
    
    # Custom connector with generic config
    - name: custom-webhook
      connector: my-custom-sink
      topics:
        - "cdc.events"
      consumerGroup: custom
      enabled: true
      config:
        url: https://api.example.com/webhook
        method: POST
        headers:
          Content-Type: application/json
        batchSize: 100
  
  settings:
    topic:
      autoCreate: true
      defaultPartitions: 3
      defaultReplicationFactor: 2
    retry:
      maxRetries: 10
      initialBackoffMs: 100
      maxBackoffMs: 30000
    health:
      enabled: true
      port: 8080
    metrics:
      enabled: true
      port: 9091
  
  tls:
    enabled: true
    certSecretName: rivven-tls
```

```bash
kubectl apply -f cdc-pipeline.yaml
```

### Create an Advanced CDC Pipeline

Production-grade CDC with parallel processing, deduplication, and health monitoring:

```yaml
apiVersion: rivven.hupe1980.github.io/v1alpha1
kind: RivvenConnect
metadata:
  name: production-cdc
  namespace: default
spec:
  clusterRef:
    name: production
  
  replicas: 3
  version: "0.0.18"
  
  sources:
    - name: postgres-cdc-advanced
      connector: postgres-cdc
      topic: cdc.events
      enabled: true
      configSecretRef: postgres-credentials
      # All connector config in generic 'config' field (Kafka Connect style)
      config:
        slotName: rivven_production_slot
        publication: rivven_all_tables
        snapshotMode: initial
        decodingPlugin: pgoutput
        tables:
          - schema: public
            table: orders
          - schema: public
            table: customers
        
        # Advanced snapshot configuration
        snapshot:
          batchSize: 50000
          parallelTables: 8
          queryTimeoutSecs: 600
          maxRetries: 5
        
        # Incremental snapshot for zero-downtime re-snapshots
        incrementalSnapshot:
          enabled: true
          chunkSize: 5000
          watermarkStrategy: insert
          maxConcurrentChunks: 4
        
        # Signal table for ad-hoc snapshot control
        signal:
          enabled: true
          dataCollection: cdc.signals
          enabledChannels: [source, topic]
        
        # Heartbeat monitoring
        heartbeat:
          enabled: true
          intervalSecs: 5
          maxLagSecs: 60
          emitEvents: true
          topic: __cdc_heartbeat
        
        # Deduplication with bloom filter
        deduplication:
          enabled: true
          bloomExpectedInsertions: 1000000
          bloomFpp: 0.001
          lruSize: 100000
          windowSecs: 7200
        
        # Transaction metadata
        transactionTopic:
          enabled: true
          topicName: __cdc_transactions
          includeDataCollections: true
        
        # Event routing
        router:
          enabled: true
          defaultDestination: cdc.default
          deadLetterQueue: cdc.dlq
          rules:
            - conditionType: table
              conditionValue: orders
              destination: cdc.orders
              priority: 10
            - conditionType: operation
              conditionValue: DELETE
              destination: cdc.deletes
              priority: 5
        
        # Custom partitioning
        partitioner:
          enabled: true
          numPartitions: 32
          strategy: key_hash
          keyColumns: [id]
        
        # SMT transforms
        transforms:
          - type: extract_new_record_state
            config:
              addFields: "op,source.ts_ms"
              dropTombstones: false
          - type: mask_field
            config:
              fields: "email,phone"
              replacement: "***MASKED***"
        
        # Parallel processing
        parallel:
          enabled: true
          concurrency: 8
          perTableBuffer: 5000
          outputBuffer: 50000
          workStealing: true
        
        # Health monitoring
        health:
          enabled: true
          checkIntervalSecs: 5
          maxLagMs: 10000
          failureThreshold: 3
          autoRecovery: true
  
  settings:
    topic:
      autoCreate: true
      defaultPartitions: 6
      defaultReplicationFactor: 3
    retry:
      maxRetries: 10
      initialBackoffMs: 100
      maxBackoffMs: 30000
    health:
      enabled: true
      port: 8080
    metrics:
      enabled: true
      port: 9091
```

```bash
kubectl apply -f production-cdc.yaml
```


### Create a RivvenTopic

Manage topics declaratively for GitOps workflows:

```yaml
apiVersion: rivven.hupe1980.github.io/v1alpha1
kind: RivvenTopic
metadata:
  name: orders-events
  namespace: default
spec:
  clusterRef:
    name: production
  
  partitions: 12
  replicationFactor: 3
  
  config:
    # 7 days retention
    retentionMs: 604800000
    cleanupPolicy: delete
    compressionType: lz4
    minInsyncReplicas: 2
  
  # Access control
  acls:
    - principal: "user:order-service"
      operations: ["Read", "Write"]
    - principal: "user:analytics"
      operations: ["Read"]
  
  # Keep topic when CRD is deleted
  deleteOnRemove: false
```

```bash
kubectl apply -f orders-topic.yaml

# Check topic status
kubectl get rivventopics
kubectl describe rivventopic orders-events
```

### Create a RivvenSchemaRegistry

Deploy and manage a high-performance Schema Registry:

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
  version: "0.0.18"
  
  # Server configuration
  server:
    port: 8081
    bindAddress: "0.0.0.0"
    requestTimeoutMs: 30000
    corsEnabled: true
  
  # Schema storage
  storage:
    mode: broker  # or "memory"
    topic: _schemas
    replicationFactor: 3
    partitions: 1
  
  # Compatibility settings
  compatibility:
    defaultLevel: BACKWARD
    perSubject:
      "order-events-value": FULL
      "user-profile-value": FORWARD
  
  # Supported schema formats
  formats:
    avro: true
    jsonSchema: true
    protobuf: true
  
  # Authentication
  auth:
    enabled: true
    method: jwt
    jwt:
      issuerUrl: "https://auth.example.com"
      audience: "schema-registry"
      usernameClaim: "sub"
      rolesClaim: "groups"
  
  # TLS
  tls:
    enabled: true
    certSecretName: schema-registry-tls
    mtlsEnabled: false
  
  # Metrics
  metrics:
    enabled: true
    port: 9090
    path: /metrics
    serviceMonitorEnabled: true
  
  # Resource requests
  resources:
    requests:
      cpu: "500m"
      memory: 1Gi
    limits:
      cpu: "2"
      memory: 4Gi
```

```bash
kubectl apply -f schema-registry.yaml

# Check schema registry status
kubectl get rivvenschemaregistries
kubectl describe rivvenschemaregistry schema-registry
```

### Check Status

```bash
# Check cluster status
kubectl get rivvenclusters
kubectl describe rivvencluster production

# Check connect status
kubectl get rivvenconnects
kubectl describe rivvenconnect cdc-pipeline

# Check topic status
kubectl get rivventopics
kubectl describe rivventopic orders-events

# Check schema registry status
kubectl get rivvenschemaregistries
kubectl describe rivvenschemaregistry schema-registry
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RIVVEN OPERATOR                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐     ┌─────────────────────────────────┐   │
│  │  Controller     │────►│  Kubernetes API                  │   │
│  │                 │     │                                   │   │
│  │  • Watch CRDs   │◄────│  • RivvenCluster events          │   │
│  │  • Reconcile    │     │  • RivvenConnect events          │   │
│  │  • Update status│     │  • RivvenSchemaRegistry events   │   │
│  └─────────────────┘     │  • StatefulSet/Deployment status │   │
│           │              └─────────────────────────────────┘   │
│           ▼                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Managed Resources (RivvenCluster)           │   │
│  │                                                          │   │
│  │  • StatefulSet (rivven-{name})                          │   │
│  │  • Headless Service (rivven-{name}-headless)            │   │
│  │  • Client Service (rivven-{name})                       │   │
│  │  • ConfigMap (rivven-{name}-config)                     │   │
│  │  • PodDisruptionBudget (rivven-{name}-pdb)              │   │
│  └─────────────────────────────────────────────────────────┘   │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Managed Resources (RivvenConnect)           │   │
│  │                                                          │   │
│  │  • Deployment (rivven-connect-{name})                   │   │
│  │  • ConfigMap (rivven-connect-{name}-config)             │   │
│  │  • Service (rivven-connect-{name})                      │   │
│  │  • ServiceMonitor (optional, for Prometheus)            │   │
│  └─────────────────────────────────────────────────────────┘   │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │            Managed Resources (RivvenSchemaRegistry)      │   │
│  │                                                          │   │
│  │  • Deployment (rivven-schema-{name})                    │   │
│  │  • ConfigMap (rivven-schema-{name}-config)              │   │
│  │  • Service (rivven-schema-{name})                       │   │
│  │  • PodDisruptionBudget (rivven-schema-{name}-pdb)       │   │
│  │  • ServiceMonitor (optional, for Prometheus)            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Custom Resource Definitions

### RivvenCluster

Manages Rivven broker clusters with StatefulSets for data persistence.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replicas` | int | 3 | Number of broker replicas |
| `version` | string | latest | Rivven image version |
| `image` | string | `ghcr.io/hupe1980/rivven` | Container image |
| `storage.size` | string | `10Gi` | PVC size per broker |
| `storage.storageClassName` | string | `""` | Storage class |
| `resources` | ResourceRequirements | - | CPU/memory requests/limits |
| `config` | BrokerConfig | - | Broker configuration |
| `tls.enabled` | bool | false | Enable TLS |
| `metrics.enabled` | bool | true | Enable Prometheus metrics |

### RivvenConnect

Manages Rivven Connect instances for data pipelines.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `clusterRef.name` | string | required | RivvenCluster to connect to |
| `clusterRef.namespace` | string | same namespace | Cluster namespace |
| `replicas` | int | 1 | Number of connect workers |
| `version` | string | latest | Connect image version |
| `sources` | []SourceConnectorSpec | [] | Source connectors |
| `sinks` | []SinkConnectorSpec | [] | Sink connectors |
| `settings.topic.autoCreate` | bool | true | Auto-create topics |
| `settings.retry.maxRetries` | int | 10 | Max retry attempts |
| `tls.enabled` | bool | false | Enable TLS for broker |

#### Source Connector Spec

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | required | Unique connector name |
| `connector` | string | required | Type (postgres-cdc, mysql-cdc, http, datagen) |
| `topic` | string | required | Target topic |
| `topicRouting` | string | - | Topic pattern with placeholders |
| `enabled` | bool | true | Enable/disable connector |
| `postgresCdc` | PostgresCdcConfig | - | **Typed** PostgreSQL CDC config |
| `mysqlCdc` | MysqlCdcConfig | - | **Typed** MySQL CDC config |
| `http` | HttpSourceConfig | - | **Typed** HTTP source config |
| `datagen` | DatagenConfig | - | **Typed** Datagen config |
| `config` | object | {} | Generic config (for custom connectors) |
| `configSecretRef` | string | - | Secret for sensitive config |
| `topicConfig` | SourceTopicConfigSpec | - | Topic auto-creation settings |

**Note**: CDC connectors (postgres-cdc, mysql-cdc) have `tables` field inside their typed configs (e.g., `postgresCdc.tables`).

#### Sink Connector Spec

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | required | Unique connector name |
| `connector` | string | required | Type (stdout, s3, http, elasticsearch) |
| `topics` | []string | required | Topics to consume (supports wildcards) |
| `consumerGroup` | string | required | Consumer group for offsets |
| `enabled` | bool | true | Enable/disable connector |
| `startOffset` | string | latest | Starting offset (earliest, latest, timestamp) |
| `s3` | S3SinkConfig | - | **Typed** S3 sink config |
| `http` | HttpSinkConfig | - | **Typed** HTTP sink config |
| `stdout` | StdoutSinkConfig | - | **Typed** stdout sink config |
| `config` | object | {} | Generic config (for custom connectors) |
| `configSecretRef` | string | - | Secret for sensitive config |
| `rateLimit.eventsPerSecond` | int | 0 | Rate limit (0 = unlimited) |

#### Typed Source Configs

**PostgresCdcConfig** (for `connector: postgres-cdc`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `slotName` | string | auto | Replication slot name |
| `publication` | string | auto | PostgreSQL publication name |
| `snapshotMode` | string | initial | `initial`, `always`, `never`, `when_needed`, `initial_only`, `schema_only`, `recovery`, `exported`, `custom` |
| `decodingPlugin` | string | pgoutput | `pgoutput`, `wal2json`, `decoderbufs` |
| `includeTransactionMetadata` | bool | false | Include transaction info |
| `heartbeatIntervalMs` | int | 0 | Heartbeat interval (0 = disabled) |
| `signalTable` | string | - | Signal table for runtime control |
| `tables` | []TableSpec | [] | **Tables to capture from PostgreSQL** |
| `snapshot` | SnapshotCdcConfigSpec | - | Advanced snapshot configuration |
| `incrementalSnapshot` | IncrementalSnapshotSpec | - | Non-blocking incremental snapshot |
| `signal` | SignalTableSpec | - | Signal table for ad-hoc snapshots |
| `heartbeat` | HeartbeatCdcSpec | - | Heartbeat monitoring configuration |
| `deduplication` | DeduplicationCdcSpec | - | Event deduplication (bloom filter/LRU) |
| `transactionTopic` | TransactionTopicSpec | - | Transaction metadata topic |
| `schemaChangeTopic` | SchemaChangeTopicSpec | - | Schema change capture |
| `tombstone` | TombstoneCdcSpec | - | Tombstone event handling |
| `fieldEncryption` | FieldEncryptionSpec | - | Field-level encryption |
| `readOnlyReplica` | ReadOnlyReplicaSpec | - | PostgreSQL read replica support |
| `router` | EventRouterSpec | - | Event routing configuration |
| `partitioner` | PartitionerSpec | - | Custom partitioning strategy |
| `transforms` | []SmtTransformSpec | [] | SMT (Single Message Transforms) |
| `parallel` | ParallelCdcSpec | - | Parallel processing configuration |
| `outbox` | OutboxSpec | - | Outbox pattern configuration |
| `health` | HealthMonitorSpec | - | Health monitoring configuration |

**MysqlCdcConfig** (for `connector: mysql-cdc`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `serverId` | int | auto | Unique server ID for binlog replication |
| `snapshotMode` | string | initial | `initial`, `always`, `never`, `when_needed`, `initial_only`, `schema_only`, `recovery`, `exported`, `custom` |
| `includeGtid` | bool | false | Include GTID in events |
| `heartbeatIntervalMs` | int | 0 | Heartbeat interval |
| `databaseHistoryTopic` | string | auto | Topic for schema changes |
| `tables` | []TableSpec | [] | **Tables to capture from MySQL** |
| `snapshot` | SnapshotCdcConfigSpec | - | Advanced snapshot configuration |
| `incrementalSnapshot` | IncrementalSnapshotSpec | - | Non-blocking incremental snapshot |
| `signal` | SignalTableSpec | - | Signal table for ad-hoc snapshots |
| `heartbeat` | HeartbeatCdcSpec | - | Heartbeat monitoring configuration |
| `deduplication` | DeduplicationCdcSpec | - | Event deduplication (bloom filter/LRU) |
| `transactionTopic` | TransactionTopicSpec | - | Transaction metadata topic |
| `schemaChangeTopic` | SchemaChangeTopicSpec | - | Schema change capture |
| `tombstone` | TombstoneCdcSpec | - | Tombstone event handling |
| `fieldEncryption` | FieldEncryptionSpec | - | Field-level encryption |
| `router` | EventRouterSpec | - | Event routing configuration |
| `partitioner` | PartitionerSpec | - | Custom partitioning strategy |
| `transforms` | []SmtTransformSpec | [] | SMT (Single Message Transforms) |
| `parallel` | ParallelCdcSpec | - | Parallel processing configuration |
| `outbox` | OutboxSpec | - | Outbox pattern configuration |
| `health` | HealthMonitorSpec | - | Health monitoring configuration |

**DatagenConfig** (for `connector: datagen`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `eventsPerSecond` | int | 1 | Events per second to generate |
| `maxEvents` | int | 0 | Total events (0 = unlimited) |
| `schemaType` | string | json | Output schema type |
| `seed` | int | - | Random seed for reproducibility |

**ExternalSourceConfig** (for `connector: external-source`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | []string | required | External broker addresses |
| `topic` | string | required | External topic to consume from |
| `consumerGroup` | string | required | Consumer group ID |
| `startOffset` | string | latest | `earliest`, `latest` |
| `securityProtocol` | string | plaintext | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `saslMechanism` | string | - | `plain`, `scram-sha-256`, `scram-sha-512` |
| `saslUsername` | string | - | SASL username (use secret for password) |

**MqttSourceConfig** (for `connector: mqtt-source`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokerUrl` | string | required | MQTT broker URL (e.g., mqtt://broker:1883) |
| `topics` | []string | required | MQTT topics (supports wildcards: +, #) |
| `clientId` | string | - | MQTT client ID |
| `qos` | string | at_least_once | `at_most_once`, `at_least_once`, `exactly_once` |
| `cleanSession` | bool | true | Clean session on connect |
| `mqttVersion` | string | v311 | `v3`, `v311`, `v5` |
| `username` | string | - | Auth username (use secret for password) |

**SqsSourceConfig** (for `connector: sqs-source`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queueUrl` | string | required | SQS queue URL |
| `region` | string | required | AWS region (e.g., us-east-1) |
| `maxMessages` | int | 10 | Max messages per poll (1-10) |
| `waitTimeSeconds` | int | 20 | Long polling wait time |
| `visibilityTimeout` | int | 30 | Visibility timeout in seconds |
| `awsProfile` | string | - | AWS profile name |
| `roleArn` | string | - | IAM role ARN to assume |

**PubSubSourceConfig** (for `connector: pubsub-source`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `projectId` | string | required | GCP project ID |
| `subscription` | string | required | Pub/Sub subscription name |
| `topic` | string | - | Topic name (for auto-created subscriptions) |
| `maxMessages` | int | 100 | Max messages per pull (1-1000) |
| `ackDeadlineSeconds` | int | 30 | Ack deadline (10-600 seconds) |
| `credentialsFile` | string | - | Path to service account credentials |
| `useAdc` | bool | true | Use Application Default Credentials |

#### Typed Sink Configs

**S3SinkConfig** (for `connector: s3`, rivven-storage):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | required | S3 bucket name |
| `region` | string | - | AWS region |
| `endpointUrl` | string | - | Custom endpoint (MinIO, etc.) |
| `prefix` | string | - | Object key prefix |
| `format` | string | json | `json`, `jsonl`, `parquet`, `avro` |
| `compression` | string | none | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `batchSize` | int | 1000 | Events per file |
| `flushIntervalSeconds` | int | 60 | Flush interval |

**HttpSinkConfig** (for `connector: http`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | required | Target URL |
| `method` | string | POST | `POST`, `PUT`, `PATCH` |
| `contentType` | string | application/json | Content-Type header |
| `timeoutMs` | int | 30000 | Request timeout |
| `batchSize` | int | 100 | Events per request |

**ExternalSinkConfig** (for `connector: external-sink`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | []string | required | External broker addresses |
| `topic` | string | required | Target external topic |
| `acks` | string | all | `none`, `leader`, `all`, `0`, `1`, `-1` |
| `compression` | string | none | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `batchSize` | int | 16384 | Batch size in bytes |
| `lingerMs` | int | 0 | Linger time in milliseconds |
| `securityProtocol` | string | plaintext | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `saslMechanism` | string | - | `plain`, `scram-sha-256`, `scram-sha-512` |
| `saslUsername` | string | - | SASL username (use secret for password) |

**GcsSinkConfig** (for `connector: gcs`, rivven-storage):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | required | GCS bucket name |
| `prefix` | string | - | Object key prefix |
| `format` | string | jsonl | `json`, `jsonl`, `parquet`, `avro` |
| `compression` | string | none | `none`, `gzip` |
| `partitioning` | string | none | `none`, `daily`, `hourly` |
| `batchSize` | int | 1000 | Events per file |
| `flushIntervalSeconds` | int | 60 | Flush interval |
| `credentialsFile` | string | - | Path to service account JSON |
| `useAdc` | bool | true | Use Application Default Credentials |

**AzureBlobSinkConfig** (for `connector: azure-blob`, rivven-storage):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `accountName` | string | required | Storage account name |
| `container` | string | required | Container name |
| `prefix` | string | - | Blob prefix |
| `format` | string | jsonl | `json`, `jsonl`, `parquet`, `avro` |
| `compression` | string | none | `none`, `gzip` |
| `partitioning` | string | none | `none`, `daily`, `hourly` |
| `batchSize` | int | 1000 | Events per file |
| `flushIntervalSeconds` | int | 60 | Flush interval |

**SnowflakeSinkConfig** (for `connector: snowflake`, rivven-warehouse):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `account` | string | required | Snowflake account identifier |
| `user` | string | required | Snowflake user name |
| `privateKeyPath` | string | required | Path to PKCS#8 private key (use secret) |
| `database` | string | required | Target database |
| `schema` | string | required | Target schema |
| `table` | string | required | Target table |
| `warehouse` | string | - | Snowflake warehouse name |
| `role` | string | - | Snowflake role name |
| `batchSize` | int | 1000 | Rows per batch insert |

**BigQuerySinkConfig** (for `connector: bigquery`, rivven-warehouse):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `projectId` | string | required | GCP project ID |
| `datasetId` | string | required | BigQuery dataset ID |
| `tableId` | string | required | BigQuery table ID |
| `credentialsFile` | string | - | Path to service account JSON |
| `useAdc` | bool | true | Use Application Default Credentials |
| `batchSize` | int | 500 | Rows per insert request |
| `autoCreateTable` | bool | false | Auto-create table if not exists |

**RedshiftSinkConfig** (for `connector: redshift`, rivven-warehouse):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | required | Redshift cluster endpoint |
| `port` | int | 5439 | Redshift port |
| `database` | string | required | Database name |
| `user` | string | required | Redshift user name |
| `schema` | string | public | Target schema |
| `table` | string | required | Target table |
| `sslMode` | string | prefer | `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `batchSize` | int | 1000 | Rows per batch insert |

**IcebergSinkConfig** (for `connector: iceberg`, rivven-connect lakehouse):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog.type` | string | rest | `rest`, `hive`, `glue`, `jdbc`, `nessie`, `memory` |
| `catalog.rest.uri` | string | - | REST catalog URI (Polaris, Tabular, Lakekeeper) |
| `catalog.rest.credential` | string | - | Client credentials (client_id:client_secret) |
| `catalog.rest.token` | string | - | Bearer token for authentication |
| `catalog.warehouse` | string | - | Warehouse location (s3://bucket/warehouse) |
| `namespace` | string | required | Iceberg namespace/database |
| `table` | string | required | Iceberg table name |
| `partitioning` | string | table_default | `none`, `table_default`, `identity`, `bucket`, `year`, `month`, `day`, `hour` |
| `partitionFields` | []string | [] | Fields to partition by |
| `bucketCount` | int | - | Number of buckets for bucket partitioning |
| `commitMode` | string | append | `append`, `overwrite`, `upsert` |
| `upsertKeyFields` | []string | [] | Key fields for upsert/merge operations |
| `batchSize` | int | 10000 | Events per commit |
| `flushIntervalSeconds` | int | 60 | Flush interval |
| `targetFileSizeMb` | int | 128 | Target Parquet file size (rolling files) |
| `compression` | string | snappy | `none`, `snappy`, `gzip`, `lz4`, `zstd`, `brotli` |
| `schemaEvolution` | string | strict | `strict`, `add_columns`, `full` |
| `storage.s3.region` | string | - | AWS region |
| `storage.s3.endpointUrl` | string | - | Custom endpoint (MinIO) |
| `storage.s3.pathStyleAccess` | bool | false | Use path-style access (required for MinIO) |

**DeltaLakeSinkConfig** (for `connector: delta`, rivven-connect lakehouse):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tablePath` | string | required | Delta table path (s3://bucket/delta/table) |
| `partitionBy` | []string | [] | Partition columns |
| `writeMode` | string | append | `append`, `overwrite`, `merge` |
| `mergeKeyFields` | []string | [] | Key fields for merge operations |
| `targetFileSizeMb` | int | 128 | Target Parquet file size |
| `compression` | string | snappy | `none`, `snappy`, `gzip`, `lz4`, `zstd` |
| `batchSize` | int | 10000 | Events per commit |
| `flushIntervalSeconds` | int | 60 | Flush interval |
| `storage.s3.region` | string | - | AWS region |
| `storage.s3.endpointUrl` | string | - | Custom endpoint (MinIO) |

#### TableSpec (CDC Tables)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema` | string | - | Schema/namespace (e.g., "public") |
| `table` | string | required | Table name |
| `topic` | string | - | Override topic for this table |
| `columns` | []string | [] | Columns to include (empty = all) |
| `excludeColumns` | []string | [] | Columns to exclude |
| `columnMasks` | map[string]string | {} | Column masking rules |

#### Advanced CDC Configuration Types

These types provide production-grade CDC features for both PostgreSQL and MySQL connectors.

**SnapshotCdcConfigSpec** (initial data capture settings):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batchSize` | int | 10000 | SELECT query batch size (rows) |
| `parallelTables` | int | 4 | Tables to snapshot in parallel (1-32) |
| `queryTimeoutSecs` | int | 300 | Query timeout seconds (10-3600) |
| `throttleDelayMs` | int | 0 | Delay between batches for backpressure |
| `maxRetries` | int | 3 | Max retries per batch on failure |
| `includeTables` | []string | [] | Tables to include (empty = all) |
| `excludeTables` | []string | [] | Tables to exclude |

**IncrementalSnapshotSpec** (non-blocking re-snapshot):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable incremental snapshots |
| `chunkSize` | int | 1024 | Rows per chunk (100-100000) |
| `watermarkStrategy` | string | - | `insert` or `update_and_insert` |
| `watermarkSignalTable` | string | - | Signal table for watermark tracking |
| `maxConcurrentChunks` | int | 1 | Max concurrent chunks (1-16) |
| `chunkDelayMs` | int | 0 | Delay between chunks |

**SignalTableSpec** (ad-hoc snapshot control):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable signal processing |
| `dataCollection` | string | - | Signal table name (schema.table) |
| `topic` | string | - | Topic for signal messages |
| `enabledChannels` | []string | [] | Channels: `source`, `topic`, `file`, `api` |
| `pollIntervalMs` | int | 1000 | Poll interval for file channel |

**HeartbeatCdcSpec** (connection health monitoring):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable heartbeat monitoring |
| `intervalSecs` | int | 10 | Heartbeat interval (1-3600s) |
| `maxLagSecs` | int | 300 | Max allowed lag before unhealthy |
| `emitEvents` | bool | false | Emit heartbeat events to topic |
| `topic` | string | - | Topic for heartbeat events |
| `actionQuery` | string | - | SQL to execute on each heartbeat |

**DeduplicationCdcSpec** (duplicate event prevention):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable deduplication |
| `bloomExpectedInsertions` | int | 100000 | Bloom filter expected items |
| `bloomFpp` | float | 0.01 | Bloom filter false positive rate |
| `lruSize` | int | 10000 | LRU cache size |
| `windowSecs` | int | 3600 | Deduplication window (60-604800s) |

**TransactionTopicSpec** (transaction metadata):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable transaction topic |
| `topicName` | string | - | Transaction metadata topic |
| `includeDataCollections` | bool | true | Include affected tables list |
| `minEventsThreshold` | int | 0 | Min events to emit transaction |

**SchemaChangeTopicSpec** (DDL change capture):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable schema change capture |
| `topicName` | string | - | Schema change topic |
| `includeColumns` | bool | true | Include column details |
| `schemas` | []string | [] | Schemas to monitor (empty = all) |

**TombstoneCdcSpec** (delete event handling):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable tombstone handling |
| `afterDelete` | bool | true | Emit tombstone after delete |
| `behavior` | string | - | `emit_null` or `emit_with_key` |

**FieldEncryptionSpec** (field-level encryption):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable field encryption |
| `keySecretRef` | string | - | Secret containing encryption key |
| `fields` | []string | [] | Fields to encrypt |
| `algorithm` | string | aes-256-gcm | Encryption algorithm |

**ReadOnlyReplicaSpec** (PostgreSQL read replica support):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable read replica support |
| `lagThresholdMs` | int | 5000 | Max acceptable replication lag |
| `deduplicate` | bool | true | Deduplicate events across replicas |
| `watermarkSource` | string | - | `primary` or `replica` |

**EventRouterSpec** (dynamic event routing):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable event routing |
| `defaultDestination` | string | - | Default destination topic |
| `deadLetterQueue` | string | - | DLQ for unroutable events |
| `dropUnroutable` | bool | false | Drop events with no route |
| `rules` | []RouteRuleSpec | [] | Routing rules |

**RouteRuleSpec** (routing rule definition):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `conditionType` | string | required | `always`, `table`, `table_pattern`, `schema`, `operation`, `field_equals`, `field_exists` |
| `conditionValue` | string | - | Condition value/pattern |
| `destination` | string | required | Destination topic |
| `priority` | int | 0 | Rule priority (higher = first) |

**PartitionerSpec** (custom partitioning):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable custom partitioning |
| `numPartitions` | int | 16 | Number of partitions (1-1000) |
| `strategy` | string | key_hash | `round_robin`, `key_hash`, `table_hash`, `full_table_hash`, `sticky` |
| `keyColumns` | []string | [] | Columns for key-based partitioning |

**SmtTransformSpec** (Single Message Transform):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | required | Transform type (see below) |
| `config` | object | {} | Transform-specific configuration |

Supported transform types:
- `extract_new_record_state` - Extract after state from change events
- `mask_field` - Mask sensitive fields
- `filter` - Filter events by condition
- `flatten` - Flatten nested structures
- `cast` - Cast field types
- `insert_field` - Add static fields
- `replace_field` - Replace field values
- `value_to_key` - Promote value fields to key
- `regex_router` - Route by regex pattern
- `content_router` - Route by content
- `timestamp_router` - Route by timestamp
- `header_from` - Copy value to header
- `drop_headers` - Remove headers
- `message_timestamp` - Set message timestamp
- `unwrap_envelope` - Unwrap CDC envelope
- `convert_timezone` - Convert timestamps
- `drop_null` - Drop null fields

**ParallelCdcSpec** (parallel processing):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable parallel processing |
| `concurrency` | int | 4 | Worker thread count (1-64) |
| `perTableBuffer` | int | 1000 | Buffer per table (100-100000) |
| `outputBuffer` | int | 10000 | Output buffer size |
| `workStealing` | bool | true | Enable work stealing |
| `perTableRateLimit` | int | - | Events/sec per table |
| `shutdownTimeoutSecs` | int | 30 | Graceful shutdown timeout |

**OutboxSpec** (transactional outbox pattern):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable outbox pattern |
| `tableName` | string | outbox | Outbox table name |
| `pollIntervalMs` | int | 100 | Poll interval (10-60000ms) |
| `batchSize` | int | 100 | Batch size (1-10000) |
| `maxRetries` | int | 3 | Max delivery retries |
| `deliveryTimeoutSecs` | int | 30 | Delivery timeout |
| `orderedDelivery` | bool | true | Maintain message order |
| `retentionSecs` | int | 86400 | Message retention (1hr-30days) |
| `maxConcurrency` | int | 10 | Max concurrent deliveries |

**HealthMonitorSpec** (connector health monitoring):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable health monitoring |
| `checkIntervalSecs` | int | 10 | Health check interval (1-3600s) |
| `maxLagMs` | int | 30000 | Max acceptable lag |
| `failureThreshold` | int | 3 | Failures before unhealthy |
| `successThreshold` | int | 2 | Successes before healthy |
| `checkTimeoutSecs` | int | 5 | Health check timeout |
| `autoRecovery` | bool | true | Auto-recovery on failure |
| `recoveryDelaySecs` | int | 1 | Initial recovery delay |
| `maxRecoveryDelaySecs` | int | 60 | Max recovery delay (exponential backoff) |


### RivvenSchemaRegistry

Manages a high-performance Schema Registry for schema validation and evolution.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `clusterRef.name` | string | required | RivvenCluster to connect to |
| `clusterRef.namespace` | string | same namespace | Cluster namespace |
| `replicas` | int | 1 | Number of registry replicas |
| `version` | string | latest | Registry image version |
| `server.port` | int | 8081 | HTTP server port |
| `server.bindAddress` | string | `0.0.0.0` | Bind address |
| `server.requestTimeoutMs` | int | 30000 | Request timeout |
| `server.corsEnabled` | bool | false | Enable CORS |
| `storage.mode` | string | `broker` | Storage: `memory` or `broker` |
| `storage.topic` | string | `_schemas` | Schema storage topic |
| `storage.replicationFactor` | int | 1 | Topic replication factor |
| `compatibility.defaultLevel` | string | `BACKWARD` | Default compatibility level |
| `compatibility.perSubject` | map[string]string | {} | Per-subject compatibility overrides |
| `formats.avro` | bool | true | Enable Avro schemas |
| `formats.jsonSchema` | bool | true | Enable JSON Schema |
| `formats.protobuf` | bool | true | Enable Protobuf schemas |
| `auth.enabled` | bool | false | Enable authentication |
| `auth.method` | string | `basic` | Auth method: `basic`, `jwt`, `cedar` |
| `tls.enabled` | bool | false | Enable TLS |
| `tls.certSecretName` | string | - | TLS certificate secret |
| `tls.mtlsEnabled` | bool | false | Enable mutual TLS |
| `metrics.enabled` | bool | true | Enable Prometheus metrics |
| `metrics.port` | int | 9090 | Metrics port |
| `metrics.serviceMonitorEnabled` | bool | false | Create ServiceMonitor |

#### Schema Compatibility Levels

| Level | Description |
|-------|-------------|
| `NONE` | No compatibility checking |
| `BACKWARD` | New schema can read old data |
| `BACKWARD_TRANSITIVE` | All previous schemas can be read |
| `FORWARD` | Old schema can read new data |
| `FORWARD_TRANSITIVE` | All future schemas can read old data |
| `FULL` | Both backward and forward compatible |
| `FULL_TRANSITIVE` | Full compatibility with all versions |

#### Authentication Methods

**Basic Auth** (`auth.method: basic`):
```yaml
auth:
  enabled: true
  method: basic
  users:
    - username: admin
      passwordSecretKey: admin-password
      role: admin
    - username: readonly
      passwordSecretKey: ro-password
      role: reader
```

**JWT/OIDC Auth** (`auth.method: jwt`):
```yaml
auth:
  enabled: true
  method: jwt
  jwt:
    issuerUrl: "https://auth.example.com"
    jwksUrl: "https://auth.example.com/.well-known/jwks.json"
    audience: "schema-registry"
    usernameClaim: "sub"
    rolesClaim: "groups"
```

**Cedar Policy Auth** (`auth.method: cedar`):
```yaml
auth:
  enabled: true
  method: cedar
  cedar:
    policySecretRef: cedar-policies
```

#### External Registry Sync

Sync schemas with external registries:

```yaml
externalRegistry:
  enabled: true
  registryType: compatible  # or "glue"
  registryUrl: "https://external-sr.example.com"
  syncMode: mirror  # "mirror", "push", "bidirectional"
  syncSubjects:
    - "orders-*"
    - "users-*"
  syncIntervalSeconds: 300
  credentialsSecretRef: external-creds
```

## Configuration

### Operator Flags

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--metrics-addr` | `METRICS_ADDR` | `0.0.0.0:8080` | Metrics server address |
| `--health-addr` | `HEALTH_ADDR` | `0.0.0.0:8081` | Health probe address |
| `--leader-election` | `LEADER_ELECTION` | `true` | Enable leader election |
| `--namespace` | `WATCH_NAMESPACE` | `""` | Namespace to watch (empty = all) |

## Supported Connectors

### Built-in Sources (rivven-connect core)
- `postgres-cdc` - PostgreSQL Change Data Capture (logical replication)
- `mysql-cdc` - MySQL Change Data Capture (binlog replication)
- `http` - HTTP/Webhook source
- `datagen` - Data generator for testing

### Queue Sources (rivven-queue crate)
- `external-source` - External message queue consumer (for migrations)
- `mqtt-source` - MQTT broker subscriber (IoT data ingestion)
- `sqs-source` - AWS SQS queue consumer
- `pubsub-source` - Google Cloud Pub/Sub subscriber

### Built-in Sinks (rivven-connect core)
- `stdout` - Standard output (debugging)
- `http-webhook` - HTTP webhooks

### Queue Sinks (rivven-queue crate)
- `external-sink` - External message queue producer (hybrid deployments)

### Storage Sinks (rivven-storage crate)
- `s3` - Amazon S3 / MinIO object storage
- `gcs` - Google Cloud Storage
- `azure-blob` - Azure Blob Storage

### Warehouse Sinks (rivven-warehouse crate)
- `snowflake` - Snowflake Data Warehouse (Snowpipe Streaming)
- `bigquery` - Google BigQuery
- `redshift` - Amazon Redshift
- `clickhouse` - ClickHouse OLAP (native RowBinary over HTTP)

## Development

### Build

```bash
cargo build --release -p rivven-operator
```

### Run Locally (against kind/minikube)

```bash
# Install CRDs
kubectl apply -f deploy/crds/

# Run operator locally
cargo run -p rivven-operator -- --leader-election=false
```

### Run Tests

```bash
cargo test -p rivven-operator
```

### Generate CRD YAML

```bash
cargo run -p rivven-operator -- crd > deploy/crds/rivven.hupe1980.github.io_rivvenclusters.yaml
```

## Documentation

- [Kubernetes Deployment](https://rivven.hupe1980.github.io/rivven/docs/kubernetes)
- [Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)

## License

Apache-2.0. See [LICENSE](../../LICENSE).

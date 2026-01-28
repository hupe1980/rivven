# Rivven Kubernetes Operator

Production-grade Kubernetes operator for deploying and managing Rivven clusters and connectors.

## Features

- **Custom Resource Definitions**: 
  - `RivvenCluster` for declarative cluster management
  - `RivvenConnect` for declarative connector pipeline management
  - `RivvenTopic` for declarative topic management (GitOps-friendly)
- **Automated Reconciliation**: Continuous state management with eventual consistency
- **StatefulSet Management**: Ordered deployment, scaling, and rolling updates
- **Service Discovery**: Automatic headless service for broker discovery
- **Configuration Management**: ConfigMaps for broker and connector configuration
- **Secret Management**: Secure credential handling for sources and sinks
- **Metrics**: Prometheus-compatible operator metrics
- **Finalizers**: Clean resource cleanup on deletion

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
apiVersion: rivven.io/v1alpha1
kind: RivvenCluster
metadata:
  name: production
  namespace: default
spec:
  replicas: 3
  version: "0.0.1"
  
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

```yaml
apiVersion: rivven.io/v1alpha1
kind: RivvenConnect
metadata:
  name: cdc-pipeline
  namespace: default
spec:
  clusterRef:
    name: production
  
  replicas: 2
  version: "0.0.1"
  
  # PostgreSQL CDC source with TYPED config
  sources:
    - name: postgres-cdc
      connector: postgres-cdc
      topic: cdc.events
      enabled: true
      configSecretRef: postgres-credentials  # Secret with host/port/user/pass
      # Typed PostgreSQL CDC configuration (validated by CRD)
      postgresCdc:
        slotName: rivven_cdc_slot
        publication: rivven_pub
        snapshotMode: initial
        decodingPlugin: pgoutput
        heartbeatIntervalMs: 10000
        # Table selection with column filtering (inside CDC config)
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
      topicConfig:
        partitions: 6
        replicationFactor: 2
  
  # S3 sink with TYPED config
  sinks:
    - name: s3-archive
      connector: s3
      topics:
        - "cdc.*"
      consumerGroup: s3-archiver
      enabled: true
      startOffset: earliest
      configSecretRef: s3-credentials  # Secret with AWS keys
      # Typed S3 configuration (validated by CRD)
      s3:
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
    
    # Debug stdout sink with TYPED config
    - name: debug-output
      connector: stdout
      topics:
        - "cdc.events"
      consumerGroup: debug
      enabled: true
      stdout:
        format: json
        pretty: true
        includeMetadata: true
    
    # Custom connector with GENERIC config
    - name: custom-webhook
      connector: my-custom-sink
      topics:
        - "cdc.events"
      consumerGroup: custom
      enabled: true
      # Generic config for custom connectors
      config:
        customField: value
        nested:
          option: true
  
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

### Create a RivvenTopic

Manage topics declaratively for GitOps workflows:

```yaml
apiVersion: rivven.io/v1alpha1
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
│  │  • Update status│     │  • StatefulSet/Deployment status │   │
│  └─────────────────┘     └─────────────────────────────────┘   │
│           │                                                     │
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
| `snapshotMode` | string | initial | `initial`, `never`, `when_needed`, `exported` |
| `decodingPlugin` | string | pgoutput | `pgoutput`, `wal2json`, `decoderbufs` |
| `includeTransactionMetadata` | bool | false | Include transaction info |
| `heartbeatIntervalMs` | int | 0 | Heartbeat interval (0 = disabled) |
| `signalTable` | string | - | Signal table for runtime control |
| `tables` | []TableSpec | [] | **Tables to capture from PostgreSQL** |

**MysqlCdcConfig** (for `connector: mysql-cdc`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `serverId` | int | auto | Unique server ID for binlog replication |
| `snapshotMode` | string | initial | `initial`, `never`, `when_needed`, `schema_only` |
| `includeGtid` | bool | false | Include GTID in events |
| `heartbeatIntervalMs` | int | 0 | Heartbeat interval |
| `databaseHistoryTopic` | string | auto | Topic for schema changes |
| `tables` | []TableSpec | [] | **Tables to capture from MySQL** |

**DatagenConfig** (for `connector: datagen`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `eventsPerSecond` | int | 1 | Events per second to generate |
| `maxEvents` | int | 0 | Total events (0 = unlimited) |
| `schemaType` | string | json | Output schema type |
| `seed` | int | - | Random seed for reproducibility |

**KafkaSourceConfig** (for `connector: kafka-source`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | []string | required | Kafka broker addresses |
| `topic` | string | required | Kafka topic to consume from |
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

**KafkaSinkConfig** (for `connector: kafka-sink`, rivven-queue):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | []string | required | Kafka broker addresses |
| `topic` | string | required | Target Kafka topic |
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

#### TableSpec (CDC Tables)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema` | string | - | Schema/namespace (e.g., "public") |
| `table` | string | required | Table name |
| `topic` | string | - | Override topic for this table |
| `columns` | []string | [] | Columns to include (empty = all) |
| `excludeColumns` | []string | [] | Columns to exclude |
| `columnMasks` | map[string]string | {} | Column masking rules |

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
- `kafka-source` - Apache Kafka consumer (migration from Kafka)
- `mqtt-source` - MQTT broker subscriber (IoT data ingestion)
- `sqs-source` - AWS SQS queue consumer
- `pubsub-source` - Google Cloud Pub/Sub subscriber

### Built-in Sinks (rivven-connect core)
- `stdout` - Standard output (debugging)
- `http-webhook` - HTTP webhooks

### Queue Sinks (rivven-queue crate)
- `kafka-sink` - Apache Kafka producer (hybrid deployments)

### Storage Sinks (rivven-storage crate)
- `s3` - Amazon S3 / MinIO object storage
- `gcs` - Google Cloud Storage
- `azure-blob` - Azure Blob Storage

### Warehouse Sinks (rivven-warehouse crate)
- `snowflake` - Snowflake Data Warehouse (Snowpipe Streaming)
- `bigquery` - Google BigQuery
- `redshift` - Amazon Redshift

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
cargo run -p rivven-operator -- crd > deploy/crds/rivven.io_rivvenclusters.yaml
```

## License

See root [LICENSE](../../LICENSE) file.

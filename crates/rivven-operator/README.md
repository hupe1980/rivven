# Rivven Kubernetes Operator

Production-grade Kubernetes operator for deploying and managing Rivven clusters and connectors.

## Features

- **Custom Resource Definitions**: 
  - `RivvenCluster` for declarative cluster management
  - `RivvenConnect` for declarative connector pipeline management
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
  version: "0.1.0"
  
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
  version: "0.1.0"
  
  # PostgreSQL CDC source
  sources:
    - name: postgres-cdc
      connector: postgres-cdc
      topic: cdc.events
      enabled: true
      configSecretRef: postgres-credentials
      tables:
        - schema: public
          table: orders
        - schema: public
          table: customers
      topicConfig:
        partitions: 6
        replicationFactor: 2
  
  # S3 sink for data lake
  sinks:
    - name: s3-archive
      connector: s3
      topics:
        - "cdc.*"
      consumerGroup: s3-archiver
      enabled: true
      startOffset: earliest
      configSecretRef: s3-credentials
      rateLimit:
        eventsPerSecond: 10000
        burstCapacity: 1000
    
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

### Check Status

```bash
# Check cluster status
kubectl get rivvenclusters
kubectl describe rivvencluster production

# Check connect status
kubectl get rivvenconnects
kubectl describe rivvenconnect cdc-pipeline
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
| `config` | object | {} | Connector-specific config |
| `configSecretRef` | string | - | Secret for sensitive config |
| `tables` | []TableSpec | [] | Tables to capture |

#### Sink Connector Spec

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | required | Unique connector name |
| `connector` | string | required | Type (stdout, s3, http, elasticsearch) |
| `topics` | []string | required | Topics to consume (supports wildcards) |
| `consumerGroup` | string | required | Consumer group for offsets |
| `enabled` | bool | true | Enable/disable connector |
| `startOffset` | string | latest | Starting offset (earliest, latest, timestamp) |
| `config` | object | {} | Connector-specific config |
| `configSecretRef` | string | - | Secret for sensitive config |
| `rateLimit.eventsPerSecond` | int | 0 | Rate limit (0 = unlimited) |

## Configuration

### Operator Flags

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--metrics-addr` | `METRICS_ADDR` | `0.0.0.0:8080` | Metrics server address |
| `--health-addr` | `HEALTH_ADDR` | `0.0.0.0:8081` | Health probe address |
| `--leader-election` | `LEADER_ELECTION` | `true` | Enable leader election |
| `--namespace` | `WATCH_NAMESPACE` | `""` | Namespace to watch (empty = all) |

## Supported Connectors

### Sources
- `postgres-cdc` - PostgreSQL Change Data Capture
- `mysql-cdc` - MySQL Change Data Capture
- `http` - HTTP/Webhook source
- `datagen` - Data generator for testing

### Sinks
- `stdout` - Standard output (debugging)
- `s3` - Amazon S3 / MinIO
- `http` - HTTP webhooks
- `elasticsearch` - Elasticsearch

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

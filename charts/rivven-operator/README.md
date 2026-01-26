# Rivven Operator Helm Chart

A Helm chart for deploying the Rivven Kubernetes Operator.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.0+
- kubectl configured to communicate with your cluster

## Installation

### Quick Start

```bash
# Add the Rivven Helm repository (if published)
helm repo add rivven https://hupe1980.github.io/rivven
helm repo update

# Install the operator
helm install rivven-operator rivven/rivven-operator -n rivven-system --create-namespace
```

### Install from Local Chart

```bash
# From the repository root
helm install rivven-operator ./charts/rivven-operator -n rivven-system --create-namespace
```

### Custom Values

```bash
# Install with custom values
helm install rivven-operator ./charts/rivven-operator \
  -n rivven-system \
  --create-namespace \
  -f custom-values.yaml
```

## Configuration

See [values.yaml](values.yaml) for the full list of configurable parameters.

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of operator replicas | `1` |
| `image.repository` | Operator image repository | `ghcr.io/hupe1980/rivven-operator` |
| `image.tag` | Operator image tag | Chart appVersion |
| `operator.leaderElection` | Enable leader election | `true` |
| `operator.watchNamespace` | Namespace to watch (empty = all) | `""` |
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `128Mi` |
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |

### Security Configuration

```yaml
# Example: Hardened security settings
podSecurityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault

securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop:
      - ALL
```

### High Availability

```yaml
# Example: HA configuration
replicaCount: 3
operator:
  leaderElection: true

podDisruptionBudget:
  enabled: true
  minAvailable: 1

affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              app.kubernetes.io/name: rivven-operator
          topologyKey: topology.kubernetes.io/zone
```

## Creating a Rivven Cluster

Once the operator is installed, create a RivvenCluster resource:

```yaml
apiVersion: rivven.io/v1alpha1
kind: RivvenCluster
metadata:
  name: my-cluster
spec:
  replicas: 3
  storage:
    size: "50Gi"
    storageClassName: standard
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2"
      memory: "4Gi"
```

```bash
kubectl apply -f my-cluster.yaml
```

## Creating a RivvenConnect Pipeline

Create a RivvenConnect resource to deploy connectors:

```yaml
apiVersion: rivven.io/v1alpha1
kind: RivvenConnect
metadata:
  name: my-pipeline
spec:
  clusterRef:
    name: my-cluster
  replicas: 2
  
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
  
  sinks:
    - name: debug-output
      connector: stdout
      topics:
        - "cdc.*"
      consumerGroup: debug
      enabled: true
      config:
        format: json
        pretty: true
  
  settings:
    topic:
      autoCreate: true
      defaultPartitions: 3
    health:
      enabled: true
      port: 8080
    metrics:
      enabled: true
      port: 9091
```

```bash
kubectl apply -f my-pipeline.yaml
```

## Uninstallation

```bash
# Uninstall the operator
helm uninstall rivven-operator -n rivven-system

# Optional: Remove CRDs (WARNING: this will delete all RivvenCluster and RivvenConnect resources)
kubectl delete crd rivvenclusters.rivven.io
kubectl delete crd rivvenconnects.rivven.io

# Optional: Remove namespace
kubectl delete namespace rivven-system
```

## Upgrading

```bash
# Upgrade to a new version
helm upgrade rivven-operator rivven/rivven-operator -n rivven-system

# Upgrade with new values
helm upgrade rivven-operator ./charts/rivven-operator \
  -n rivven-system \
  -f new-values.yaml
```

## Troubleshooting

### Check Operator Status

```bash
# Check operator pods
kubectl get pods -n rivven-system

# Check operator logs
kubectl logs -n rivven-system -l app.kubernetes.io/name=rivven-operator

# Check CRD installation
kubectl get crd rivvenclusters.rivven.io
kubectl get crd rivvenconnects.rivven.io
```

### Check Cluster Status

```bash
# List all Rivven clusters
kubectl get rivvenclusters -A

# Describe a specific cluster
kubectl describe rivvencluster my-cluster

# Check cluster events
kubectl get events --field-selector involvedObject.name=my-cluster
```

### Check Connect Status

```bash
# List all RivvenConnect instances
kubectl get rivvenconnects -A

# Describe a specific connect instance
kubectl describe rivvenconnect my-pipeline

# Check connect events
kubectl get events --field-selector involvedObject.name=my-pipeline
```

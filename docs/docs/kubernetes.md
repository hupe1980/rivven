---
layout: default
title: Kubernetes
nav_order: 7
---

# Kubernetes Deployment
{: .no_toc }

Production-ready Kubernetes deployment.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven is designed for **cloud-native deployment** with:

- **StatefulSet** for ordered, stable pod identities
- **Headless Service** for peer discovery
- **PersistentVolumeClaims** for data durability
- **Horizontal Pod Autoscaler** for dynamic scaling
- **Prometheus ServiceMonitor** for observability

---

## Quick Start

### Helm Installation

```bash
# Add Rivven Helm repository
helm repo add rivven https://hupe1980.github.io/rivven/helm
helm repo update

# Install Rivven cluster
helm install rivven rivven/rivven \
  --namespace rivven \
  --create-namespace \
  --set cluster.replicas=3 \
  --set storage.size=100Gi
```

### Verify Deployment

```bash
# Check pods
kubectl get pods -n rivven -w

# Expected output:
# NAME       READY   STATUS    RESTARTS   AGE
# rivven-0   1/1     Running   0          2m
# rivven-1   1/1     Running   0          1m
# rivven-2   1/1     Running   0          30s

# Check cluster status
kubectl exec -n rivven rivven-0 -- rivven cluster status
```

---

## Helm Chart Values

### Basic Configuration

```yaml
# values.yaml
cluster:
  replicas: 3
  
image:
  repository: ghcr.io/hupe1980/rivven
  tag: latest
  pullPolicy: IfNotPresent

storage:
  size: 100Gi
  storageClass: ""  # Use default

resources:
  requests:
    cpu: "1"
    memory: 2Gi
  limits:
    cpu: "4"
    memory: 8Gi

service:
  type: ClusterIP
  port: 9292
```

### High Availability

```yaml
# ha-values.yaml
cluster:
  replicas: 5

affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app.kubernetes.io/name: rivven
        topologyKey: kubernetes.io/hostname

topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: rivven

persistence:
  storageClass: premium-ssd
  size: 500Gi
```

### TLS Configuration

```yaml
# tls-values.yaml
tls:
  enabled: true
  certManager:
    enabled: true
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
  
  # Or use existing secret
  # existingSecret: rivven-tls

mtls:
  enabled: true
  clientCA:
    secretName: rivven-client-ca
```

### Authentication

```yaml
auth:
  enabled: true
  mechanism: mtls  # mtls, scram, token
  
  # For SCRAM
  scram:
    existingSecret: rivven-users
  
  rbac:
    enabled: true
    configMapName: rivven-rbac
```

---

## Rivven Operator (CRDs)

The Rivven Operator provides **declarative management** via Custom Resource Definitions.

### Install the Operator

```bash
# Install CRDs
kubectl apply -f https://github.com/hupe1980/rivven/releases/latest/download/rivven-crds.yaml

# Deploy operator
kubectl apply -f https://github.com/hupe1980/rivven/releases/latest/download/rivven-operator.yaml
```

### RivvenCluster CRD

```yaml
apiVersion: rivven.io/v1alpha1
kind: RivvenCluster
metadata:
  name: production
spec:
  replicas: 3
  version: "0.0.1"
  storage:
    size: 100Gi
    storageClassName: fast-ssd
  config:
    defaultPartitions: 3
    defaultReplicationFactor: 2
  metrics:
    enabled: true
```

### RivvenConnect CRD

Manage CDC pipelines declaratively with **typed connector configs**:

```yaml
apiVersion: rivven.io/v1alpha1
kind: RivvenConnect
metadata:
  name: cdc-pipeline
spec:
  clusterRef:
    name: production
  replicas: 2
  
  sources:
    - name: postgres-cdc
      connector: postgres-cdc
      topic: cdc.events
      configSecretRef: postgres-credentials
      # Typed PostgreSQL CDC config (validated)
      postgresCdc:
        slotName: rivven_slot
        publication: rivven_pub
        snapshotMode: initial
        decodingPlugin: pgoutput
        # Tables are inside CDC config (type-safe)
        tables:
          - schema: public
            table: orders
            columns: [id, customer_id, total]
          - schema: public
            table: customers
            columnMasks:
              email: "***@***.***"
  
  sinks:
    - name: s3-archive
      connector: s3
      topics: ["cdc.*"]
      consumerGroup: s3-archiver
      configSecretRef: s3-credentials
      # Typed S3 config (validated)
      s3:
        bucket: data-lake
        format: parquet
        compression: zstd
```

### Custom Connectors

For custom connectors, use the generic `config` field:

```yaml
sources:
  - name: my-custom-source
    connector: my-plugin
    topic: custom.events
    config:  # Generic JSON for custom connectors
      customField: value
      nested:
        option: true
```

### RivvenTopic CRD

Manage topics declaratively for **GitOps workflows**:

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
    retentionMs: 604800000      # 7 days
    cleanupPolicy: delete
    compressionType: lz4
    minInsyncReplicas: 2
  
  acls:
    - principal: "user:order-service"
      operations: ["Read", "Write"]
    - principal: "user:analytics"
      operations: ["Read"]
  
  # Keep topic when CRD is deleted
  deleteOnRemove: false
  
  topicLabels:
    team: orders
    environment: production
```

#### Topic Configuration Options

| Field | Description | Default |
|-------|-------------|---------|
| `retentionMs` | Retention time in milliseconds | 604800000 (7 days) |
| `retentionBytes` | Retention size per partition (-1 = unlimited) | -1 |
| `segmentBytes` | Segment file size | 1073741824 (1GB) |
| `cleanupPolicy` | `delete`, `compact`, or `delete,compact` | `delete` |
| `compressionType` | `none`, `gzip`, `snappy`, `lz4`, `zstd` | `lz4` |
| `minInsyncReplicas` | Minimum ISR for writes | 1 |
| `maxMessageBytes` | Maximum message size | 1048576 (1MB) |

#### Check Topic Status

```bash
# List all topics
kubectl get rivventopics

# NAME            CLUSTER     PARTITIONS   REPLICATION   PHASE   AGE
# orders-events   production  12           3             Ready   5m

# Detailed status
kubectl describe rivventopic orders-events
```

---

## Manual Deployment

### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: rivven
  labels:
    app.kubernetes.io/name: rivven
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rivven-config
  namespace: rivven
data:
  rivven.yaml: |
    node:
      data_dir: /data
    
    cluster:
      bootstrap_expect: 3
      transport: quic
    
    storage:
      segment_size: 1073741824  # 1 GB
      retention_bytes: 107374182400  # 100 GB
    
    observability:
      metrics:
        enabled: true
        port: 9090
```

### StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rivven
  namespace: rivven
spec:
  serviceName: rivven-headless
  replicas: 3
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app.kubernetes.io/name: rivven
  template:
    metadata:
      labels:
        app.kubernetes.io/name: rivven
    spec:
      terminationGracePeriodSeconds: 60
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: rivven
          image: ghcr.io/hupe1980/rivven:latest
          ports:
            - name: client
              containerPort: 9292
            - name: cluster
              containerPort: 9393
            - name: metrics
              containerPort: 9090
          env:
            - name: RIVVEN_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: RIVVEN_ADVERTISE_ADDR
              value: "$(RIVVEN_NODE_ID).rivven-headless.rivven.svc.cluster.local:9393"
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/rivven
          resources:
            requests:
              cpu: "1"
              memory: 2Gi
            limits:
              cpu: "4"
              memory: 8Gi
          livenessProbe:
            httpGet:
              path: /health/live
              port: 9090
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health/ready
              port: 9090
            initialDelaySeconds: 10
            periodSeconds: 5
          startupProbe:
            httpGet:
              path: /health/startup
              port: 9090
            failureThreshold: 30
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: rivven-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 100Gi
```

### Services

```yaml
# Headless service for StatefulSet
apiVersion: v1
kind: Service
metadata:
  name: rivven-headless
  namespace: rivven
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: rivven
  ports:
    - name: cluster
      port: 9393
---
# Client service
apiVersion: v1
kind: Service
metadata:
  name: rivven
  namespace: rivven
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: rivven
  ports:
    - name: client
      port: 9292
      targetPort: 9292
```

### Ingress (External Access)

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: rivven
  namespace: rivven
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - rivven.example.com
      secretName: rivven-tls
  rules:
    - host: rivven.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: rivven
                port:
                  number: 9292
```

---

## Scaling

### Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: rivven
  namespace: rivven
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: rivven
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 60
```

### Manual Scaling

```bash
# Scale up
kubectl scale statefulset rivven -n rivven --replicas=5

# Scale down (graceful)
kubectl scale statefulset rivven -n rivven --replicas=3
```

### Storage Expansion

```bash
# Expand PVC (if storage class supports it)
kubectl patch pvc data-rivven-0 -n rivven \
  -p '{"spec":{"resources":{"requests":{"storage":"200Gi"}}}}'
```

---

## Monitoring

### ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: rivven
  namespace: rivven
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: rivven
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

### PodMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: rivven
  namespace: rivven
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: rivven
  podMetricsEndpoints:
    - port: metrics
      interval: 15s
```

### Grafana Dashboard

Import the Rivven dashboard:

```bash
kubectl apply -f https://raw.githubusercontent.com/hupe1980/rivven/main/deploy/grafana-dashboard.yaml
```

### Alerting Rules

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: rivven-alerts
  namespace: rivven
spec:
  groups:
    - name: rivven
      rules:
        - alert: RivvenNodeDown
          expr: up{job="rivven"} == 0
          for: 1m
          labels:
            severity: critical
          annotations:
            summary: "Rivven node {{ $labels.pod }} is down"
        
        - alert: RivvenHighLag
          expr: rivven_consumer_lag_records > 10000
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High consumer lag on {{ $labels.topic }}"
        
        - alert: RivvenDiskUsageHigh
          expr: (rivven_storage_bytes_used / rivven_storage_bytes_total) > 0.85
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Disk usage > 85% on {{ $labels.pod }}"
```

---

## Backup & Recovery

### Scheduled Backups

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: rivven-backup
  namespace: rivven
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: ghcr.io/hupe1980/rivven:latest
              command:
                - /bin/sh
                - -c
                - |
                  rivven backup create \
                    --output s3://backups/rivven/$(date +%Y%m%d) \
                    --compress
              env:
                - name: AWS_ACCESS_KEY_ID
                  valueFrom:
                    secretKeyRef:
                      name: aws-credentials
                      key: access-key
                - name: AWS_SECRET_ACCESS_KEY
                  valueFrom:
                    secretKeyRef:
                      name: aws-credentials
                      key: secret-key
          restartPolicy: OnFailure
```

### Recovery

```bash
# List backups
rivven backup list --source s3://backups/rivven/

# Restore to new cluster
rivven backup restore \
  --source s3://backups/rivven/20260125 \
  --target /data
```

---

## Troubleshooting

### Check Cluster Health

```bash
# Cluster status
kubectl exec -n rivven rivven-0 -- rivven cluster status

# Node info
kubectl exec -n rivven rivven-0 -- rivven cluster nodes

# Raft status
kubectl exec -n rivven rivven-0 -- rivven cluster raft-status
```

### View Logs

```bash
# All pods
kubectl logs -n rivven -l app.kubernetes.io/name=rivven --tail=100

# Specific pod
kubectl logs -n rivven rivven-0 -f

# Previous container
kubectl logs -n rivven rivven-0 --previous
```

### Debug Pod

```bash
# Shell into pod
kubectl exec -n rivven -it rivven-0 -- /bin/sh

# Check disk usage
kubectl exec -n rivven rivven-0 -- df -h /data

# Check network connectivity
kubectl exec -n rivven rivven-0 -- \
  nc -zv rivven-1.rivven-headless.rivven.svc.cluster.local 9393
```

### Common Issues

| Issue | Solution |
|:------|:---------|
| Pods stuck in Pending | Check PVC binding, storage class |
| Split brain | Verify network policies, check quorum |
| High latency | Check resource limits, disk IOPS |
| OOM kills | Increase memory limits |

---

## Production Checklist

- [ ] 3+ replicas across availability zones
- [ ] Pod anti-affinity rules configured
- [ ] PersistentVolumes with adequate IOPS
- [ ] TLS enabled for all traffic
- [ ] Network policies applied
- [ ] Resource requests and limits set
- [ ] Liveness/readiness probes configured
- [ ] Monitoring and alerting enabled
- [ ] Backup schedule configured
- [ ] Disaster recovery tested

---

## Next Steps

- [Security](/rivven/docs/security) - Kubernetes security hardening
- [Architecture](/rivven/docs/architecture) - Distributed design
- [Getting Started](/rivven/docs/getting-started) - Basic operations

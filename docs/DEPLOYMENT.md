# Deployment Guide

This guide covers deploying the Sensor Data Query Service in various environments.

## Quick Start

### Docker Compose (Recommended for Development)

```bash
# Clone repository
git clone <repo-url>
cd sensor-data-query-service

# Configure environment
cp .env.example .env
# Edit .env with your configuration

# Start service
make dev
```

Access the service at http://localhost:8080

### Docker (Single Container)

```bash
# Build image
docker build -t sensor-query-service .

# Run container
docker run \
  -p 8080:8080 \
  --env-file .env \
  -v /path/to/data:/data:ro \
  sensor-query-service
```

## Production Deployments

### Kubernetes

#### Prerequisites

- Kubernetes cluster (v1.20+)
- kubectl configured
- Storage backend (Azure Blob Storage or NFS)

#### Deploy Steps

1. **Create namespace and secrets:**

```bash
kubectl apply -f deployment/kubernetes/namespace.yaml

# Edit secret with your values
cp deployment/kubernetes/secret.yaml deployment/kubernetes/secret-local.yaml
# Edit secret-local.yaml
kubectl apply -f deployment/kubernetes/secret-local.yaml
```

2. **Deploy configuration:**

```bash
kubectl apply -f deployment/kubernetes/configmap.yaml
```

3. **Deploy service:**

```bash
kubectl apply -f deployment/kubernetes/deployment.yaml
```

4. **Verify deployment:**

```bash
kubectl get pods -n sensor-query
kubectl logs -f deployment/query-service -n sensor-query
```

5. **Access service:**

```bash
# Port forward for testing
kubectl port-forward svc/query-service 8080:8080 -n sensor-query

# Or configure ingress for external access
```

#### Auto-scaling

The deployment includes Horizontal Pod Autoscaler (HPA):

- **Min replicas**: 2
- **Max replicas**: 10
- **CPU threshold**: 70%
- **Memory threshold**: 80%

### Azure Container Instances

```bash
# Create resource group
az group create --name sensor-query-rg --location eastus

# Deploy container
az container create \
  --resource-group sensor-query-rg \
  --name sensor-query-service \
  --image your-registry/sensor-query-service:latest \
  --cpu 2 \
  --memory 4 \
  --ports 8080 \
  --environment-variables \
    STORAGE_MODE="azure" \
    AZURE_CONTAINER_NAME="sensor-data-cold-storage" \
    CACHE_ENABLED="true" \
    CACHE_SIZE_MB="1024" \
  --secure-environment-variables \
    AZURE_STORAGE_ACCOUNT="youraccount" \
    AZURE_STORAGE_KEY="yourkey"
```

### AWS ECS (Fargate)

1. **Create task definition:**

```json
{
  "family": "sensor-query-service",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::account:role/ecsTaskExecutionRole",
  "containerDefinitions": [
    {
      "name": "query-service",
      "image": "your-registry/sensor-query-service:latest",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {"name": "STORAGE_MODE", "value": "azure"},
        {"name": "CACHE_ENABLED", "value": "true"}
      ],
      "secrets": [
        {"name": "AZURE_STORAGE_KEY", "valueFrom": "arn:aws:ssm:region:account:parameter/sensor-query/azure-key"}
      ]
    }
  ]
}
```

2. **Create service:**

```bash
aws ecs create-service \
  --cluster your-cluster \
  --service-name sensor-query-service \
  --task-definition sensor-query-service \
  --desired-count 2 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-123],securityGroups=[sg-123],assignPublicIp=ENABLED}"
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_MODE` | `hybrid` | Storage mode: azure, local, hybrid |
| `AZURE_STORAGE_ACCOUNT` | - | Azure storage account name |
| `AZURE_STORAGE_KEY` | - | Azure storage access key |
| `LOCAL_STORAGE_PATH` | `/data/raw` | Local storage path |
| `CACHE_ENABLED` | `true` | Enable query caching |
| `CACHE_SIZE_MB` | `512` | Cache size in MB |
| `MAX_QUERY_DURATION_HOURS` | `168` | Maximum query duration (7 days) |
| `API_PORT` | `8080` | API server port |
| `LOG_LEVEL` | `INFO` | Logging level |

### Storage Backends

#### Azure Blob Storage

Required for `azure` or `hybrid` storage mode:

```env
STORAGE_MODE=azure
AZURE_STORAGE_ACCOUNT=youraccount
AZURE_STORAGE_KEY=yourkey
AZURE_CONTAINER_NAME=sensor-data-cold-storage
```

#### Local File System

Required for `local` or `hybrid` storage mode:

```env
STORAGE_MODE=local
LOCAL_STORAGE_PATH=/data/raw
```

Data should be organized in the hierarchy:
```
/data/raw/
├── asset_001/
│   ├── 2024/01/15/14/
│   │   ├── quad_ch1.parquet
│   │   └── quad_ch2.parquet
```

#### Hybrid Mode

Uses both Azure and local storage with automatic fallback:

```env
STORAGE_MODE=hybrid
AZURE_STORAGE_ACCOUNT=youraccount
AZURE_STORAGE_KEY=yourkey
LOCAL_STORAGE_PATH=/data/raw
```

### Caching

#### In-Memory Cache

Default option, suitable for single-instance deployments:

```env
CACHE_ENABLED=true
CACHE_SIZE_MB=512
CACHE_TTL_SECONDS=3600
```

#### Redis Cache

For multi-instance deployments:

```env
CACHE_ENABLED=true
REDIS_URL=redis://redis-server:6379
```

## Monitoring and Observability

### Health Checks

- **Liveness**: `GET /health/simple`
- **Readiness**: `GET /health`

### Metrics

Prometheus metrics available at `/metrics`:

- `query_total` - Total queries executed
- `cache_hits_total` - Cache hits
- `cache_hit_rate` - Cache hit rate
- `avg_execution_time_ms` - Average execution time
- `tier_usage_total{tier}` - Usage by storage tier

### Logs

Structured JSON logs include:

- Query execution details
- Cache hit/miss information
- Storage backend health
- Performance metrics

### Dashboards

Grafana dashboards for monitoring:

- Query performance and trends
- Cache effectiveness
- Storage backend health
- API response times

## Performance Tuning

### Memory Configuration

| Deployment Size | Recommended Memory | Cache Size |
|-----------------|-------------------|------------|
| Small (< 100 QPM) | 1GB | 256MB |
| Medium (< 1000 QPM) | 2GB | 512MB |
| Large (< 10000 QPM) | 4GB | 1GB |

### CPU Configuration

- **Development**: 0.5 CPU
- **Production**: 1-2 CPU
- **High Load**: 2-4 CPU with auto-scaling

### Storage Optimization

#### Local Storage

- Use SSD storage for better I/O performance
- Mount data volumes with appropriate permissions
- Consider read-only mounts for data directories

#### Azure Storage

- Use storage accounts in the same region as compute
- Configure appropriate connection timeouts
- Enable Azure CDN for frequently accessed data

### Query Performance

#### Caching Strategy

```env
# Large cache for analytical workloads
CACHE_SIZE_MB=2048
CACHE_TTL_SECONDS=7200

# Smaller cache for real-time workloads
CACHE_SIZE_MB=256
CACHE_TTL_SECONDS=300
```

#### Aggregation Settings

```env
# Enable smart aggregation for large datasets
ENABLE_SMART_AGGREGATION=true

# Adjust tier thresholds based on use case
RAW_TIER_MAX_HOURS=24      # Raw data for recent queries
AGGREGATED_TIER_MAX_HOURS=168  # 1-minute aggregates for weekly queries
DAILY_TIER_THRESHOLD_HOURS=168 # Hourly summaries for historical data
```

## Security

### Network Security

- Use HTTPS in production
- Configure appropriate firewall rules
- Implement VPC/subnet isolation

### Access Control

- Implement authentication middleware
- Use API keys or JWT tokens
- Configure CORS for web applications

### Data Security

- Encrypt data in transit and at rest
- Use Azure Storage encryption
- Implement audit logging

## Troubleshooting

### Common Issues

#### Service Won't Start

1. Check configuration:
```bash
kubectl logs deployment/query-service -n sensor-query
```

2. Verify storage access:
```bash
curl http://localhost:8080/health
```

#### High Memory Usage

1. Reduce cache size:
```env
CACHE_SIZE_MB=256
```

2. Check for memory leaks:
```bash
kubectl top pods -n sensor-query
```

#### Slow Query Performance

1. Check tier selection:
```bash
curl http://localhost:8080/api/v1/stats
```

2. Monitor cache hit rate:
```bash
curl http://localhost:8080/metrics | grep cache_hit_rate
```

#### Storage Backend Issues

1. Test Azure connectivity:
```bash
# Check Azure storage health
curl http://localhost:8080/health
```

2. Verify local storage:
```bash
# Check mount points and permissions
kubectl exec -it deployment/query-service -n sensor-query -- ls -la /data
```

### Debug Mode

Enable debug logging:

```env
LOG_LEVEL=DEBUG
API_DEBUG=true
```

### Performance Profiling

Access built-in profiling:

```bash
# Query statistics
curl http://localhost:8080/api/v1/stats

# Cache statistics  
curl http://localhost:8080/health | jq .cache_status

# Storage backend health
curl http://localhost:8080/health | jq .storage_backends
```
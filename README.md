# Sensor Data Query Service

A high-performance microservice for querying sensor data with smart aggregation, multi-tier storage optimization, and intelligent caching.

## Features

- 🚀 **High Performance**: Multi-tier query optimization with automatic tier selection
- 📊 **Smart Aggregation**: 4-step aggregation pipeline (raw → pre-aggregated → daily → cached)
- ☁️ **Dual Storage**: Supports both Azure Blob Storage and local file systems
- ⚡ **Fast Queries**: DuckDB-powered analytics with intelligent caching
- 🎯 **Flexible API**: Time range, sensor selection, interval control, and data point limiting
- 📈 **Auto-Optimization**: Automatically selects optimal data tier based on query parameters
- 🔄 **Real-time**: Supports both real-time and historical data queries
- 🐳 **Production Ready**: Docker, Kubernetes, monitoring, and observability

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Query API     │───▶│  Smart Engine    │───▶│  Storage Tiers  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │                         │
                              ▼                         │
                       ┌──────────────────┐            │
                       │  Cache Layer     │◄───────────┘
                       └──────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    4-Tier Storage System                        │
│  Raw Data    │ Pre-Aggregated │ Daily Summary │ Smart Cache     │
│  (1-sec)     │ (1-min avg)    │ (hourly avg)  │ (in-memory)     │
│  Full Detail │ 4-7x faster    │ 50-100x faster│ Instant         │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Development
```bash
git clone <repo-url>
cd sensor-data-query-service
cp .env.example .env
# Edit .env with your configuration
make dev
```

### Production
```bash
make docker-build
docker-compose up -d
```

### API Usage
```bash
# Basic query
curl "http://localhost:8080/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-01T01:00:00Z&sensors=quad_ch1,quad_ch2"

# With interval and limits
curl "http://localhost:8080/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&sensors=quad_ch1&intervalMs=60000&maxDatapoints=1000"

# Get available sensors
curl "http://localhost:8080/api/v1/sensors"

# Health check
curl "http://localhost:8080/health"
```

## API Endpoints

### Query Endpoints
- `GET /api/v1/query` - Query sensor data with smart optimization
- `GET /api/v1/sensors` - List available sensors and assets
- `GET /api/v1/assets` - List available assets
- `GET /api/v1/timerange` - Get available time range for sensors

### Management Endpoints
- `POST /api/v1/cache/clear` - Clear query cache
- `POST /api/v1/aggregation/rebuild` - Rebuild aggregation tiers
- `GET /api/v1/stats` - Get query performance statistics

### System Endpoints
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

## Query Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `start` | ISO DateTime | Start time (inclusive) | `2024-01-01T00:00:00Z` |
| `end` | ISO DateTime | End time (exclusive) | `2024-01-01T01:00:00Z` |
| `sensors` | Comma-separated | Sensor list | `quad_ch1,quad_ch2,quad_ch3` |
| `assets` | Comma-separated | Asset filter (optional) | `asset_001,asset_002` |
| `intervalMs` | Integer | Interval between points (ms) | `60000` (1 minute) |
| `maxDatapoints` | Integer | Maximum data points | `1000` |
| `aggregation` | String | Aggregation method | `avg,min,max,last` |

## Smart Query Optimization

The service automatically selects the optimal data tier:

1. **Raw Data** (1-second): For queries < 1 hour with high precision
2. **Pre-Aggregated** (1-minute): For queries < 1 day with medium precision
3. **Daily Summary** (1-hour): For queries > 1 day with hourly precision
4. **Smart Cache**: For repeated queries and popular time ranges

## Performance Features

- **Intelligent Caching**: LRU cache with configurable TTL
- **Parallel Processing**: Multi-threaded data loading and aggregation
- **Efficient Storage**: Columnar Parquet with optimized compression
- **Query Planning**: Cost-based optimizer for tier selection
- **Connection Pooling**: Reused connections for Azure/local storage

## Configuration

Key environment variables:

```env
# Storage
AZURE_STORAGE_ACCOUNT=your_account
AZURE_STORAGE_KEY=your_key
AZURE_CONTAINER_NAME=sensor-data-cold-storage
LOCAL_STORAGE_PATH=/data/raw
STORAGE_MODE=hybrid  # azure, local, hybrid

# Query Performance
CACHE_SIZE_MB=512
MAX_QUERY_DURATION_HOURS=168  # 7 days
DEFAULT_MAX_DATAPOINTS=10000
ENABLE_SMART_AGGREGATION=true

# API
API_HOST=0.0.0.0
API_PORT=8080
API_WORKERS=4
```

## Development

```bash
# Install dependencies
make install

# Run locally
make run

# Run tests
make test

# Format code
make format

# Build Docker image
make docker-build
```

## Deployment

- **Docker Compose**: Single-node deployment with dependencies
- **Kubernetes**: Production-ready manifests with auto-scaling
- **Azure Container Instances**: Serverless deployment option
- **AWS ECS**: Container deployment on AWS

See [deployment/README.md](deployment/README.md) for detailed instructions.

## Monitoring

- **Health Checks**: Component-level health monitoring
- **Metrics**: Query performance, cache hit rates, storage statistics
- **Logging**: Structured JSON logging with query tracing
- **Alerts**: Configurable alerts for performance and errors# data-query-service

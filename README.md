# Sensor Data Query Service

Clean, high-performance API service for querying time-series sensor data with intelligent multi-tier storage optimization and caching.

## Features

- 🚀 **High Performance**: Multi-tier query optimization with automatic tier selection
- 📊 **Smart Aggregation**: 4-tier aggregation pipeline (raw → aggregated → hourly → daily)
- ☁️ **Dual Storage**: Supports both Azure Blob Storage and local file systems
- ⚡ **Fast Queries**: Intelligent caching with tier-based optimization
- 🎯 **Clean API**: Simple, single-sensor endpoints for raw and aggregated data
- 📈 **Auto-Optimization**: Automatically selects optimal data tier based on query duration
- 🔄 **Real-time**: Supports both real-time and historical data queries
- 🐳 **Production Ready**: Docker, monitoring, and observability

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Clean APIs    │───▶│  Smart Engine    │───▶│  Storage Tiers  │
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
│  Raw Data    │ Aggregated   │ Hourly       │ Daily Summary      │
│  (1-sec)     │ (1-min avg)  │ (1-hour avg) │ (daily summaries)  │
│  < 2 hours   │ < 24 hours   │ < 7 days     │ >= 7 days          │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Development
```bash
git clone <repo-url>
cd data-query-service
cp .env.example .env
# Edit .env with your configuration
make dev
```

### Production
```bash
make docker-build
docker-compose up -d
```

## API Usage

### Raw Data API
```bash
# Get raw sensor data (1-second precision)
curl -X POST "http://localhost:8080/api/v2/raw" \
  -H "Content-Type: application/json" \
  -d '{
    "device": "0330372d-cfe9-4b44-bf3c-9906576b9fc2",
    "sensor": "de_inboard_seal_face_temperature_degree_c",
    "start_time": "2022-03-21T00:00:00.000Z",
    "end_time": "2022-03-21T01:00:00.000Z"
  }'
```

### Aggregated Data API
```bash
# Get aggregated sensor data with specified interval
curl -X POST "http://localhost:8080/api/v2/aggregated" \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2022-03-21T00:00:00.000Z",
    "end_time": "2022-03-22T00:00:00.000Z",
    "sensor": "de_inboard_seal_face_temperature_degree_c",
    "device": "0330372d-cfe9-4b44-bf3c-9906576b9fc2",
    "interval_ms": 240000,
    "max_data_points": 1000,
    "aggregation_method": "mean"
  }'
```

### Discovery APIs
```bash
# List available devices
curl "http://localhost:8080/api/v2/devices"

# List available sensors
curl "http://localhost:8080/api/v2/sensors"

# API information
curl "http://localhost:8080/api/v2/info"

# Health check
curl "http://localhost:8080/api/v2/health"
```

## API Endpoints

### Data Endpoints
- `POST /api/v2/raw` - Raw sensor data with 1-second precision
- `POST /api/v2/aggregated` - Aggregated sensor data with specified intervals

### Discovery Endpoints
- `GET /api/v2/devices` - List available devices (with filtering)
- `GET /api/v2/sensors` - List available sensors (with filtering)

### System Endpoints
- `GET /api/v2/health` - Detailed health check
- `GET /api/v2/info` - API information and capabilities
- `GET /health` - Simple health check

## Request/Response Format

### Raw API Request
```json
{
  "device": "device_id_or_asset_id",
  "sensor": "sensor_name", 
  "start_time": "2022-03-21T00:00:00.000Z",
  "end_time": "2022-03-21T01:00:00.000Z"
}
```

### Raw API Response
```json
{
  "data": [
    [1647820800000, 85.5],
    [1647820801000, 85.7],
    [1647820802000, 85.9]
  ],
  "count": 3,
  "device": "device_id_or_asset_id",
  "sensor": "sensor_name",
  "execution_time_ms": 45.2
}
```

### Aggregated API Request
```json
{
  "start_time": "2022-03-21T00:00:00.000Z",
  "end_time": "2022-03-22T00:00:00.000Z",
  "sensor": "sensor_name",
  "device": "device_id_or_asset_id",
  "interval_ms": 240000,
  "max_data_points": 1000,
  "aggregation_method": "mean"
}
```

### Aggregated API Response
```json
{
  "data": [
    [1647820800000, 85.5],
    [1647821040000, 85.7]
  ],
  "count": 2,
  "device": "device_id_or_asset_id",
  "sensor": "sensor_name",
  "interval_ms": 240000,
  "aggregation_method": "mean",
  "truncated": false,
  "truncated_end_time": null,
  "execution_time_ms": 127.3
}
```

## Tier Selection Logic

The service automatically selects the optimal data tier based on query duration:

- **Duration < 2h** → Raw tier (1-second precision)
- **Duration < 24h** → Aggregated tier (1-minute averages)
- **Duration < 7d** → Hourly tier (1-hour averages)
- **Duration ≥ 7d** → Daily tier (daily summaries)

## Truncation Logic (Aggregated API)

When data points exceed `max_data_points`:
1. **Maintains exact `interval_ms`** - Never changes the requested interval
2. **Truncates to limit** - Keeps first N points chronologically
3. **Provides `truncated_end_time`** - Shows exact timestamp where data was cut off
4. **Sets `truncated: true`** - Indicates data was limited

## Configuration

Key environment variables:

```env
# Storage
STORAGE_MODE=hybrid  # azure, local, hybrid

# Azure Storage - SAS Token Only (see AZURE_CONFIGURATION.md for detailed setup)
AZURE_BLOB_ENDPOINT=https://youraccount.blob.core.windows.net
AZURE_SAS_TOKEN=your-sas-token
AZURE_CONTAINER_NAME=sensor-data-cold-storage
AZURE_DATA_PREFIX=production/sensors/  # Optional directory path

# Local Storage
LOCAL_STORAGE_PATH=/data

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

📚 **For detailed Azure Blob storage configuration including directory paths, authentication methods, and examples, see [AZURE_CONFIGURATION.md](./AZURE_CONFIGURATION.md)**

## Development

```bash
# Install dependencies
make install

# Run locally
make run

# Run tests
python scripts/test_simplified_apis.py

# Format code
make format

# Build Docker image
make docker-build
```

## Supported Devices

The service supports both generation devices:

- **Gen1 Devices**: UUID-based device IDs (e.g., `0330372d-cfe9-4b44-bf3c-9906576b9fc2`)
- **Gen2 Devices**: Custom device IDs (e.g., `287b1b90-f3ad-5ab6-b25e-176a1bca6f8c-NDE`)

Device type is automatically detected based on the ID format.

## Time Format Support

All APIs support flexible time formats:

- **ISO Strings**: `"2022-03-21T00:00:00.000Z"`
- **Epoch Timestamps**: `1647820800000` (milliseconds)
- **Datetime Objects**: In code/SDKs

## Testing

```bash
# Test both APIs
python scripts/test_simplified_apis.py

# Test specific API
python scripts/test_simplified_apis.py --url http://localhost:8080
```

## Documentation

- **Swagger UI**: `/docs` (interactive API documentation)
- **ReDoc**: `/redoc` (alternative API documentation)
- **API Info**: `GET /api/v2/info` (programmatic API information)
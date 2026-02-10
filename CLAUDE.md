# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance sensor data query service built with Python/FastAPI that provides intelligent multi-tier storage optimization and caching for time-series sensor data. The service automatically selects optimal data tiers based on query parameters and supports both Azure Blob Storage and local file systems.

## Development Commands

### Core Commands
- `make install` - Install Python dependencies (including pytest)
- `make dev` - Start development environment with hot reload
- `make run` - Run service locally on port 8080
- `make test` - Run pytest test suite with coverage reporting
- `make lint` - Run flake8 and mypy linting
- `make format` - Format code with black and isort

### Testing and API
- `make test-api` - Run API integration tests using scripts/test-queries.py
- `python scripts/test-queries.py` - Test basic API functionality
- `python scripts/test-specialized-apis.py` - Test specialized API endpoints

### Docker
- `make docker-build` - Build Docker image
- `make docker-run` - Run in Docker container
- `make compose-up` - Start with docker-compose
- `make compose-dev` - Start development stack with monitoring

### Monitoring
- `make health` - Check service health at /health
- `make stats` - Get service statistics
- `make cache-clear` - Clear query cache
- `make list-sensors` - List available sensors

## Architecture

### Core Components

**SmartQueryEngine** (`app/query/engine.py`) - Main query orchestration with automatic tier selection:
- Validates query parameters and determines optimal storage tier
- Manages cache lookup and storage across tiers
- Handles fallback between Azure/local storage backends
- Applies post-processing aggregation and downsampling

**Storage Backends** (`app/storage/`):
- `AzureStorageBackend` - Azure Blob Storage interface with connection pooling
- `LocalStorageBackend` - Local filesystem interface with caching
- `base.py` - Common SensorDataReader interface

**Cache Management** (`app/cache/cache_manager.py`) - LRU cache with TTL:
- In-memory caching for query results
- Configurable size limits and TTL
- Optional Redis backend support

**Configuration** (`app/config.py`) - Environment-based configuration with validation:
- Storage mode selection (azure/local/hybrid)
- Query limits and performance tuning
- Multi-tier storage thresholds

### Multi-Tier Storage System
1. **Raw Data** (1-second precision) - For queries < 24 hours
2. **Pre-Aggregated** (1-minute averages) - For queries < 7 days  
3. **Daily Summary** (hourly averages) - For queries > 7 days
4. **Smart Cache** (in-memory) - For repeated queries

### API Structure
- **Query API** (`app/api/routes.py`) - Core sensor data queries
- **Specialized API** (`app/api/routes_specialized.py`) - Enhanced endpoints with metadata
- **FastAPI Application** (`app/main.py`) - Service initialization and health checks

## Key Configuration

The service uses environment variables for configuration. Key settings:

```bash
# Storage
STORAGE_MODE=hybrid  # azure, local, hybrid
AZURE_STORAGE_ACCOUNT=your_account
LOCAL_STORAGE_PATH=/data/raw

# Performance  
CACHE_SIZE_MB=512
MAX_QUERY_DURATION_HOURS=168
DEFAULT_MAX_DATAPOINTS=10000
ENABLE_SMART_AGGREGATION=true

# API
API_HOST=0.0.0.0
API_PORT=8080
```

## Development Notes

- The service is designed for defensive security analysis and monitoring of sensor data
- Uses pandas/pyarrow for efficient time-series data processing
- Implements intelligent query optimization with automatic tier selection
- All storage backends support parallel data loading with configurable worker threads
- Health checks validate storage backend connectivity and cache status
- Comprehensive statistics tracking for query performance monitoring

## Data Organization

Expected data structure for local storage:
```
/data/raw/
├── asset_001/
│   ├── 2024/01/15/14/
│   │   ├── quad_ch1.parquet
│   │   └── quad_ch2.parquet
```

## Testing

Run the full test suite with `make test`. For API testing, use `make test-api` which runs integration tests against a live service instance.
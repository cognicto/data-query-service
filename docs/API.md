# API Documentation

## Overview

The Sensor Data Query Service provides a RESTful API for querying sensor data with intelligent optimization and caching.

## Base URL

```
http://localhost:8080/api/v1
```

## Authentication

Currently, no authentication is required. In production, implement appropriate authentication mechanisms.

## Query Endpoints

### GET /query

Query sensor data with automatic optimization.

**Parameters:**

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `sensors` | string | Yes | Comma-separated sensor names | `quad_ch1,quad_ch2` |
| `start_time` | ISO datetime | Yes | Start time (inclusive) | `2024-01-01T00:00:00Z` |
| `end_time` | ISO datetime | Yes | End time (exclusive) | `2024-01-01T01:00:00Z` |
| `asset_ids` | string | No | Comma-separated asset IDs | `asset_001,asset_002` |
| `interval_ms` | integer | No | Interval between points (ms) | `60000` |
| `max_datapoints` | integer | No | Maximum data points | `1000` |
| `aggregation` | string | No | Aggregation method | `avg,min,max,last` |

**Example:**

```bash
curl "http://localhost:8080/api/v1/query?sensors=quad_ch1&start_time=2024-01-01T00:00:00Z&end_time=2024-01-01T01:00:00Z&max_datapoints=100"
```

**Response:**

```json
{
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00Z",
      "sensor_name": "quad_ch1",
      "asset_id": "asset_001",
      "temperature": 25.6,
      "humidity": 60.2
    }
  ],
  "metadata": {
    "cache_hit": false,
    "tier_used": "raw",
    "execution_time_ms": 45.2,
    "truncated": false,
    "actual_end_time": "2024-01-01T01:00:00Z"
  },
  "count": 1
}
```

### POST /query

Query sensor data using request body (for complex queries).

**Request Body:**

```json
{
  "sensors": ["quad_ch1", "quad_ch2"],
  "start_time": "2024-01-01T00:00:00Z",
  "end_time": "2024-01-01T01:00:00Z",
  "asset_ids": ["asset_001"],
  "interval_ms": 60000,
  "max_datapoints": 500,
  "aggregation": "avg"
}
```

## Discovery Endpoints

### GET /sensors

List available sensors.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `asset_id` | string | No | Filter by asset ID |

**Response:**

```json
{
  "sensors": [
    {
      "name": "quad_ch1",
      "asset_ids": ["asset_001", "asset_002"],
      "data_count": 10000,
      "first_seen": "2024-01-01T00:00:00Z",
      "last_seen": "2024-01-02T00:00:00Z"
    }
  ],
  "total_count": 1
}
```

### GET /assets

List available assets.

**Response:**

```json
{
  "assets": [
    {
      "id": "asset_001",
      "sensors": ["quad_ch1", "quad_ch2"],
      "data_count": 20000,
      "first_seen": "2024-01-01T00:00:00Z",
      "last_seen": "2024-01-02T00:00:00Z"
    }
  ],
  "total_count": 1
}
```

### GET /timerange

Get available time range for sensors.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sensors` | string | Yes | Comma-separated sensor names |
| `asset_ids` | string | No | Comma-separated asset IDs |

**Response:**

```json
{
  "sensors": ["quad_ch1"],
  "asset_ids": ["asset_001"],
  "min_time": "2024-01-01T00:00:00Z",
  "max_time": "2024-01-02T00:00:00Z",
  "duration_hours": 24.0
}
```

## Management Endpoints

### POST /cache/clear

Clear query cache.

**Response:**

```json
{
  "success": true,
  "message": "Cache cleared successfully",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

### GET /stats

Get service statistics.

**Response:**

```json
{
  "query_stats": {
    "total_queries": 150,
    "cache_hits": 45,
    "cache_hit_rate": 0.3,
    "avg_execution_time_ms": 125.5,
    "tier_usage": {
      "raw": 50,
      "aggregated": 75,
      "daily": 25
    }
  },
  "cache_stats": {
    "hits": 45,
    "misses": 105,
    "hit_rate": 0.3,
    "entries": 12,
    "size_mb": 45.2,
    "enabled": true
  },
  "uptime_seconds": 3600.5
}
```

## Health Endpoints

### GET /health

Comprehensive health check.

**Response:**

```json
{
  "overall_healthy": true,
  "storage_backends": {
    "azure": {
      "status": {
        "healthy": true,
        "issues": []
      },
      "details": {
        "container_exists": true,
        "sample_files_accessible": true
      }
    }
  },
  "cache_status": {
    "enabled": true,
    "entries": 12,
    "size_mb": 45.2
  },
  "query_stats": { ... },
  "timestamp": "2024-01-01T12:00:00Z"
}
```

### GET /health/simple

Simple health check.

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

## Error Responses

All endpoints return consistent error responses:

```json
{
  "error": "Invalid time range",
  "detail": "end_time must be after start_time",
  "error_code": "INVALID_TIME_RANGE",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

**HTTP Status Codes:**

- `200` - Success
- `400` - Bad Request (invalid parameters)
- `404` - Not Found
- `500` - Internal Server Error
- `503` - Service Unavailable

## Query Optimization

The service automatically optimizes queries using:

1. **Tier Selection**: Automatically chooses raw, aggregated, or daily data
2. **Caching**: Intelligent caching with configurable TTL
3. **Aggregation**: Smart downsampling for large datasets
4. **Parallel Processing**: Multi-threaded data loading

### Tier Selection Logic

| Query Duration | Tier Used | Precision |
|----------------|-----------|-----------|
| < 24 hours | Raw | 1 second |
| 24 hours - 7 days | Aggregated | 1 minute |
| > 7 days | Daily | 1 hour |

## Rate Limiting

Default rate limit: 100 requests per minute per IP.

## Pagination

Large result sets are automatically truncated based on `max_datapoints`. Check the `truncated` field in metadata.

## Data Formats

- **Timestamps**: ISO 8601 format with UTC timezone (`2024-01-01T00:00:00Z`)
- **Numbers**: JSON numbers (integers and floats)
- **Strings**: UTF-8 encoded
- **Nulls**: JSON null for missing values
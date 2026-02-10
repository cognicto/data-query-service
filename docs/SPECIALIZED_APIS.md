# Specialized APIs Documentation

## Overview

The Sensor Data Query Service provides two optimized APIs specifically designed for different use cases:

1. **Raw Data API** - Returns original 1-second precision data from TimescaleDB
2. **Aggregated Data API** - Returns aggregated data with smart optimization

## Configuration

The maximum number of data points per query is configurable via:

```env
MAX_ABSOLUTE_DATAPOINTS=100000  # Default: 100,000 points
```

## 📊 Raw Data API

### Purpose
Returns raw sensor data with 1-second interval precision, exactly as stored in the original TimescaleDB and converted to Parquet format.

### GET /api/v1/raw-data

**Parameters:**
| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `start_date` | ISO datetime | Yes | Start date (inclusive) | `2024-01-01T00:00:00Z` |
| `end_date` | ISO datetime | Yes | End date (exclusive) | `2024-01-01T01:00:00Z` |
| `sensor_types` | string | Yes | Comma-separated sensor types | `quad_ch1,quad_ch2` |

**Example Request:**
```bash
curl "http://localhost:8080/api/v1/raw-data?start_date=2024-01-01T00:00:00Z&end_date=2024-01-01T01:00:00Z&sensor_types=quad_ch1,quad_ch2"
```

### POST /api/v1/raw-data

**Request Body:**
```json
{
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-01-01T01:00:00Z",
  "sensor_types": ["quad_ch1", "quad_ch2"]
}
```

**Response:**
```json
{
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00Z",
      "sensor_type": "quad_ch1",
      "asset_id": "asset_001",
      "temperature": 25.6,
      "humidity": 60.2,
      "pressure": 1013.25
    }
  ],
  "metadata": {
    "total_data_points": 3600,
    "truncated": false,
    "actual_end_date": null,
    "max_datapoints_limit": 100000,
    "interval_ms_used": 1000,
    "cache_hit": false,
    "execution_time_ms": 156.7,
    "tier_used": "raw"
  }
}
```

### Raw Data Features

- **1-second precision**: Always returns data at 1-second intervals
- **Original values**: No aggregation applied, preserves all original sensor readings
- **Automatic truncation**: Limits results to `maxDatapoints` and returns `actual_end_date` if truncated
- **Performance optimized**: Uses raw data tier for fastest access to recent data

## 📈 Aggregated Data API

### Purpose
Returns aggregated sensor data with user-specified or auto-calculated intervals, optimized for performance with large time ranges.

### GET /api/v1/aggregated-data

**Parameters:**
| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `start_date` | ISO datetime | Yes | Start date (inclusive) | `2024-01-01T00:00:00Z` |
| `end_date` | ISO datetime | Yes | End date (exclusive) | `2024-01-02T00:00:00Z` |
| `sensor_types` | string | Yes | Comma-separated sensor types | `quad_ch1,quad_ch2` |
| `aggregation_type` | enum | Yes | `min`, `max`, or `mean` | `mean` |
| `interval_ms` | integer | No | Interval in ms (auto-calculated if not provided) | `60000` |

**Example Request:**
```bash
curl "http://localhost:8080/api/v1/aggregated-data?start_date=2024-01-01T00:00:00Z&end_date=2024-01-02T00:00:00Z&sensor_types=quad_ch1,quad_ch2&aggregation_type=mean&interval_ms=60000"
```

### POST /api/v1/aggregated-data

**Request Body:**
```json
{
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-01-02T00:00:00Z",
  "sensor_types": ["quad_ch1", "quad_ch2"],
  "aggregation_type": "mean",
  "interval_ms": 300000
}
```

**Response:**
```json
{
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00Z",
      "sensor_type": "quad_ch1",
      "asset_id": "asset_001",
      "temperature": 25.8,
      "humidity": 62.1,
      "pressure": 1013.4
    }
  ],
  "metadata": {
    "total_data_points": 288,
    "truncated": false,
    "actual_end_date": null,
    "max_datapoints_limit": 100000,
    "interval_ms_used": 300000,
    "cache_hit": true,
    "execution_time_ms": 45.2,
    "tier_used": "aggregated"
  }
}
```

### Aggregation Types

| Type | Description | Use Case |
|------|-------------|----------|
| `min` | Minimum value in interval | Finding lowest readings |
| `max` | Maximum value in interval | Finding peak values |
| `mean` | Average value in interval | General trend analysis |

### Smart Interval Calculation

If `interval_ms` is not provided, the system automatically calculates the optimal interval:

**Calculation Logic:**
```python
# Calculate points per sensor to stay under limit
max_points_per_sensor = max_datapoints / number_of_sensors

# Calculate minimum interval needed
duration_ms = (end_date - start_date).total_seconds() * 1000
min_interval_ms = duration_ms / max_points_per_sensor

# Round up to nearest standard interval
standard_intervals = [1000, 5000, 10000, 30000, 60000, 300000, ...]
optimal_interval = next(interval for interval in standard_intervals if interval >= min_interval_ms)
```

**Standard Intervals:**
- 1 second (1000ms)
- 5 seconds (5000ms)
- 10 seconds (10000ms)
- 30 seconds (30000ms)
- 1 minute (60000ms)
- 5 minutes (300000ms)
- 10 minutes (600000ms)
- 30 minutes (1800000ms)
- 1 hour (3600000ms)
- 2 hours (7200000ms)
- 4 hours (14400000ms)
- 6 hours (21600000ms)
- 12 hours (43200000ms)
- 24 hours (86400000ms)

## 🛠️ Helper Endpoints

### GET /api/v1/interval/recommend

Get recommended interval for optimal performance.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | ISO datetime | Yes | Start date |
| `end_date` | ISO datetime | Yes | End date |
| `sensor_types` | string | Yes | Comma-separated sensor types |
| `target_points` | integer | No | Target number of data points |

**Response:**
```json
{
  "recommended_interval_ms": 60000,
  "estimated_datapoints": 2880,
  "duration_hours": 24.0,
  "max_datapoints_limit": 100000
}
```

### GET /api/v1/estimate

Estimate number of data points for given parameters.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | ISO datetime | Yes | Start date |
| `end_date` | ISO datetime | Yes | End date |
| `sensor_types` | string | Yes | Comma-separated sensor types |
| `interval_ms` | integer | Yes | Interval in milliseconds |

**Response:**
```json
{
  "estimated_datapoints": 7200,
  "sensor_types": ["quad_ch1", "quad_ch2"],
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-01-02T00:00:00Z",
  "interval_ms": 60000,
  "duration_hours": 24.0
}
```

## 📋 Discovery Endpoints

### GET /api/v1/sensors

List available sensors.

**Response:**
```json
{
  "sensors": [
    {
      "name": "quad_ch1",
      "asset_ids": ["asset_001", "asset_002"],
      "data_count": null,
      "first_seen": null,
      "last_seen": null
    }
  ],
  "total_count": 14
}
```

### GET /api/v1/timerange

Get available time range for sensors.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sensor_types` | string | Yes | Comma-separated sensor types |

**Response:**
```json
{
  "sensor_types": ["quad_ch1", "quad_ch2"],
  "min_date": "2024-01-01T00:00:00Z",
  "max_date": "2024-01-15T00:00:00Z",
  "duration_hours": 336.0
}
```

### GET /api/v1/config

Get service configuration.

**Response:**
```json
{
  "max_datapoints": 100000,
  "supported_aggregations": ["min", "max", "mean"],
  "storage_mode": "hybrid",
  "tier_thresholds": {
    "raw_tier_max_hours": 24,
    "aggregated_tier_max_hours": 168,
    "daily_tier_threshold_hours": 168
  }
}
```

## ⚡ Performance Optimization

### Automatic Tier Selection

The system automatically selects the optimal storage tier based on query duration:

| Duration | Tier Used | Performance Gain |
|----------|-----------|------------------|
| < 24 hours | Raw | Baseline (1-second data) |
| 24h - 7 days | Aggregated | 4-7x faster |
| > 7 days | Daily | 50-100x faster |

### Intelligent Caching

- **LRU Cache**: Most recent queries cached in memory
- **Adaptive TTL**: Popular queries cached longer
- **Cache Keys**: Based on exact query parameters
- **Cache Hit Rate**: Typically 30-70% for analytical workloads

### Data Truncation

Both APIs automatically enforce the `maxDatapoints` limit:

**For Raw Data:**
- Calculates expected points: `duration_seconds × number_of_sensors`
- Truncates time range if exceeds limit
- Returns `actual_end_date` in metadata

**For Aggregated Data:**
- Uses smart interval calculation to fit within limit
- Applies additional downsampling if needed
- Preserves data quality while respecting limits

## 🚨 Error Responses

All endpoints return consistent error responses:

```json
{
  "error": "Invalid date range",
  "detail": "end_date must be after start_date",
  "error_code": "INVALID_DATE_RANGE",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

**Common HTTP Status Codes:**
- `200` - Success
- `400` - Bad Request (invalid parameters)
- `422` - Validation Error (missing parameters)
- `500` - Internal Server Error

## 📊 Usage Examples

### Example 1: Real-time Monitoring (Last Hour)
```bash
# Raw data for detailed analysis
curl "http://localhost:8080/api/v1/raw-data?start_date=2024-01-01T11:00:00Z&end_date=2024-01-01T12:00:00Z&sensor_types=quad_ch1"
```

### Example 2: Daily Trend Analysis
```bash
# Aggregated data with 5-minute intervals
curl "http://localhost:8080/api/v1/aggregated-data?start_date=2024-01-01T00:00:00Z&end_date=2024-01-02T00:00:00Z&sensor_types=quad_ch1,quad_ch2&aggregation_type=mean&interval_ms=300000"
```

### Example 3: Weekly Performance Report
```bash
# Auto-calculated intervals for optimal performance
curl -X POST "http://localhost:8080/api/v1/aggregated-data" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-08T00:00:00Z",
    "sensor_types": ["quad_ch1", "quad_ch2", "quad_ch3"],
    "aggregation_type": "mean"
  }'
```

### Example 4: Finding Peak Values
```bash
# Maximum values over 24 hours with 1-hour intervals
curl "http://localhost:8080/api/v1/aggregated-data?start_date=2024-01-01T00:00:00Z&end_date=2024-01-02T00:00:00Z&sensor_types=quad_ch1&aggregation_type=max&interval_ms=3600000"
```

### Example 5: Interval Planning
```bash
# Get recommendation for optimal interval
curl "http://localhost:8080/api/v1/interval/recommend?start_date=2024-01-01T00:00:00Z&end_date=2024-01-08T00:00:00Z&sensor_types=quad_ch1,quad_ch2,quad_ch3"

# Estimate data points for specific interval
curl "http://localhost:8080/api/v1/estimate?start_date=2024-01-01T00:00:00Z&end_date=2024-01-08T00:00:00Z&sensor_types=quad_ch1,quad_ch2&interval_ms=3600000"
```
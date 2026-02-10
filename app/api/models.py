"""
Pydantic models for API request/response schemas.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class AggregationMethod(str, Enum):
    """Supported aggregation methods."""
    min = "min" 
    max = "max"
    mean = "mean"  # Changed from avg to mean for clarity


class RawDataRequest(BaseModel):
    """Request model for raw sensor data queries."""
    start_date: datetime = Field(..., description="Start date (inclusive) in ISO 8601 format")
    end_date: datetime = Field(..., description="End date (exclusive) in ISO 8601 format")
    sensor_types: List[str] = Field(..., description="List of sensor types (e.g., quad_ch1, quad_ch2)", min_items=1)
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class AggregatedDataRequest(BaseModel):
    """Request model for aggregated sensor data queries."""
    start_date: datetime = Field(..., description="Start date (inclusive) in ISO 8601 format")
    end_date: datetime = Field(..., description="End date (exclusive) in ISO 8601 format")
    sensor_types: List[str] = Field(..., description="List of sensor types (e.g., quad_ch1, quad_ch2)", min_items=1)
    interval_ms: Optional[int] = Field(None, description="Interval between data points in milliseconds (auto-calculated if not provided)", gt=0)
    aggregation_type: AggregationMethod = Field(..., description="Aggregation method: min, max, or mean")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class DataPoint(BaseModel):
    """Single sensor data point."""
    timestamp: datetime
    sensor_type: str
    asset_id: Optional[str] = None
    value: Optional[float] = None
    # Additional fields will be included dynamically


class QueryMetadata(BaseModel):
    """Metadata about query execution."""
    total_data_points: int = Field(..., description="Total number of data points in response")
    truncated: bool = Field(..., description="Whether results were truncated due to max_datapoints")
    actual_end_date: Optional[datetime] = Field(None, description="Actual end date if truncated")
    max_datapoints_limit: int = Field(..., description="Maximum datapoints limit applied")
    interval_ms_used: Optional[int] = Field(None, description="Actual interval used (ms)")
    cache_hit: bool = Field(..., description="Whether result was served from cache")
    execution_time_ms: float = Field(..., description="Query execution time in milliseconds")
    tier_used: str = Field(..., description="Storage tier used (raw/aggregated/daily)")


class RawDataResponse(BaseModel):
    """Response model for raw sensor data queries."""
    data: List[Dict[str, Any]] = Field(..., description="Raw sensor data (1-second interval)")
    metadata: QueryMetadata = Field(..., description="Query execution metadata")


class AggregatedDataResponse(BaseModel):
    """Response model for aggregated sensor data queries."""
    data: List[Dict[str, Any]] = Field(..., description="Aggregated sensor data")
    metadata: QueryMetadata = Field(..., description="Query execution metadata")


class SensorInfo(BaseModel):
    """Information about a sensor."""
    name: str = Field(..., description="Sensor name")
    asset_ids: List[str] = Field(..., description="Asset IDs where this sensor is present")
    data_count: Optional[int] = Field(None, description="Approximate number of data points")
    first_seen: Optional[datetime] = Field(None, description="First data timestamp")
    last_seen: Optional[datetime] = Field(None, description="Last data timestamp")


class SensorListResponse(BaseModel):
    """Response model for listing sensors."""
    sensors: List[SensorInfo] = Field(..., description="List of available sensors")
    total_count: int = Field(..., description="Total number of sensors")


class AssetInfo(BaseModel):
    """Information about an asset."""
    id: str = Field(..., description="Asset ID")
    sensors: List[str] = Field(..., description="Sensors available for this asset")
    data_count: Optional[int] = Field(None, description="Approximate number of data points")
    first_seen: Optional[datetime] = Field(None, description="First data timestamp")
    last_seen: Optional[datetime] = Field(None, description="Last data timestamp")


class AssetListResponse(BaseModel):
    """Response model for listing assets."""
    assets: List[AssetInfo] = Field(..., description="List of available assets")
    total_count: int = Field(..., description="Total number of assets")


class TimeRangeResponse(BaseModel):
    """Response model for time range queries."""
    sensor_types: List[str] = Field(..., description="Sensor types queried")
    min_date: Optional[datetime] = Field(None, description="Earliest available data")
    max_date: Optional[datetime] = Field(None, description="Latest available data") 
    duration_hours: Optional[float] = Field(None, description="Total duration in hours")


class CacheStats(BaseModel):
    """Cache statistics."""
    hits: int = Field(..., description="Cache hits")
    misses: int = Field(..., description="Cache misses")
    hit_rate: float = Field(..., description="Cache hit rate (0.0 - 1.0)")
    entries: int = Field(..., description="Number of cached entries")
    size_mb: float = Field(..., description="Cache size in MB")
    enabled: bool = Field(..., description="Whether caching is enabled")


class QueryStats(BaseModel):
    """Query execution statistics."""
    total_queries: int = Field(..., description="Total number of queries executed")
    cache_hits: int = Field(..., description="Number of cache hits")
    cache_hit_rate: float = Field(..., description="Overall cache hit rate")
    avg_execution_time_ms: float = Field(..., description="Average execution time")
    tier_usage: Dict[str, int] = Field(..., description="Usage count by storage tier")
    total_execution_time_ms: float = Field(..., description="Total execution time across all queries")


class StatsResponse(BaseModel):
    """Response model for service statistics."""
    query_stats: QueryStats = Field(..., description="Query execution statistics")
    cache_stats: CacheStats = Field(..., description="Cache statistics")
    uptime_seconds: Optional[float] = Field(None, description="Service uptime in seconds")


class HealthStatus(BaseModel):
    """Health check status."""
    healthy: bool = Field(..., description="Whether component is healthy")
    issues: List[str] = Field(default_factory=list, description="List of issues if unhealthy")


class ComponentHealth(BaseModel):
    """Health status for a component."""
    status: HealthStatus = Field(..., description="Component health status")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional health details")


class HealthResponse(BaseModel):
    """Response model for health checks."""
    overall_healthy: bool = Field(..., description="Overall service health")
    storage_backends: Dict[str, ComponentHealth] = Field(..., description="Storage backend health")
    cache_status: Dict[str, Any] = Field(..., description="Cache status")
    query_stats: QueryStats = Field(..., description="Query statistics")
    timestamp: datetime = Field(..., description="Health check timestamp")


class ConfigResponse(BaseModel):
    """Response model for configuration info."""
    max_datapoints: int = Field(..., description="Maximum data points per query")
    supported_aggregations: List[str] = Field(..., description="Supported aggregation methods")
    storage_mode: str = Field(..., description="Storage mode (azure/local/hybrid)")
    tier_thresholds: Dict[str, int] = Field(..., description="Tier selection thresholds in hours")


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error description")
    error_code: Optional[str] = Field(None, description="Error code")
    timestamp: datetime = Field(..., description="Error timestamp")


class SuccessResponse(BaseModel):
    """Generic success response."""
    success: bool = Field(True, description="Whether operation was successful")
    message: str = Field(..., description="Success message")
    timestamp: datetime = Field(..., description="Operation timestamp")
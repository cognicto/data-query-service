"""
Pydantic models for API request/response schemas.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class AggregationMethod(str, Enum):
    """Supported aggregation methods."""
    avg = "avg"
    min = "min" 
    max = "max"
    last = "last"
    first = "first"
    count = "count"
    sum = "sum"


class QueryRequest(BaseModel):
    """Request model for sensor data queries."""
    sensors: List[str] = Field(..., description="List of sensor names", min_items=1)
    start_time: datetime = Field(..., description="Start time (inclusive) in ISO 8601 format")
    end_time: datetime = Field(..., description="End time (exclusive) in ISO 8601 format")
    asset_ids: Optional[List[str]] = Field(None, description="List of asset IDs to filter by")
    interval_ms: Optional[int] = Field(None, description="Interval between data points in milliseconds", gt=0)
    max_datapoints: Optional[int] = Field(None, description="Maximum number of data points to return", gt=0)
    aggregation: Optional[AggregationMethod] = Field(None, description="Aggregation method")
    
    @validator('end_time')
    def validate_date_range(cls, v, values):
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError('end_time must be after start_time')
        return v


class QueryMetadata(BaseModel):
    """Metadata about query execution."""
    cache_hit: bool = Field(..., description="Whether result was served from cache")
    tier_used: str = Field(..., description="Storage tier used (raw/aggregated/daily)")
    execution_time_ms: float = Field(..., description="Query execution time in milliseconds")
    truncated: bool = Field(..., description="Whether results were truncated due to max_datapoints")
    actual_end_time: Optional[datetime] = Field(None, description="Actual end time if truncated")
    original_datapoints: Optional[int] = Field(None, description="Original number of datapoints before truncation")


class QueryResponse(BaseModel):
    """Response model for sensor data queries."""
    data: List[Dict[str, Any]] = Field(..., description="Sensor data results")
    metadata: QueryMetadata = Field(..., description="Query execution metadata")
    count: int = Field(..., description="Number of data points returned")



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
    sensors: List[str] = Field(..., description="Sensors queried")
    asset_ids: Optional[List[str]] = Field(None, description="Asset IDs queried")
    min_time: Optional[datetime] = Field(None, description="Earliest available data")
    max_time: Optional[datetime] = Field(None, description="Latest available data") 
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
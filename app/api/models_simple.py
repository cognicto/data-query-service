"""
Simplified API models for sensor data service.
"""

from typing import List, Optional, Union, Any, Dict
from datetime import datetime
from pydantic import BaseModel, Field, validator


class TimeRange(BaseModel):
    """Time range specification."""
    start: Union[str, datetime, int] = Field(..., description="Start time")
    end: Union[str, datetime, int] = Field(..., description="End time")
    
    def to_datetime_range(self) -> tuple[datetime, datetime]:
        """Convert to datetime objects."""
        def convert_time(time_val):
            if isinstance(time_val, datetime):
                return time_val
            elif isinstance(time_val, str):
                return datetime.fromisoformat(time_val.replace('Z', '+00:00'))
            elif isinstance(time_val, int):
                return datetime.fromtimestamp(time_val / 1000)
            else:
                raise ValueError(f"Invalid time format: {time_val}")
        
        return convert_time(self.start), convert_time(self.end)


class RawDataRequest(BaseModel):
    """Raw sensor data request."""
    device: str = Field(..., description="Device/asset identifier")
    sensor: str = Field(..., description="Sensor name")
    time_range: TimeRange = Field(..., description="Query time range")
    
    def get_datetime_range(self) -> tuple[datetime, datetime]:
        """Get datetime range."""
        return self.time_range.to_datetime_range()


class AggregatedDataRequest(BaseModel):
    """Aggregated sensor data request."""
    device: str = Field(..., description="Device/asset identifier")
    sensor: str = Field(..., description="Sensor name") 
    time_range: TimeRange = Field(..., description="Query time range")
    interval_ms: int = Field(..., description="Aggregation interval in milliseconds", ge=1000)
    aggregation_method: str = Field("avg", description="Aggregation method")
    max_data_points: int = Field(10000, description="Maximum data points to return", ge=1, le=100000)
    
    @validator('aggregation_method')
    def validate_aggregation_method(cls, v):
        allowed_methods = ['avg', 'min', 'max', 'sum', 'count']
        if v.lower() not in allowed_methods:
            raise ValueError(f"Aggregation method must be one of: {allowed_methods}")
        return v.lower()
    
    def get_datetime_range(self) -> tuple[datetime, datetime]:
        """Get datetime range."""
        return self.time_range.to_datetime_range()


class RawDataResponse(BaseModel):
    """Raw data response."""
    data: List[List[Union[int, float, None]]] = Field(..., description="Array of [timestamp_ms, value] pairs")
    count: int = Field(..., description="Number of data points")
    device: str = Field(..., description="Device identifier")
    sensor: str = Field(..., description="Sensor name")
    execution_time_ms: float = Field(..., description="Query execution time")


class AggregatedDataResponse(BaseModel):
    """Aggregated data response."""
    data: List[List[Union[int, float, None]]] = Field(..., description="Array of [timestamp_ms, value] pairs")
    count: int = Field(..., description="Number of data points")
    device: str = Field(..., description="Device identifier")
    sensor: str = Field(..., description="Sensor name")
    interval_ms: int = Field(..., description="Aggregation interval")
    aggregation_method: str = Field(..., description="Aggregation method used")
    truncated: bool = Field(..., description="Whether data was truncated")
    truncated_end_time: Optional[int] = Field(None, description="End timestamp if truncated")
    execution_time_ms: float = Field(..., description="Query execution time")


class AssetListResponse(BaseModel):
    """Asset list response."""
    assets: List[str] = Field(..., description="Available asset IDs")
    count: int = Field(..., description="Number of assets")


class SensorListResponse(BaseModel):
    """Sensor list response.""" 
    sensors: List[str] = Field(..., description="Available sensor names")
    count: int = Field(..., description="Number of sensors")
    asset_filter: Optional[str] = Field(None, description="Asset filter applied")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Overall health status")
    storage_mode: str = Field(..., description="Storage backend mode")
    engine_healthy: bool = Field(..., description="Query engine health")
    duckdb_version: Optional[str] = Field(None, description="DuckDB version")
    statistics: Dict[str, Any] = Field(..., description="Performance statistics")


class ErrorResponse(BaseModel):
    """Error response."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
"""
Unified API models inspired by Flatpak patterns.
Supports both Gen1 and Gen2 devices through a clean, modern interface.
"""

from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
import re


class DeviceTarget(BaseModel):
    """Unified device and sensor target specification."""
    device: str = Field(..., description="Device identifier (UUID for Gen1, custom ID for Gen2)")
    sensor: str = Field(..., description="Sensor name/identifier")
    location: Optional[str] = Field(None, description="Location path (e.g., 'Alng::Trinidad::C-21521')")
    asset: Optional[str] = Field(None, description="Asset identifier")
    
    @validator('device')
    def validate_device(cls, v):
        if not v or not v.strip():
            raise ValueError('device cannot be empty')
        return v.strip()
    
    @validator('sensor')
    def validate_sensor(cls, v):
        if not v or not v.strip():
            raise ValueError('sensor cannot be empty')
        return v.strip()


class TimeRange(BaseModel):
    """Flexible time range specification."""
    start: Union[str, datetime, int] = Field(..., description="Start time (ISO string, datetime, or epoch ms)")
    end: Union[str, datetime, int] = Field(..., description="End time (ISO string, datetime, or epoch ms)")
    
    def to_datetime_range(self) -> tuple[datetime, datetime]:
        """Convert to datetime objects."""
        def convert_time(time_val):
            if isinstance(time_val, datetime):
                return time_val
            elif isinstance(time_val, str):
                # Handle ISO format
                if time_val.endswith('Z'):
                    time_val = time_val[:-1] + '+00:00'
                return datetime.fromisoformat(time_val.replace('Z', '+00:00'))
            elif isinstance(time_val, int):
                # Handle epoch milliseconds
                return datetime.fromtimestamp(time_val / 1000)
            else:
                raise ValueError(f"Unsupported time format: {type(time_val)}")
        
        start_dt = convert_time(self.start)
        end_dt = convert_time(self.end)
        
        if start_dt >= end_dt:
            raise ValueError("start time must be before end time")
        
        return start_dt, end_dt


class RawDataRequest(BaseModel):
    """Simple raw data request with basic parameters."""
    device: str = Field(..., description="Asset ID or Device ID")
    sensor: str = Field(..., description="Sensor name")
    start_time: Union[str, datetime, int] = Field(..., description="Start time (ISO string, datetime, or epoch ms)")
    end_time: Union[str, datetime, int] = Field(..., description="End time (ISO string, datetime, or epoch ms)")
    
    def get_datetime_range(self) -> tuple[datetime, datetime]:
        """Convert to datetime objects."""
        def convert_time(time_val):
            if isinstance(time_val, datetime):
                return time_val
            elif isinstance(time_val, str):
                # Handle ISO format
                if time_val.endswith('Z'):
                    time_val = time_val[:-1] + '+00:00'
                return datetime.fromisoformat(time_val.replace('Z', '+00:00'))
            elif isinstance(time_val, int):
                # Handle epoch milliseconds
                return datetime.fromtimestamp(time_val / 1000)
            else:
                raise ValueError(f"Unsupported time format: {type(time_val)}")
        
        start_dt = convert_time(self.start_time)
        end_dt = convert_time(self.end_time)
        
        if start_dt >= end_dt:
            raise ValueError("start_time must be before end_time")
        
        return start_dt, end_dt
    
    class Config:
        schema_extra = {
            "examples": [
                {
                    "device": "0330372d-cfe9-4b44-bf3c-9906576b9fc2",
                    "sensor": "de_inboard_seal_face_temperature_degree_c",
                    "start_time": "2022-03-21T00:00:00.000Z",
                    "end_time": "2022-03-21T01:00:00.000Z"
                },
                {
                    "device": "287b1b90-f3ad-5ab6-b25e-176a1bca6f8c-NDE",
                    "sensor": "inboard_temperature_celsius",
                    "start_time": 1647820800000,
                    "end_time": 1647824400000
                }
            ]
        }


class AggregatedDataRequest(BaseModel):
    """Clean aggregated data request for single sensor."""
    start_time: Union[str, datetime, int] = Field(..., description="Start time (ISO string, datetime, or epoch ms)")
    end_time: Union[str, datetime, int] = Field(..., description="End time (ISO string, datetime, or epoch ms)")
    sensor: str = Field(..., description="Sensor name")
    device: str = Field(..., description="Device ID or Asset ID")
    interval_ms: int = Field(..., description="Aggregation interval in milliseconds", gt=0)
    max_data_points: int = Field(..., description="Maximum data points to return", gt=0, le=100000)
    aggregation_method: Optional[str] = Field("mean", description="Aggregation method: mean, min, max, last, first, count, sum")
    
    @validator('aggregation_method')
    def validate_aggregation_method(cls, v):
        valid_methods = ['mean', 'min', 'max', 'last', 'first', 'count', 'sum']
        if v and v not in valid_methods:
            raise ValueError(f'aggregation_method must be one of: {valid_methods}')
        return v
    
    def get_datetime_range(self) -> tuple[datetime, datetime]:
        """Convert to datetime objects."""
        def convert_time(time_val):
            if isinstance(time_val, datetime):
                return time_val
            elif isinstance(time_val, str):
                # Handle ISO format
                if time_val.endswith('Z'):
                    time_val = time_val[:-1] + '+00:00'
                return datetime.fromisoformat(time_val.replace('Z', '+00:00'))
            elif isinstance(time_val, int):
                # Handle epoch milliseconds
                return datetime.fromtimestamp(time_val / 1000)
            else:
                raise ValueError(f"Unsupported time format: {type(time_val)}")
        
        start_dt = convert_time(self.start_time)
        end_dt = convert_time(self.end_time)
        
        if start_dt >= end_dt:
            raise ValueError("start_time must be before end_time")
        
        return start_dt, end_dt
    
    class Config:
        schema_extra = {
            "examples": [
                {
                    "start_time": "2022-03-21T00:00:00.000Z",
                    "end_time": "2022-03-22T00:00:00.000Z",
                    "sensor": "de_inboard_seal_face_temperature_degree_c",
                    "device": "0330372d-cfe9-4b44-bf3c-9906576b9fc2",
                    "interval_ms": 240000,
                    "max_data_points": 1000,
                    "aggregation_method": "mean"
                },
                {
                    "start_time": 1647820800000,
                    "end_time": 1647907200000,
                    "sensor": "inboard_temperature_celsius",
                    "device": "287b1b90-f3ad-5ab6-b25e-176a1bca6f8c-NDE",
                    "interval_ms": 60000,
                    "max_data_points": 500,
                    "aggregation_method": "max"
                }
            ]
        }


class RawDataResponse(BaseModel):
    """Simple response for raw data with timestamped values."""
    data: List[List[Union[float, int, None]]] = Field(..., description="Array of [timestamp_ms, value] pairs")
    count: int = Field(..., description="Number of data points")
    device: str = Field(..., description="Device identifier")
    sensor: str = Field(..., description="Sensor name")
    execution_time_ms: float = Field(..., description="Query execution time")
    
    class Config:
        schema_extra = {
            "example": {
                "data": [
                    [1647820800000, 85.5],
                    [1647820801000, 85.7],
                    [1647820802000, 85.9],
                    [1647820803000, None]
                ],
                "count": 4,
                "device": "0330372d-cfe9-4b44-bf3c-9906576b9fc2",
                "sensor": "de_inboard_seal_face_temperature_degree_c", 
                "execution_time_ms": 45.2
            }
        }


class AggregatedDataResponse(BaseModel):
    """Response for aggregated data with truncation support (single sensor)."""
    data: List[List[Union[float, int, None]]] = Field(..., description="Array of [timestamp_ms, value] pairs")
    count: int = Field(..., description="Number of data points")
    device: str = Field(..., description="Device identifier")
    sensor: str = Field(..., description="Sensor name")
    interval_ms: int = Field(..., description="Aggregation interval used")
    aggregation_method: str = Field(..., description="Aggregation method used")
    truncated: bool = Field(..., description="Whether results were truncated due to max_data_points limit")
    truncated_end_time: Optional[int] = Field(None, description="End timestamp (ms) where data was truncated, if truncated=true")
    execution_time_ms: float = Field(..., description="Query execution time")
    
    class Config:
        schema_extra = {
            "example": {
                "data": [
                    [1647820800000, 85.5],
                    [1647821040000, 85.7],
                    [1647821280000, 85.9],
                    [1647821520000, 86.1]
                ],
                "count": 4,
                "device": "0330372d-cfe9-4b44-bf3c-9906576b9fc2",
                "sensor": "de_inboard_seal_face_temperature_degree_c",
                "interval_ms": 240000,
                "aggregation_method": "mean",
                "truncated": True,
                "truncated_end_time": 1647821520000,
                "execution_time_ms": 127.3
            }
        }


class DeviceInfo(BaseModel):
    """Information about a device."""
    device_id: str = Field(..., description="Device identifier")
    device_type: str = Field(..., description="Device type/generation (gen1/gen2)")
    location: Optional[str] = Field(None, description="Device location")
    available_sensors: List[str] = Field(..., description="Available sensors for this device")
    first_seen: Optional[datetime] = Field(None, description="First data timestamp")
    last_seen: Optional[datetime] = Field(None, description="Last data timestamp")
    data_count: Optional[int] = Field(None, description="Approximate data points available")


class DeviceListResponse(BaseModel):
    """Response for device discovery."""
    devices: List[DeviceInfo] = Field(..., description="Available devices")
    total_count: int = Field(..., description="Total number of devices")
    filters_applied: Optional[Dict[str, Any]] = Field(None, description="Any filters that were applied")


class SensorInfo(BaseModel):
    """Information about a sensor across devices."""
    sensor_name: str = Field(..., description="Sensor identifier")
    available_devices: List[str] = Field(..., description="Devices that have this sensor")
    data_range: Optional[Dict[str, datetime]] = Field(None, description="Time range of available data")
    typical_units: Optional[str] = Field(None, description="Typical units for this sensor")


class SensorListResponse(BaseModel):
    """Response for sensor discovery."""
    sensors: List[SensorInfo] = Field(..., description="Available sensors")
    total_count: int = Field(..., description="Total number of unique sensors")
    device_filter: Optional[str] = Field(None, description="Device filter applied")


class DeviceDetector:
    """Utility class for device type detection and parsing."""
    
    @staticmethod
    def detect_device_type(device_id: str) -> str:
        """Detect device generation based on identifier format."""
        # Gen1: Standard UUID format
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if re.match(uuid_pattern, device_id, re.IGNORECASE):
            return "gen1"
        
        # Gen2: Custom format, often with suffixes
        if '-' in device_id and not re.match(uuid_pattern, device_id, re.IGNORECASE):
            return "gen2"
        
        # Default fallback
        return "unknown"
    
    @staticmethod
    def parse_target(target: DeviceTarget) -> Dict[str, str]:
        """Parse target into internal query parameters."""
        device_type = DeviceDetector.detect_device_type(target.device)
        
        # Determine asset_id mapping
        if device_type == "gen1":
            # For Gen1, use device UUID as asset_id
            asset_id = target.device
        elif device_type == "gen2":
            # For Gen2, use device identifier as asset_id
            asset_id = target.device
        else:
            # Fallback
            asset_id = target.asset or target.device
        
        # Normalize sensor name
        sensor_name = target.sensor.replace('-', '_').replace(' ', '_')
        
        return {
            'device_type': device_type,
            'asset_id': asset_id,
            'sensor_name': sensor_name,
            'location': target.location,
            'original_device': target.device,
            'original_sensor': target.sensor
        }
    
    @staticmethod
    def convert_aggregation_method(method: str) -> str:
        """Convert user aggregation method to internal method."""
        mapping = {
            'mean': 'avg',
            'average': 'avg',
            'avg': 'avg',
            'min': 'min',
            'max': 'max',
            'last': 'last',
            'first': 'first',
            'count': 'count',
            'sum': 'sum'
        }
        return mapping.get(method.lower(), 'avg')
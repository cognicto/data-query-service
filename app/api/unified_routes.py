"""
Unified API routes inspired by Flatpak patterns.
Provides clean, modern APIs that support both Gen1 and Gen2 devices natively.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import time
import pandas as pd

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse

from app.query.engine import SmartQueryEngine
from app.api.unified_models import (
    RawDataRequest, AggregatedDataRequest, RawDataResponse, AggregatedDataResponse,
    DeviceTarget, TimeRange, DeviceListResponse, SensorListResponse, 
    DeviceInfo, SensorInfo, DeviceDetector
)

logger = logging.getLogger(__name__)

# Create unified router
unified_router = APIRouter(prefix="/api/v2", tags=["Unified Device APIs"])

# Global query engine instance - set by app initialization
_query_engine: Optional[SmartQueryEngine] = None

def set_query_engine(engine: SmartQueryEngine):
    """Set the global query engine instance."""
    global _query_engine
    _query_engine = engine

def get_query_engine() -> SmartQueryEngine:
    """Dependency to get the query engine instance."""
    if _query_engine is None:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    return _query_engine


def convert_dataframe_to_raw_response(df: pd.DataFrame, device: str, sensor: str, execution_time: float) -> RawDataResponse:
    """Convert DataFrame result to simple raw data response format."""
    data = []
    
    if not df.empty and 'timestamp' in df.columns:
        # Find value columns (exclude metadata columns)
        value_columns = [col for col in df.columns 
                        if col not in ['timestamp', 'sensor_name', 'asset_id', 'device_id']]
        
        # Use the first numeric column as the primary value
        value_col = None
        for col in value_columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                value_col = col
                break
        
        if value_col:
            for _, row in df.iterrows():
                try:
                    timestamp = pd.to_datetime(row['timestamp'])
                    timestamp_ms = int(timestamp.timestamp() * 1000)
                    value = row[value_col] if pd.notna(row[value_col]) else None
                    
                    data.append([timestamp_ms, value])
                except Exception as e:
                    logger.warning(f"Failed to convert data point: {e}")
                    continue
    
    return RawDataResponse(
        data=data,
        count=len(data),
        device=device,
        sensor=sensor,
        execution_time_ms=execution_time
    )


def convert_dataframe_to_aggregated_response(
    df: pd.DataFrame, 
    device: str, 
    sensor: str, 
    interval_ms: int,
    aggregation_method: str,
    max_data_points: int,
    execution_time: float,
    query_metadata: Optional[Dict] = None
) -> AggregatedDataResponse:
    """Convert DataFrame result to aggregated response format with accurate truncation metadata.
    
    Uses QueryResult metadata to provide correct truncation information instead of
    recalculating, avoiding double-truncation issues.
    """
    
    data = []
    
    # Use truncation metadata from query result if available
    if query_metadata:
        truncated = query_metadata.get('truncated', False)
        actual_end_time = query_metadata.get('actual_end_time')
        
        # Convert actual_end_time to milliseconds if available
        truncated_end_time = None
        if truncated and actual_end_time:
            try:
                if hasattr(actual_end_time, 'timestamp'):
                    # It's a datetime object
                    truncated_end_time = int(actual_end_time.timestamp() * 1000)
                elif isinstance(actual_end_time, (int, float)):
                    # It's already in milliseconds or seconds
                    truncated_end_time = int(actual_end_time * 1000) if actual_end_time < 1e10 else int(actual_end_time)
            except Exception as e:
                logger.warning(f"Failed to convert actual_end_time to milliseconds: {e}")
                truncated_end_time = None
    else:
        # Fallback: use old truncation logic if no metadata provided
        truncated = False
        truncated_end_time = None
    
    if not df.empty and 'timestamp' in df.columns:
        # Ensure data is sorted by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        # Find value columns
        value_columns = [col for col in df.columns 
                        if col not in ['timestamp', 'sensor_name', 'asset_id', 'device_id']]
        
        # Use the first numeric column as the primary value
        value_col = None
        for col in value_columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                value_col = col
                break
        
        if value_col:
            for _, row in df.iterrows():
                try:
                    timestamp = pd.to_datetime(row['timestamp'])
                    timestamp_ms = int(timestamp.timestamp() * 1000)
                    value = row[value_col] if pd.notna(row[value_col]) else None
                    
                    data.append([timestamp_ms, value])
                except Exception as e:
                    logger.warning(f"Failed to convert data point for {sensor}: {e}")
                    continue
        
        # Only apply fallback truncation if no metadata was provided
        if not query_metadata and len(data) > max_data_points:
            truncated = True
            # Keep first N points to maintain interval consistency
            truncated_data = data[:max_data_points]
            data = truncated_data
            
            # Track the end timestamp of truncation
            if truncated_data:
                truncated_end_time = truncated_data[-1][0]  # [timestamp_ms, value]
    
    return AggregatedDataResponse(
        data=data,
        count=len(data),
        device=device,
        sensor=sensor,
        interval_ms=interval_ms,
        aggregation_method=aggregation_method,
        truncated=truncated,
        truncated_end_time=truncated_end_time,
        execution_time_ms=execution_time
    )


@unified_router.post("/raw", response_model=RawDataResponse)
async def query_raw_data(
    request: RawDataRequest,
    engine: SmartQueryEngine = Depends(get_query_engine)
):
    """
    Query raw sensor data with simple parameters.
    
    Returns timestamped values for a specific device/sensor combination.
    Supports both Gen1 (UUID-based) and Gen2 (custom ID) devices.
    """
    try:
        start_exec_time = time.time()
        
        # Parse time range
        start_dt, end_dt = request.get_datetime_range()
        
        # Normalize sensor name
        sensor_name = request.sensor.replace('-', '_').replace(' ', '_')
        
        # Use device as asset_id (works for both Gen1 and Gen2)
        asset_id = request.device
        
        # Execute raw data query
        result = engine.query_sensor_data(
            sensors=[sensor_name],
            start_time=start_dt,
            end_time=end_dt,
            asset_ids=[asset_id],
            interval_ms=None,  # Raw data - no aggregation
            max_datapoints=None,  # Return all raw data points  
            aggregation=None
        )
        
        execution_time = (time.time() - start_exec_time) * 1000
        
        # Convert to simple response format
        response = convert_dataframe_to_raw_response(
            result.data, request.device, request.sensor, execution_time
        )
        
        return response
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Raw data query failed: {e}")
        raise HTTPException(status_code=500, detail="Raw data query execution failed")


@unified_router.post("/aggregated", response_model=AggregatedDataResponse)
async def query_aggregated_data(
    request: AggregatedDataRequest,
    engine: SmartQueryEngine = Depends(get_query_engine)
):
    """
    Query aggregated sensor data with clean parameters.
    
    Supports automatic tier selection and truncation with exact interval preservation.
    If data points exceed max_data_points, truncates while maintaining interval_ms.
    """
    try:
        start_exec_time = time.time()
        
        # Parse time range
        start_dt, end_dt = request.get_datetime_range()
        
        # Normalize sensor name
        sensor_name = request.sensor.replace('-', '_').replace(' ', '_')
        
        # Convert aggregation method
        aggregation_method = DeviceDetector.convert_aggregation_method(request.aggregation_method)
        
        # Use device as asset_id (works for both Gen1 and Gen2)
        asset_id = request.device
        
        # Execute aggregated data query with exact interval aggregation
        result = engine.query_sensor_data(
            sensors=[sensor_name],
            start_time=start_dt,
            end_time=end_dt,
            asset_ids=[asset_id],
            interval_ms=request.interval_ms,
            max_datapoints=request.max_data_points,
            aggregation=aggregation_method,
            use_exact_interval=True  # Force exact interval for aggregated API
        )
        
        execution_time = (time.time() - start_exec_time) * 1000
        
        # Convert to aggregated response format with truncation
        response = convert_dataframe_to_aggregated_response(
            result.data,
            request.device,
            request.sensor,  # Use original sensor name for response
            request.interval_ms,
            request.aggregation_method,
            request.max_data_points,
            execution_time,
            result.metadata  # Pass QueryResult metadata for accurate truncation info
        )
        
        return response
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Aggregated data query failed: {e}")
        raise HTTPException(status_code=500, detail="Aggregated data query execution failed")


@unified_router.get("/devices", response_model=DeviceListResponse)
async def list_devices(
    device_type: Optional[str] = Query(None, description="Filter by device type (gen1/gen2)"),
    location: Optional[str] = Query(None, description="Filter by location pattern"),
    engine: SmartQueryEngine = Depends(get_query_engine)
):
    """
    Discover available devices across both Gen1 and Gen2 systems.
    """
    try:
        # Get available assets (which map to devices)
        assets = engine.get_available_assets()
        
        devices = []
        filters_applied = {}
        
        for asset in assets:
            # Detect device type
            detected_type = DeviceDetector.detect_device_type(asset)
            
            # Apply device type filter
            if device_type and detected_type != device_type:
                continue
            
            # Get sensors for this device
            sensors = engine.get_available_sensors(asset)
            
            # Apply location filter (if provided)
            if location and location.lower() not in asset.lower():
                continue
            
            # Get time range for this device
            try:
                min_time, max_time = engine.get_time_range(sensors[:1], [asset])
                data_count = None  # Could be enhanced with actual count query
            except:
                min_time, max_time, data_count = None, None, None
            
            device_info = DeviceInfo(
                device_id=asset,
                device_type=detected_type,
                location=None,  # Could be extracted from asset naming
                available_sensors=sensors,
                first_seen=min_time,
                last_seen=max_time,
                data_count=data_count
            )
            
            devices.append(device_info)
        
        # Track applied filters
        if device_type:
            filters_applied['device_type'] = device_type
        if location:
            filters_applied['location'] = location
        
        return DeviceListResponse(
            devices=devices,
            total_count=len(devices),
            filters_applied=filters_applied if filters_applied else None
        )
        
    except Exception as e:
        logger.error(f"Failed to list devices: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve device list")


@unified_router.get("/sensors", response_model=SensorListResponse)
async def list_sensors(
    device: Optional[str] = Query(None, description="Filter by specific device"),
    sensor_pattern: Optional[str] = Query(None, description="Filter by sensor name pattern"),
    engine: SmartQueryEngine = Depends(get_query_engine)
):
    """
    Discover available sensors across all devices.
    """
    try:
        if device:
            # Get sensors for specific device
            sensors = engine.get_available_sensors(device)
            device_filter = device
        else:
            # Get all sensors across all devices
            sensors = engine.get_available_sensors()
            device_filter = None
        
        sensor_infos = []
        
        for sensor in sensors:
            # Apply sensor pattern filter
            if sensor_pattern and sensor_pattern.lower() not in sensor.lower():
                continue
            
            # Get devices that have this sensor
            if device:
                available_devices = [device]
            else:
                # Would need to query which devices have this sensor
                available_devices = []  # Simplified for now
            
            # Get time range for this sensor
            try:
                min_time, max_time = engine.get_time_range([sensor], None)
                data_range = {'min_time': min_time, 'max_time': max_time} if min_time and max_time else None
            except:
                data_range = None
            
            sensor_info = SensorInfo(
                sensor_name=sensor,
                available_devices=available_devices,
                data_range=data_range,
                typical_units=None  # Could be inferred from sensor name
            )
            
            sensor_infos.append(sensor_info)
        
        return SensorListResponse(
            sensors=sensor_infos,
            total_count=len(sensor_infos),
            device_filter=device_filter
        )
        
    except Exception as e:
        logger.error(f"Failed to list sensors: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve sensor list")


@unified_router.get("/health")
async def health_check():
    """Health check for unified API layer."""
    return {
        "status": "healthy",
        "api_version": "v2",
        "features": {
            "multi_device_support": True,
            "gen1_support": True,
            "gen2_support": True,
            "auto_detection": True,
            "unified_responses": True
        },
        "endpoints": {
            "raw_data": "/api/v2/raw",
            "aggregated_data": "/api/v2/aggregated",
            "device_discovery": "/api/v2/devices",
            "sensor_discovery": "/api/v2/sensors"
        },
        "timestamp": datetime.utcnow()
    }


@unified_router.get("/info")
async def api_info():
    """Information about the unified API."""
    return {
        "api": "Unified Device API",
        "version": "2.0",
        "description": "Modern API supporting both Gen1 and Gen2 devices",
        "inspiration": "Designed based on Flatpak API patterns",
        "features": {
            "device_support": {
                "gen1": "UUID-based devices with standard sensor identifiers",
                "gen2": "Custom device IDs with flexible sensor naming"
            },
            "query_types": {
                "raw": "High-resolution raw data with 1-second precision",
                "aggregated": "Smart aggregation with automatic optimization"
            },
            "multi_target": "Query multiple devices/sensors in single request",
            "auto_optimization": "Automatic tier selection and data optimization",
            "flexible_time": "Support for ISO strings, datetime objects, and epoch timestamps"
        },
        "migration_from_flatpak": {
            "raw_data": "Use /api/v2/raw with targets array",
            "aggregated_data": "Use /api/v2/aggregated with targets array and optimization parameters"
        }
    }
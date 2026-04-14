"""
Simplified API routes for sensor data service.
"""

import logging
import time
import pandas as pd
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse

from app.query_engine_simple import SimpleQueryEngine
from app.api.models_simple import (
    RawDataRequest, AggregatedDataRequest, RawDataResponse, AggregatedDataResponse,
    AssetListResponse, SensorListResponse, HealthResponse, ErrorResponse
)

logger = logging.getLogger(__name__)

# Global query engine instance
_query_engine: Optional[SimpleQueryEngine] = None

def set_query_engine(engine: SimpleQueryEngine):
    """Set the global query engine instance."""
    global _query_engine
    _query_engine = engine

def get_query_engine() -> SimpleQueryEngine:
    """Get the query engine instance."""
    if _query_engine is None:
        raise HTTPException(status_code=500, detail="Query engine not initialized")
    return _query_engine

# Create router
router = APIRouter()


def convert_dataframe_to_raw_response(df: pd.DataFrame, device: str, sensor: str, 
                                    execution_time: float) -> RawDataResponse:
    """Convert DataFrame to raw data response."""
    data = []
    
    if not df.empty and 'timestamp' in df.columns and 'value' in df.columns:
        for _, row in df.iterrows():
            try:
                timestamp = pd.to_datetime(row['timestamp'])
                timestamp_ms = int(timestamp.timestamp() * 1000)
                value = row['value'] if pd.notna(row['value']) else None
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


def convert_dataframe_to_aggregated_response(df: pd.DataFrame, device: str, sensor: str,
                                           interval_ms: int, aggregation_method: str,
                                           max_data_points: int, execution_time: float,
                                           truncated: bool) -> AggregatedDataResponse:
    """Convert DataFrame to aggregated data response."""
    data = []
    truncated_end_time = None
    
    if not df.empty and 'timestamp' in df.columns and 'value' in df.columns:
        for _, row in df.iterrows():
            try:
                timestamp = pd.to_datetime(row['timestamp'])
                timestamp_ms = int(timestamp.timestamp() * 1000)
                value = row['value'] if pd.notna(row['value']) else None
                data.append([timestamp_ms, value])
            except Exception as e:
                logger.warning(f"Failed to convert data point: {e}")
                continue
        
        # Set truncated end time if truncated
        if truncated and data:
            truncated_end_time = data[-1][0]
    
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


@router.post("/raw", response_model=RawDataResponse)
async def query_raw_data(
    request: RawDataRequest,
    engine: SimpleQueryEngine = Depends(get_query_engine)
):
    """Query raw sensor data."""
    try:
        # Parse time range
        start_dt, end_dt = request.get_datetime_range()
        
        # Normalize sensor name
        sensor_name = request.sensor.replace('-', '_').replace(' ', '_')
        
        # Execute query
        result = engine.query_sensor_data(
            sensors=[sensor_name],
            start_time=start_dt,
            end_time=end_dt,
            asset_ids=[request.device],
            interval_ms=None,  # Raw data
            max_datapoints=None  # Return all raw data
        )
        
        # Convert to response
        response = convert_dataframe_to_raw_response(
            result.data, request.device, request.sensor, result.execution_time_ms
        )
        
        logger.info(f"Raw query completed: {response.count} rows in {result.execution_time_ms:.1f}ms")
        return response
        
    except ValueError as e:
        logger.warning(f"Invalid raw data request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Raw data query failed: {e}")
        raise HTTPException(status_code=500, detail="Raw data query execution failed")


@router.post("/aggregated", response_model=AggregatedDataResponse) 
async def query_aggregated_data(
    request: AggregatedDataRequest,
    engine: SimpleQueryEngine = Depends(get_query_engine)
):
    """Query aggregated sensor data."""
    try:
        # Parse time range
        start_dt, end_dt = request.get_datetime_range()
        
        # Normalize sensor name
        sensor_name = request.sensor.replace('-', '_').replace(' ', '_')
        
        # Execute query
        result = engine.query_sensor_data(
            sensors=[sensor_name],
            start_time=start_dt,
            end_time=end_dt,
            asset_ids=[request.device],
            interval_ms=request.interval_ms,
            max_datapoints=request.max_data_points,
            aggregation_method=request.aggregation_method
        )
        
        # Convert to response
        response = convert_dataframe_to_aggregated_response(
            result.data, request.device, request.sensor,
            request.interval_ms, request.aggregation_method,
            request.max_data_points, result.execution_time_ms,
            result.truncated
        )
        
        logger.info(f"Aggregated query completed: {response.count} rows in {result.execution_time_ms:.1f}ms")
        return response
        
    except ValueError as e:
        logger.warning(f"Invalid aggregated data request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Aggregated data query failed: {e}")
        raise HTTPException(status_code=500, detail="Aggregated data query execution failed")


@router.get("/assets", response_model=AssetListResponse)
async def list_assets(
    engine: SimpleQueryEngine = Depends(get_query_engine)
):
    """List available assets."""
    try:
        assets = engine.get_available_assets()
        return AssetListResponse(
            assets=assets,
            count=len(assets)
        )
    except Exception as e:
        logger.error(f"Failed to list assets: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve asset list")


@router.get("/sensors", response_model=SensorListResponse)
async def list_sensors(
    asset: Optional[str] = None,
    engine: SimpleQueryEngine = Depends(get_query_engine)
):
    """List available sensors."""
    try:
        sensors = engine.get_available_sensors(asset)
        return SensorListResponse(
            sensors=sensors,
            count=len(sensors),
            asset_filter=asset
        )
    except Exception as e:
        logger.error(f"Failed to list sensors: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve sensor list")


@router.get("/health", response_model=HealthResponse)
async def health_check(
    engine: SimpleQueryEngine = Depends(get_query_engine)
):
    """Check service health."""
    try:
        health = engine.health_check()
        
        return HealthResponse(
            status="healthy" if health['engine_healthy'] else "unhealthy",
            storage_mode=health['configuration']['storage_mode'],
            engine_healthy=health['engine_healthy'],
            duckdb_version=health['storage_backend'].get('duckdb_version'),
            statistics=health['statistics']
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            storage_mode="unknown", 
            engine_healthy=False,
            statistics={}
        )


@router.get("/info")
async def api_info():
    """Get API information."""
    return {
        "name": "Simplified Sensor Data API",
        "version": "2.0-simple",
        "description": "DuckDB-only sensor data query service with ADLS Gen2 support",
        "features": {
            "duckdb_native": True,
            "adls_gen2_support": True,
            "local_storage_support": True,
            "unified_backend": True
        },
        "endpoints": {
            "raw_data": "/raw",
            "aggregated_data": "/aggregated", 
            "asset_discovery": "/assets",
            "sensor_discovery": "/sensors",
            "health_check": "/health"
        }
    }
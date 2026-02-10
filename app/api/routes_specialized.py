"""
Specialized FastAPI routes for raw and aggregated data APIs.
"""

import logging
from typing import List, Optional
from datetime import datetime
import time

from fastapi import FastAPI, HTTPException, Query, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import AppConfig
from app.query.engine import SmartQueryEngine
from app.query.specialized_engine import RawDataEngine, AggregatedDataEngine
from app.api.models import (
    RawDataRequest, AggregatedDataRequest, RawDataResponse, AggregatedDataResponse,
    QueryMetadata, SensorListResponse, TimeRangeResponse, StatsResponse, 
    HealthResponse, ErrorResponse, SuccessResponse, ConfigResponse,
    AggregationMethod, SensorInfo, QueryStats, CacheStats
)

logger = logging.getLogger(__name__)

# Global engine references
query_engine: Optional[SmartQueryEngine] = None
raw_data_engine: Optional[RawDataEngine] = None
aggregated_data_engine: Optional[AggregatedDataEngine] = None
service_start_time = time.time()


def get_engines():
    """Dependency to get all engines."""
    if query_engine is None or raw_data_engine is None or aggregated_data_engine is None:
        raise HTTPException(status_code=503, detail="Engines not initialized")
    return query_engine, raw_data_engine, aggregated_data_engine


def create_specialized_app(config: AppConfig, engine: SmartQueryEngine) -> FastAPI:
    """Create FastAPI application with specialized raw and aggregated endpoints."""
    global query_engine, raw_data_engine, aggregated_data_engine
    
    query_engine = engine
    raw_data_engine = RawDataEngine(engine, config)
    aggregated_data_engine = AggregatedDataEngine(engine, config)
    
    app = FastAPI(
        title="Sensor Data Query Service",
        description="Optimized APIs for raw and aggregated sensor data queries",
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Add CORS middleware if configured
    if config.api.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.api.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    # Error handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logger.error(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="Internal server error",
                detail=str(exc),
                timestamp=datetime.utcnow()
            ).dict()
        )
    
    # ===== RAW DATA API =====
    @app.get("/api/v1/raw-data", response_model=RawDataResponse, 
             summary="Get Raw Sensor Data", 
             description="Returns raw sensor data with 1-second precision from original TimescaleDB")
    async def get_raw_data(
        start_date: datetime = Query(..., description="Start date (inclusive)"),
        end_date: datetime = Query(..., description="End date (exclusive)"),
        sensor_types: str = Query(..., description="Comma-separated sensor types (e.g., quad_ch1,quad_ch2)"),
        engines = Depends(get_engines)
    ):
        """Get raw sensor data with 1-second precision."""
        try:
            base_engine, raw_engine, _ = engines
            
            # Parse sensor types
            sensor_list = [s.strip() for s in sensor_types.split(',')]
            
            # Validate parameters
            if start_date >= end_date:
                raise HTTPException(status_code=400, detail="start_date must be before end_date")
            
            # Execute raw data query
            result = raw_engine.query_raw_data(sensor_list, start_date, end_date)
            
            return RawDataResponse(
                data=result['data'],
                metadata=QueryMetadata(**result['metadata'])
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Raw data query failed: {e}")
            raise HTTPException(status_code=500, detail="Raw data query failed")
    
    @app.post("/api/v1/raw-data", response_model=RawDataResponse,
              summary="Get Raw Sensor Data (POST)",
              description="Returns raw sensor data using POST body")
    async def post_raw_data(
        request: RawDataRequest = Body(...),
        engines = Depends(get_engines)
    ):
        """Get raw sensor data using POST request."""
        try:
            base_engine, raw_engine, _ = engines
            
            # Execute raw data query
            result = raw_engine.query_raw_data(
                request.sensor_types, 
                request.start_date, 
                request.end_date
            )
            
            return RawDataResponse(
                data=result['data'],
                metadata=QueryMetadata(**result['metadata'])
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Raw data query failed: {e}")
            raise HTTPException(status_code=500, detail="Raw data query failed")
    
    # ===== AGGREGATED DATA API =====
    @app.get("/api/v1/aggregated-data", response_model=AggregatedDataResponse,
             summary="Get Aggregated Sensor Data",
             description="Returns aggregated sensor data with smart optimization")
    async def get_aggregated_data(
        start_date: datetime = Query(..., description="Start date (inclusive)"),
        end_date: datetime = Query(..., description="End date (exclusive)"),
        sensor_types: str = Query(..., description="Comma-separated sensor types"),
        aggregation_type: AggregationMethod = Query(..., description="Aggregation method: min, max, or mean"),
        interval_ms: Optional[int] = Query(None, description="Interval in milliseconds (auto-calculated if not provided)"),
        engines = Depends(get_engines)
    ):
        """Get aggregated sensor data with smart optimization."""
        try:
            base_engine, _, agg_engine = engines
            
            # Parse sensor types
            sensor_list = [s.strip() for s in sensor_types.split(',')]
            
            # Validate parameters
            if start_date >= end_date:
                raise HTTPException(status_code=400, detail="start_date must be before end_date")
            
            # Execute aggregated data query
            result = agg_engine.query_aggregated_data(
                sensor_list, start_date, end_date, interval_ms, aggregation_type.value
            )
            
            return AggregatedDataResponse(
                data=result['data'],
                metadata=QueryMetadata(**result['metadata'])
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Aggregated data query failed: {e}")
            raise HTTPException(status_code=500, detail="Aggregated data query failed")
    
    @app.post("/api/v1/aggregated-data", response_model=AggregatedDataResponse,
              summary="Get Aggregated Sensor Data (POST)",
              description="Returns aggregated sensor data using POST body")
    async def post_aggregated_data(
        request: AggregatedDataRequest = Body(...),
        engines = Depends(get_engines)
    ):
        """Get aggregated sensor data using POST request."""
        try:
            base_engine, _, agg_engine = engines
            
            # Execute aggregated data query
            result = agg_engine.query_aggregated_data(
                request.sensor_types,
                request.start_date, 
                request.end_date,
                request.interval_ms,
                request.aggregation_type.value
            )
            
            return AggregatedDataResponse(
                data=result['data'],
                metadata=QueryMetadata(**result['metadata'])
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Aggregated data query failed: {e}")
            raise HTTPException(status_code=500, detail="Aggregated data query failed")
    
    # ===== HELPER ENDPOINTS =====
    @app.get("/api/v1/interval/recommend", 
             summary="Get Recommended Interval",
             description="Get recommended interval for optimal query performance")
    async def get_recommended_interval(
        start_date: datetime = Query(..., description="Start date"),
        end_date: datetime = Query(..., description="End date"),
        sensor_types: str = Query(..., description="Comma-separated sensor types"),
        target_points: Optional[int] = Query(None, description="Target number of data points"),
        engines = Depends(get_engines)
    ):
        """Get recommended interval for optimal performance."""
        try:
            base_engine, _, agg_engine = engines
            
            sensor_list = [s.strip() for s in sensor_types.split(',')]
            
            recommendation = agg_engine.get_recommended_interval(
                sensor_list, start_date, end_date, target_points
            )
            
            return recommendation
            
        except Exception as e:
            logger.error(f"Interval recommendation failed: {e}")
            raise HTTPException(status_code=500, detail="Failed to calculate recommended interval")
    
    @app.get("/api/v1/estimate", 
             summary="Estimate Data Points",
             description="Estimate number of data points for given parameters")
    async def estimate_datapoints(
        start_date: datetime = Query(..., description="Start date"),
        end_date: datetime = Query(..., description="End date"),
        sensor_types: str = Query(..., description="Comma-separated sensor types"),
        interval_ms: int = Query(..., description="Interval in milliseconds"),
        engines = Depends(get_engines)
    ):
        """Estimate number of data points for given parameters."""
        try:
            base_engine, _, agg_engine = engines
            
            sensor_list = [s.strip() for s in sensor_types.split(',')]
            
            estimated_points = agg_engine.estimate_datapoints(
                sensor_list, start_date, end_date, interval_ms
            )
            
            return {
                'estimated_datapoints': estimated_points,
                'sensor_types': sensor_list,
                'start_date': start_date,
                'end_date': end_date,
                'interval_ms': interval_ms,
                'duration_hours': (end_date - start_date).total_seconds() / 3600
            }
            
        except Exception as e:
            logger.error(f"Datapoint estimation failed: {e}")
            raise HTTPException(status_code=500, detail="Failed to estimate data points")
    
    # ===== DISCOVERY ENDPOINTS =====
    @app.get("/api/v1/sensors", response_model=SensorListResponse)
    async def list_sensors(engines = Depends(get_engines)):
        """List available sensors."""
        try:
            base_engine, _, _ = engines
            
            sensors = base_engine.get_available_sensors()
            
            # Convert to SensorInfo objects
            sensor_info = []
            for sensor in sensors:
                info = SensorInfo(
                    name=sensor,
                    asset_ids=[],  # Would need additional query
                    data_count=None,
                    first_seen=None,
                    last_seen=None
                )
                sensor_info.append(info)
            
            return SensorListResponse(
                sensors=sensor_info,
                total_count=len(sensor_info)
            )
            
        except Exception as e:
            logger.error(f"Failed to list sensors: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve sensor list")
    
    @app.get("/api/v1/timerange", response_model=TimeRangeResponse)
    async def get_time_range(
        sensor_types: str = Query(..., description="Comma-separated sensor types"),
        engines = Depends(get_engines)
    ):
        """Get available time range for sensors."""
        try:
            base_engine, _, _ = engines
            
            sensor_list = [s.strip() for s in sensor_types.split(',')]
            min_time, max_time = base_engine.get_time_range(sensor_list)
            
            duration_hours = None
            if min_time and max_time:
                duration_hours = (max_time - min_time).total_seconds() / 3600
            
            return TimeRangeResponse(
                sensor_types=sensor_list,
                min_date=min_time,
                max_date=max_time,
                duration_hours=duration_hours
            )
            
        except Exception as e:
            logger.error(f"Failed to get time range: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve time range")
    
    # ===== CONFIGURATION ENDPOINT =====
    @app.get("/api/v1/config", response_model=ConfigResponse)
    async def get_config(engines = Depends(get_engines)):
        """Get service configuration information."""
        try:
            base_engine, raw_engine, agg_engine = engines
            
            return ConfigResponse(
                max_datapoints=agg_engine.max_datapoints,
                supported_aggregations=[e.value for e in AggregationMethod],
                storage_mode=config.storage_mode.value,
                tier_thresholds={
                    'raw_tier_max_hours': config.tiers.raw_tier_max_hours,
                    'aggregated_tier_max_hours': config.tiers.aggregated_tier_max_hours,
                    'daily_tier_threshold_hours': config.tiers.daily_tier_threshold_hours
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to get config: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve configuration")
    
    # ===== MANAGEMENT ENDPOINTS =====
    @app.post("/api/v1/cache/clear", response_model=SuccessResponse)
    async def clear_cache(engines = Depends(get_engines)):
        """Clear query cache."""
        try:
            base_engine, _, _ = engines
            base_engine.clear_cache()
            return SuccessResponse(
                message="Cache cleared successfully",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            raise HTTPException(status_code=500, detail="Failed to clear cache")
    
    @app.get("/api/v1/stats", response_model=StatsResponse)
    async def get_stats(engines = Depends(get_engines)):
        """Get service statistics."""
        try:
            base_engine, _, _ = engines
            stats = base_engine.get_query_stats()
            uptime = time.time() - service_start_time
            
            return StatsResponse(
                query_stats=QueryStats(**stats),
                cache_stats=CacheStats(**stats['cache_stats']),
                uptime_seconds=uptime
            )
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve statistics")
    
    # ===== HEALTH ENDPOINTS =====
    @app.get("/health")
    async def health_check(engines = Depends(get_engines)):
        """Comprehensive health check."""
        try:
            base_engine, _, _ = engines
            health = base_engine.health_check()
            return health
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(status_code=500, detail="Health check failed")
    
    @app.get("/health/simple")
    async def simple_health_check():
        """Simple health check endpoint."""
        return {"status": "healthy", "timestamp": datetime.utcnow()}
    
    # ===== ROOT ENDPOINT =====
    @app.get("/")
    async def root():
        """Root endpoint with API information."""
        return {
            "service": "Sensor Data Query Service",
            "version": "2.0.0",
            "description": "Optimized APIs for raw and aggregated sensor data",
            "apis": {
                "raw_data": "/api/v1/raw-data",
                "aggregated_data": "/api/v1/aggregated-data",
                "sensors": "/api/v1/sensors",
                "config": "/api/v1/config"
            },
            "docs": "/docs",
            "health": "/health"
        }
    
    return app
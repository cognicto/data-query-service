"""
FastAPI routes for the sensor data query service.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import time

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import pandas as pd

from app.config import AppConfig, AggregationMethod
from app.query.engine import SmartQueryEngine
from app.api.models import (
    QueryRequest, QueryResponse, QueryMetadata, SensorListResponse, AssetListResponse,
    TimeRangeResponse, StatsResponse, HealthResponse, ErrorResponse, SuccessResponse,
    SensorInfo, AssetInfo, QueryStats, CacheStats, ComponentHealth, HealthStatus
)

logger = logging.getLogger(__name__)

# Global query engine reference
query_engine: Optional[SmartQueryEngine] = None
service_start_time = time.time()


def get_query_engine():
    """Dependency to get query engine."""
    if query_engine is None:
        raise HTTPException(status_code=503, detail="Query engine not initialized")
    return query_engine


def create_app(config: AppConfig, engine: SmartQueryEngine) -> FastAPI:
    """Create FastAPI application with all routes."""
    global query_engine
    query_engine = engine
    
    app = FastAPI(
        title="Sensor Data Query Service",
        description="High-performance API for querying sensor data with smart optimization",
        version="1.0.0",
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
    
    # Query endpoints
    @app.get("/api/v1/query", response_model=QueryResponse)
    async def query_sensor_data(
        sensors: str = Query(..., description="Comma-separated list of sensor names"),
        start: datetime = Query(..., alias="start_time", description="Start time (ISO 8601)"),
        end: datetime = Query(..., alias="end_time", description="End time (ISO 8601)"),
        assets: Optional[str] = Query(None, alias="asset_ids", description="Comma-separated asset IDs"),
        interval_ms: Optional[int] = Query(None, description="Interval in milliseconds"),
        max_datapoints: Optional[int] = Query(None, description="Maximum data points"),
        aggregation: Optional[AggregationMethod] = Query(AggregationMethod.avg, description="Aggregation method"),
        engine: SmartQueryEngine = Depends(get_query_engine)
    ):
        """Query sensor data with smart optimization."""
        try:
            # Parse parameters
            sensor_list = [s.strip() for s in sensors.split(',')]
            asset_list = [a.strip() for a in assets.split(',')] if assets else None
            
            # Execute query
            result = engine.query_sensor_data(
                sensors=sensor_list,
                start_time=start,
                end_time=end,
                asset_ids=asset_list,
                interval_ms=interval_ms,
                max_datapoints=max_datapoints,
                aggregation=aggregation.value if aggregation else None
            )
            
            # Convert DataFrame to list of dicts
            if not result.data.empty:
                # Convert timestamps to ISO format
                data_dict = result.data.to_dict('records')
                for record in data_dict:
                    if 'timestamp' in record and pd.notna(record['timestamp']):
                        record['timestamp'] = pd.to_datetime(record['timestamp']).isoformat()
            else:
                data_dict = []
            
            return QueryResponse(
                data=data_dict,
                metadata=QueryMetadata(
                    cache_hit=result.cache_hit,
                    tier_used=result.tier_used,
                    execution_time_ms=result.execution_time_ms,
                    truncated=result.truncated,
                    actual_end_time=result.actual_end_time,
                    original_datapoints=result.metadata.get('original_datapoints')
                ),
                count=len(data_dict)
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise HTTPException(status_code=500, detail="Query execution failed")
    
    @app.post("/api/v1/query", response_model=QueryResponse)
    async def query_sensor_data_post(
        request: QueryRequest,
        engine: SmartQueryEngine = Depends(get_query_engine)
    ):
        """Query sensor data using POST request body."""
        try:
            # Execute query
            result = engine.query_sensor_data(
                sensors=request.sensors,
                start_time=request.start_time,
                end_time=request.end_time,
                asset_ids=request.asset_ids,
                interval_ms=request.interval_ms,
                max_datapoints=request.max_datapoints,
                aggregation=request.aggregation.value if request.aggregation else None
            )
            
            # Convert DataFrame to list of dicts
            if not result.data.empty:
                data_dict = result.data.to_dict('records')
                for record in data_dict:
                    if 'timestamp' in record and pd.notna(record['timestamp']):
                        record['timestamp'] = pd.to_datetime(record['timestamp']).isoformat()
            else:
                data_dict = []
            
            return QueryResponse(
                data=data_dict,
                metadata=QueryMetadata(
                    cache_hit=result.cache_hit,
                    tier_used=result.tier_used,
                    execution_time_ms=result.execution_time_ms,
                    truncated=result.truncated,
                    actual_end_time=result.actual_end_time,
                    original_datapoints=result.metadata.get('original_datapoints')
                ),
                count=len(data_dict)
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise HTTPException(status_code=500, detail="Query execution failed")
    
    # Discovery endpoints
    @app.get("/api/v1/sensors", response_model=SensorListResponse)
    async def list_sensors(
        asset_id: Optional[str] = Query(None, description="Filter by asset ID"),
        engine: SmartQueryEngine = Depends(get_query_engine)
    ):
        """List available sensors."""
        try:
            sensors = engine.get_available_sensors(asset_id)
            
            # Get additional info for each sensor
            sensor_info = []
            for sensor in sensors:
                # For now, just basic info - could be enhanced with actual statistics
                info = SensorInfo(
                    name=sensor,
                    asset_ids=[],  # Would need to query this
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
    
    @app.get("/api/v1/assets", response_model=AssetListResponse)
    async def list_assets(engine: SmartQueryEngine = Depends(get_query_engine)):
        """List available assets."""
        try:
            assets = engine.get_available_assets()
            
            # Get additional info for each asset
            asset_info = []
            for asset in assets:
                # Get sensors for this asset
                sensors = engine.get_available_sensors(asset)
                
                info = AssetInfo(
                    id=asset,
                    sensors=sensors,
                    data_count=None,
                    first_seen=None,
                    last_seen=None
                )
                asset_info.append(info)
            
            return AssetListResponse(
                assets=asset_info,
                total_count=len(asset_info)
            )
            
        except Exception as e:
            logger.error(f"Failed to list assets: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve asset list")
    
    @app.get("/api/v1/timerange", response_model=TimeRangeResponse)
    async def get_time_range(
        sensors: str = Query(..., description="Comma-separated list of sensor names"),
        asset_ids: Optional[str] = Query(None, description="Comma-separated asset IDs"),
        engine: SmartQueryEngine = Depends(get_query_engine)
    ):
        """Get available time range for sensors."""
        try:
            sensor_list = [s.strip() for s in sensors.split(',')]
            asset_list = [a.strip() for a in asset_ids.split(',')] if asset_ids else None
            
            min_time, max_time = engine.get_time_range(sensor_list, asset_list)
            
            duration_hours = None
            if min_time and max_time:
                duration_hours = (max_time - min_time).total_seconds() / 3600
            
            return TimeRangeResponse(
                sensors=sensor_list,
                asset_ids=asset_list,
                min_time=min_time,
                max_time=max_time,
                duration_hours=duration_hours
            )
            
        except Exception as e:
            logger.error(f"Failed to get time range: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve time range")
    
    # Management endpoints
    @app.post("/api/v1/cache/clear", response_model=SuccessResponse)
    async def clear_cache(engine: SmartQueryEngine = Depends(get_query_engine)):
        """Clear query cache."""
        try:
            engine.clear_cache()
            return SuccessResponse(
                message="Cache cleared successfully",
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            raise HTTPException(status_code=500, detail="Failed to clear cache")
    
    @app.post("/api/v1/aggregation/rebuild", response_model=SuccessResponse)
    async def rebuild_aggregation(
        sensors: Optional[str] = Query(None, description="Comma-separated sensor names to rebuild"),
        start_date: Optional[str] = Query(None, description="Start date for rebuild (YYYY-MM-DD)"),
        end_date: Optional[str] = Query(None, description="End date for rebuild (YYYY-MM-DD)"),
        engine: SmartQueryEngine = Depends(get_query_engine)
    ):
        """Rebuild aggregated data tiers."""
        try:
            from app.aggregation.rebuilder import AggregationRebuilder
            
            rebuilder = AggregationRebuilder(engine)
            
            sensor_list = None
            if sensors:
                sensor_list = [s.strip() for s in sensors.split(',')]
            
            start_time = None
            end_time = None
            if start_date:
                start_time = datetime.strptime(start_date, '%Y-%m-%d')
            if end_date:
                end_time = datetime.strptime(end_date, '%Y-%m-%d')
            
            success = rebuilder.rebuild_aggregated_data(
                sensors=sensor_list,
                start_time=start_time,
                end_time=end_time
            )
            
            if success:
                return SuccessResponse(
                    message="Aggregation rebuild completed successfully",
                    timestamp=datetime.utcnow()
                )
            else:
                raise HTTPException(status_code=500, detail="Aggregation rebuild failed")
                
        except ImportError:
            raise HTTPException(status_code=501, detail="Aggregation rebuild not implemented")
        except Exception as e:
            logger.error(f"Failed to rebuild aggregation: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to rebuild aggregation: {str(e)}")
    
    @app.get("/api/v1/stats", response_model=StatsResponse)
    async def get_stats(engine: SmartQueryEngine = Depends(get_query_engine)):
        """Get service statistics."""
        try:
            stats = engine.get_query_stats()
            uptime = time.time() - service_start_time
            
            return StatsResponse(
                query_stats=QueryStats(**stats),
                cache_stats=CacheStats(**stats['cache_stats']),
                uptime_seconds=uptime
            )
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve statistics")
    
    # Health check endpoints
    @app.get("/health", response_model=HealthResponse)
    async def health_check(engine: SmartQueryEngine = Depends(get_query_engine)):
        """Comprehensive health check."""
        try:
            health = engine.health_check()
            
            # Convert to response model format
            storage_backends = {}
            for backend_name, backend_health in health['storage_backends'].items():
                storage_backends[backend_name] = ComponentHealth(
                    status=HealthStatus(
                        healthy=backend_health.get('healthy', False),
                        issues=backend_health.get('issues', [])
                    ),
                    details=backend_health
                )
            
            return HealthResponse(
                overall_healthy=health['overall_healthy'],
                storage_backends=storage_backends,
                cache_status=health['cache_status'],
                query_stats=QueryStats(**health['query_stats']),
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(status_code=500, detail="Health check failed")
    
    @app.get("/health/simple")
    async def simple_health_check():
        """Simple health check endpoint."""
        return {"status": "healthy", "timestamp": datetime.utcnow()}
    
    # Metrics endpoint for Prometheus
    @app.get("/metrics")
    async def metrics(engine: SmartQueryEngine = Depends(get_query_engine)):
        """Prometheus metrics endpoint."""
        try:
            stats = engine.get_query_stats()
            
            # Format as Prometheus metrics
            metrics_text = f"""# HELP query_total Total number of queries
# TYPE query_total counter
query_total {stats['total_queries']}

# HELP cache_hits_total Total cache hits
# TYPE cache_hits_total counter
cache_hits_total {stats['cache_hits']}

# HELP cache_hit_rate Cache hit rate
# TYPE cache_hit_rate gauge
cache_hit_rate {stats['cache_hit_rate']}

# HELP avg_execution_time_ms Average execution time in milliseconds
# TYPE avg_execution_time_ms gauge
avg_execution_time_ms {stats['avg_execution_time_ms']}

# HELP tier_usage_total Usage count by storage tier
# TYPE tier_usage_total counter
"""
            
            for tier, count in stats['tier_usage'].items():
                metrics_text += f'tier_usage_total{{tier="{tier}"}} {count}\n'
            
            return Response(content=metrics_text, media_type="text/plain")
            
        except Exception as e:
            logger.error(f"Failed to generate metrics: {e}")
            raise HTTPException(status_code=500, detail="Failed to generate metrics")
    
    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint with service information."""
        return {
            "service": "Sensor Data Query Service",
            "version": "1.0.0",
            "description": "High-performance API for querying sensor data",
            "docs": "/docs",
            "health": "/health",
            "api_base": "/api/v1"
        }
    
    return app
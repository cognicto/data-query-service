"""
Clean FastAPI application with unified routes.
"""

import logging
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import AppConfig
from app.query.engine import SmartQueryEngine
from app.api.unified_routes import unified_router, set_query_engine

logger = logging.getLogger(__name__)


def create_app(config: AppConfig, engine: SmartQueryEngine) -> FastAPI:
    """Create clean FastAPI application with unified routes only."""
    
    # Set the query engine for the routes
    set_query_engine(engine)
    
    app = FastAPI(
        title="Sensor Data Query Service",
        description="Clean APIs for raw and aggregated sensor data queries",
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
    
    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logger.error(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "detail": str(exc),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    # Include unified routes
    app.include_router(unified_router, tags=["Sensor Data APIs"])
    
    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint with service information."""
        return {
            "service": "Sensor Data Query Service",
            "version": "2.0.0",
            "description": "Clean APIs for raw and aggregated sensor data",
            "endpoints": {
                "raw_data": "POST /api/v2/raw",
                "aggregated_data": "POST /api/v2/aggregated",
                "device_discovery": "GET /api/v2/devices", 
                "sensor_discovery": "GET /api/v2/sensors",
                "api_info": "GET /api/v2/info"
            },
            "documentation": {
                "swagger_ui": "/docs",
                "redoc": "/redoc"
            },
            "health": "/api/v2/health"
        }
    
    # Simple health check
    @app.get("/health")
    async def simple_health():
        """Simple health endpoint."""
        return {
            "status": "healthy",
            "service": "Sensor Data Query Service", 
            "timestamp": datetime.utcnow().isoformat()
        }
    
    return app
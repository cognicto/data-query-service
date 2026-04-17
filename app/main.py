"""
Simplified main application for sensor data service.
"""


from dotenv import load_dotenv

load_dotenv()
import logging
import os
import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import load_config
from app.query.query_engine import SimpleQueryEngine
from app.api.routes import router, set_query_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global engine instance
query_engine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global query_engine
    
    # Startup
    try:
        logger.info("Starting simplified sensor data service...")
        
        # Load configuration
        config = load_config()
        logger.info(f"Configuration loaded: {config.storage_mode.value} storage mode")
        
        # Initialize query engine
        query_engine = SimpleQueryEngine(config)
        set_query_engine(query_engine)
        
        # Health check
        health = query_engine.health_check()
        if health['engine_healthy']:
            logger.info("✅ Service initialized successfully")
        else:
            logger.warning("⚠️ Service started but health check failed")
            
        yield
        
    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        raise
    
    # Shutdown
    logger.info("Shutting down service...")
    if query_engine:
        query_engine.close()
    logger.info("Service shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Simplified Sensor Data API",
    description="DuckDB-only sensor data query service with ADLS Gen2 support",
    version="2.0-simple",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v2")

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(f"Unhandled exception in {request.method} {request.url}: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "path": str(request.url)
        }
    )

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Simplified Sensor Data API",
        "version": "2.0-simple",
        "status": "running",
        "endpoints": {
            "api": "/api/v2",
            "docs": "/docs",
            "health": "/api/v2/health"
        }
    }

# Health endpoint (convenient shortcut)
@app.get("/health")
async def health():
    """Quick health check."""
    if query_engine:
        health_data = query_engine.health_check()
        return {"status": "healthy" if health_data['engine_healthy'] else "unhealthy"}
    return {"status": "unavailable"}


def main():
    """Run the application."""
    config = load_config()
    
    logger.info(f"Starting server on {config.api_host}:{config.api_port}")
    
    uvicorn.run(
        "app.main:app",
        host=config.api_host,
        port=config.api_port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
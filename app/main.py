"""
Main application entry point for the sensor data query service.
"""

import asyncio
import logging
import signal
import sys
import time
import uvicorn
from datetime import datetime

from app.config import load_config, validate_config
from app.query.engine import SmartQueryEngine
from app.api.routes_specialized import create_specialized_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/query-service.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)


class QueryService:
    """Main service class that orchestrates all components."""
    
    def __init__(self):
        """Initialize the service."""
        self.config = None
        self.query_engine = None
        self.app = None
        
    def initialize(self) -> bool:
        """Initialize all service components."""
        try:
            # Load and validate configuration
            logger.info("Loading configuration...")
            self.config = load_config()
            
            if not validate_config(self.config):
                logger.error("Configuration validation failed")
                return False
            
            # Initialize query engine
            logger.info("Initializing query engine...")
            self.query_engine = SmartQueryEngine(self.config)
            
            # Create FastAPI app with specialized endpoints
            logger.info("Creating FastAPI application with specialized endpoints...")
            self.app = create_specialized_app(self.config, self.query_engine)
            
            logger.info("Service initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize service: {e}")
            return False
    
    def run_server(self):
        """Run the FastAPI server."""
        try:
            logger.info(f"Starting server on {self.config.api.host}:{self.config.api.port}")
            
            uvicorn.run(
                self.app,
                host=self.config.api.host,
                port=self.config.api.port,
                workers=1,  # Single worker for simplicity with threading
                log_level="info",
                access_log=True,
                loop="asyncio"
            )
            
        except Exception as e:
            logger.error(f"Server failed: {e}")
            raise
    
    def health_check(self) -> bool:
        """Perform startup health check."""
        try:
            if not self.query_engine:
                return False
            
            health = self.query_engine.health_check()
            
            if not health['overall_healthy']:
                logger.error("Health check failed:")
                for backend, status in health['storage_backends'].items():
                    if not status.get('healthy', False):
                        issues = status.get('issues', [])
                        logger.error(f"  {backend}: {', '.join(issues)}")
                return False
            
            logger.info("Health check passed")
            return True
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return False
    
    def start(self) -> bool:
        """Start the complete service."""
        try:
            if not self.initialize():
                logger.error("Service initialization failed")
                return False
            
            # Perform health check
            if not self.health_check():
                logger.warning("Health check failed - some features may not work correctly")
                # Continue anyway for development/debugging
            
            # Log configuration summary
            logger.info("Service Configuration:")
            logger.info(f"  Storage Mode: {self.config.storage_mode}")
            logger.info(f"  Cache Enabled: {self.config.cache.enabled}")
            logger.info(f"  Cache Size: {self.config.cache.size_mb}MB")
            logger.info(f"  Smart Aggregation: {self.config.query.enable_smart_aggregation}")
            logger.info(f"  Max Query Duration: {self.config.query.max_query_duration_hours}h")
            logger.info(f"  API Workers: {self.config.api.workers}")
            
            if self.config.azure.storage_account:
                logger.info(f"  Azure Container: {self.config.azure.container_name}")
            
            if self.config.local_storage.data_path:
                logger.info(f"  Local Storage: {self.config.local_storage.data_path}")
            
            logger.info("Sensor Data Query Service started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            return False
    
    def run_forever(self):
        """Run the service until interrupted."""
        if not self.start():
            sys.exit(1)
        
        # Setup signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            self.run_server()
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        except Exception as e:
            logger.error(f"Service error: {e}")
            sys.exit(1)


def create_test_service():
    """Create service instance for testing."""
    service = QueryService()
    if service.initialize():
        return service.app
    return None


def main():
    """Main entry point."""
    logger.info("Starting Sensor Data Query Service...")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Startup time: {datetime.utcnow().isoformat()}")
    
    try:
        service = QueryService()
        service.run_forever()
    except Exception as e:
        logger.error(f"Service startup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
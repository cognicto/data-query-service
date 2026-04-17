"""
Simplified query engine using DuckDB only.
"""

import logging
import time
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from dataclasses import dataclass

from app.config import  AppConfig
from app.storage.storage import UnifiedStorageBackend

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Simple query result container."""
    data: pd.DataFrame
    execution_time_ms: float
    rows_returned: int
    tier_used: str
    truncated: bool
    metadata: Dict[str, Any]


class SimpleQueryEngine:
    """Simplified query engine using DuckDB for all operations."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.storage = UnifiedStorageBackend(config)
        self.stats = {
            'total_queries': 0,
            'total_execution_time_ms': 0,
            'average_response_time_ms': 0
        }
        
        logger.info(f"SimpleQueryEngine initialized with {config.storage_mode.value} storage")
    
    def query_sensor_data(self,
                         sensors: List[str],
                         start_time: datetime,
                         end_time: datetime,
                         asset_ids: List[str],
                         interval_ms: Optional[int] = None,
                         max_datapoints: Optional[int] = None,
                         aggregation_method: str = "avg") -> QueryResult:
        """Execute sensor data query."""
        
        start_exec_time = time.time()
        
        # Validate inputs
        self._validate_query_params(sensors, start_time, end_time, asset_ids)
        
        # Normalize timezone
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        # Set default max_datapoints
        if max_datapoints is None:
            max_datapoints = self.config.default_max_datapoints
        
        try:
            # Execute query through unified storage
            data = self.storage.query_sensor_data(
                sensors=sensors,
                start_time=start_time,
                end_time=end_time,
                asset_ids=asset_ids,
                interval_ms=interval_ms,
                max_datapoints=max_datapoints,
                aggregation_method=aggregation_method
            )
            
            # Get actual tier used (from storage backend)
            selected_tier = self.storage._select_data_tier(interval_ms)
            tier_used = selected_tier.value
            
            # Check if truncated
            truncated = max_datapoints and len(data) >= max_datapoints
            
            execution_time = (time.time() - start_exec_time) * 1000
            
            # Update stats
            self._update_stats(execution_time)
            
            # Convert timestamp column for consistency
            if not data.empty and 'timestamp_ms' in data.columns:
                data['timestamp'] = pd.to_datetime(data['timestamp_ms'], unit='ms')
                data = data.drop('timestamp_ms', axis=1)
            
            result = QueryResult(
                data=data,
                execution_time_ms=execution_time,
                rows_returned=len(data),
                tier_used=tier_used,
                truncated=truncated,
                metadata={
                    'storage_mode': self.config.storage_mode.value,
                    'query_type': 'aggregated' if interval_ms else 'raw',
                    'sensors_queried': len(sensors),
                    'assets_queried': len(asset_ids),
                    'time_range_hours': (end_time - start_time).total_seconds() / 3600
                }
            )
            
            logger.info(f"Query completed: {len(data)} rows in {execution_time:.1f}ms")
            return result
            
        except Exception as e:
            execution_time = (time.time() - start_exec_time) * 1000
            self._update_stats(execution_time)
            
            logger.error(f"Query failed after {execution_time:.1f}ms: {e}")
            
            # Return empty result
            return QueryResult(
                data=pd.DataFrame(),
                execution_time_ms=execution_time,
                rows_returned=0,
                tier_used="error",
                truncated=False,
                metadata={'error': str(e)}
            )
    
    def _validate_query_params(self, sensors: List[str], start_time: datetime,
                              end_time: datetime, asset_ids: List[str]):
        """Validate query parameters."""
        if not sensors:
            raise ValueError("At least one sensor must be specified")
        
        if not asset_ids:
            raise ValueError("At least one asset_id must be specified") 
            
        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")
        
        duration_hours = (end_time - start_time).total_seconds() / 3600
        if duration_hours > self.config.max_query_duration_hours:
            raise ValueError(f"Query duration ({duration_hours:.1f}h) exceeds maximum ({self.config.max_query_duration_hours}h)")
    
    def _update_stats(self, execution_time_ms: float):
        """Update query statistics."""
        self.stats['total_queries'] += 1
        self.stats['total_execution_time_ms'] += execution_time_ms
        self.stats['average_response_time_ms'] = (
            self.stats['total_execution_time_ms'] / self.stats['total_queries']
        )
    
    def get_available_assets(self) -> List[str]:
        """Get available assets."""
        try:
            return self.storage.get_available_assets()
        except Exception as e:
            logger.error(f"Failed to get assets: {e}")
            return []
    
    def get_available_sensors(self, asset_id: Optional[str] = None) -> List[str]:
        """Get available sensors."""
        try:
            return self.storage.get_available_sensors(asset_id)
        except Exception as e:
            logger.error(f"Failed to get sensors: {e}")
            return []
    
    def health_check(self) -> Dict[str, Any]:
        """Check engine health."""
        storage_health = self.storage.health_check()
        
        return {
            'engine_healthy': storage_health.get('healthy', False),
            'storage_backend': storage_health,
            'statistics': self.stats.copy(),
            'configuration': {
                'storage_mode': self.config.storage_mode.value,
                'max_query_duration_hours': self.config.max_query_duration_hours,
                'default_max_datapoints': self.config.default_max_datapoints
            }
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return self.stats.copy()
    
    def close(self):
        """Clean up resources."""
        self.storage.close()
        logger.info("SimpleQueryEngine closed")
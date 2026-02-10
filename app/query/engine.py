"""
Smart query engine with multi-tier optimization and caching.
"""

import logging
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import concurrent.futures
from threading import Lock

from app.config import AppConfig, StorageMode, AggregationMethod, get_tier_for_query, calculate_optimal_interval
from app.storage.base import SensorDataReader
from app.storage.azure_storage import AzureStorageBackend, AzureAggregatedReader
from app.storage.local_storage import LocalStorageBackend, LocalAggregatedReader
from app.cache.cache_manager import SmartCacheManager
from app.aggregation.aggregator import SmartAggregationEngine

logger = logging.getLogger(__name__)


class QueryResult:
    """Container for query results with metadata."""
    
    def __init__(self, data: pd.DataFrame, metadata: Dict):
        self.data = data
        self.metadata = metadata
        self.truncated = metadata.get('truncated', False)
        self.actual_end_time = metadata.get('actual_end_time')
        self.tier_used = metadata.get('tier_used', 'unknown')
        self.cache_hit = metadata.get('cache_hit', False)
        self.execution_time_ms = metadata.get('execution_time_ms', 0)


class SmartQueryEngine:
    """Smart query engine with automatic tier selection and optimization."""
    
    def __init__(self, config: AppConfig):
        """Initialize smart query engine."""
        self.config = config
        self._init_storage_backends()
        self.cache_manager = SmartCacheManager(config.cache)
        self.aggregation_engine = SmartAggregationEngine()
        
        # Query statistics
        self.stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'tier_usage': {'raw': 0, 'aggregated': 0, 'daily': 0},
            'total_execution_time_ms': 0
        }
        self._stats_lock = Lock()
        
        logger.info(f"Initialized smart query engine with {config.storage_mode} storage")
    
    def _init_storage_backends(self):
        """Initialize storage backends based on configuration."""
        self.azure_backend = None
        self.local_backend = None
        self.azure_reader = None
        self.local_reader = None
        
        # Initialize Azure backend
        if self.config.storage_mode in [StorageMode.AZURE, StorageMode.HYBRID]:
            try:
                self.azure_backend = AzureStorageBackend(self.config.azure)
                self.azure_reader = AzureAggregatedReader(self.azure_backend)
                logger.info("Azure storage backend initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Azure backend: {e}")
                if self.config.storage_mode == StorageMode.AZURE:
                    raise
        
        # Initialize local backend
        if self.config.storage_mode in [StorageMode.LOCAL, StorageMode.HYBRID]:
            try:
                self.local_backend = LocalStorageBackend(self.config.local_storage)
                self.local_reader = LocalAggregatedReader(self.local_backend)
                logger.info("Local storage backend initialized")
            except Exception as e:
                logger.error(f"Failed to initialize local backend: {e}")
                if self.config.storage_mode == StorageMode.LOCAL:
                    raise
    
    def query_sensor_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                         asset_ids: Optional[List[str]] = None,
                         interval_ms: Optional[int] = None,
                         max_datapoints: Optional[int] = None,
                         aggregation: Optional[str] = None) -> QueryResult:
        """Execute smart sensor data query with automatic optimization."""
        import time
        start_exec_time = time.time()
        
        # Validate and normalize parameters
        query_params = self._validate_query_params(
            sensors, start_time, end_time, asset_ids, interval_ms, max_datapoints, aggregation
        )
        
        # Check cache first
        cached_result = self.cache_manager.get_cached_result(
            query_params['sensors'], query_params['start_time'], query_params['end_time'],
            query_params['asset_ids'], query_params['interval_ms'], query_params['aggregation'],
            query_params['max_datapoints']
        )
        
        if cached_result is not None:
            execution_time = (time.time() - start_exec_time) * 1000
            self._update_stats(cache_hit=True, execution_time_ms=execution_time)
            
            return QueryResult(cached_result, {
                'cache_hit': True,
                'tier_used': 'cache',
                'execution_time_ms': execution_time,
                'truncated': False
            })
        
        # Determine optimal tier and execution strategy
        duration_hours = (query_params['end_time'] - query_params['start_time']).total_seconds() / 3600
        optimal_tier = get_tier_for_query(duration_hours, self.config.tiers)
        
        try:
            # Execute query using optimal tier
            data, tier_used = self._execute_tiered_query(query_params, optimal_tier)
            
            # Apply aggregation and downsampling if needed
            if not data.empty:
                data = self._post_process_data(data, query_params, duration_hours)
            
            # Check if we need to truncate data
            truncated = False
            actual_end_time = query_params['end_time']
            
            if len(data) > query_params['max_datapoints']:
                # Downsample to max datapoints
                data = self.aggregation_engine.aggregator.downsample_to_max_points(
                    data, query_params['max_datapoints'],
                    AggregationMethod(query_params['aggregation'])
                )
                
                truncated = True
                if not data.empty and 'timestamp' in data.columns:
                    actual_end_time = data['timestamp'].max()
            
            # Cache the result
            if not data.empty:
                self.cache_manager.cache_result(
                    data, query_params['sensors'], query_params['start_time'],
                    query_params['end_time'], query_params['asset_ids'],
                    query_params['interval_ms'], query_params['aggregation'],
                    query_params['max_datapoints']
                )
            
            execution_time = (time.time() - start_exec_time) * 1000
            self._update_stats(tier_used=tier_used, execution_time_ms=execution_time)
            
            return QueryResult(data, {
                'cache_hit': False,
                'tier_used': tier_used,
                'execution_time_ms': execution_time,
                'truncated': truncated,
                'actual_end_time': actual_end_time,
                'original_datapoints': len(data) if not truncated else query_params['max_datapoints']
            })
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            execution_time = (time.time() - start_exec_time) * 1000
            self._update_stats(execution_time_ms=execution_time)
            
            return QueryResult(pd.DataFrame(), {
                'cache_hit': False,
                'tier_used': 'error',
                'execution_time_ms': execution_time,
                'error': str(e),
                'truncated': False
            })
    
    def _validate_query_params(self, sensors: List[str], start_time: datetime, end_time: datetime,
                              asset_ids: Optional[List[str]], interval_ms: Optional[int],
                              max_datapoints: Optional[int], aggregation: Optional[str]) -> Dict:
        """Validate and normalize query parameters."""
        # Validate time range
        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")
        
        duration = end_time - start_time
        max_duration = timedelta(hours=self.config.query.max_query_duration_hours)
        
        if duration > max_duration:
            raise ValueError(f"Query duration exceeds maximum of {self.config.query.max_query_duration_hours} hours")
        
        # Validate sensors
        if not sensors:
            raise ValueError("At least one sensor must be specified")
        
        # Set defaults
        if interval_ms is None:
            duration_hours = duration.total_seconds() / 3600
            interval_ms = calculate_optimal_interval(duration_hours, max_datapoints or self.config.query.default_max_datapoints)
        
        if max_datapoints is None:
            max_datapoints = self.config.query.default_max_datapoints
        
        if max_datapoints > self.config.query.max_absolute_datapoints:
            max_datapoints = self.config.query.max_absolute_datapoints
        
        if aggregation is None:
            aggregation = AggregationMethod.AVG
        else:
            try:
                aggregation = AggregationMethod(aggregation.lower())
            except ValueError:
                aggregation = AggregationMethod.AVG
        
        return {
            'sensors': sensors,
            'start_time': start_time,
            'end_time': end_time,
            'asset_ids': asset_ids,
            'interval_ms': interval_ms,
            'max_datapoints': max_datapoints,
            'aggregation': aggregation
        }
    
    def _execute_tiered_query(self, params: Dict, preferred_tier: str) -> Tuple[pd.DataFrame, str]:
        """Execute query using tiered storage with fallback."""
        tier_methods = {
            'raw': self._query_raw_tier,
            'aggregated': self._query_aggregated_tier,
            'daily': self._query_daily_tier
        }
        
        # Try preferred tier first
        if preferred_tier in tier_methods:
            try:
                data = tier_methods[preferred_tier](params)
                if not data.empty:
                    return data, preferred_tier
                logger.info(f"No data found in {preferred_tier} tier, trying fallbacks")
            except Exception as e:
                logger.warning(f"Error querying {preferred_tier} tier: {e}")
        
        # Fallback to other tiers
        tier_order = ['raw', 'aggregated', 'daily']
        if preferred_tier in tier_order:
            tier_order.remove(preferred_tier)
        
        for tier in tier_order:
            try:
                data = tier_methods[tier](params)
                if not data.empty:
                    logger.info(f"Found data in {tier} tier (fallback)")
                    return data, tier
            except Exception as e:
                logger.warning(f"Error querying {tier} tier: {e}")
        
        # No data found in any tier
        return pd.DataFrame(), 'none'
    
    def _query_raw_tier(self, params: Dict) -> pd.DataFrame:
        """Query raw data tier."""
        results = []
        
        # Try Azure first, then local
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_raw_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure raw query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_raw_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local raw query failed: {e}")
        
        # Combine results if multiple sources
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
            return combined
        
        return pd.DataFrame()
    
    def _query_aggregated_tier(self, params: Dict) -> pd.DataFrame:
        """Query aggregated data tier."""
        results = []
        
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_aggregated_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure aggregated query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_aggregated_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local aggregated query failed: {e}")
        
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
            return combined
        
        return pd.DataFrame()
    
    def _query_daily_tier(self, params: Dict) -> pd.DataFrame:
        """Query daily summary tier."""
        results = []
        
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_daily_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure daily query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_daily_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local daily query failed: {e}")
        
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
            return combined
        
        return pd.DataFrame()
    
    def _post_process_data(self, data: pd.DataFrame, params: Dict, duration_hours: float) -> pd.DataFrame:
        """Apply post-processing to query results."""
        if data.empty:
            return data
        
        # Apply smart aggregation if needed
        if self.config.query.enable_smart_aggregation:
            data = self.aggregation_engine.apply_smart_aggregation(
                data, params['interval_ms'], params['max_datapoints'], duration_hours
            )
        
        # Filter by time range (in case tier query returned extra data)
        if 'timestamp' in data.columns:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            mask = (data['timestamp'] >= params['start_time']) & (data['timestamp'] < params['end_time'])
            data = data[mask]
        
        # Filter by sensors (in case tier query returned extra sensors)
        if 'sensor_name' in data.columns:
            data = data[data['sensor_name'].isin(params['sensors'])]
        
        # Filter by assets if specified
        if params['asset_ids'] and 'asset_id' in data.columns:
            data = data[data['asset_id'].isin(params['asset_ids'])]
        
        return data
    
    def _update_stats(self, cache_hit: bool = False, tier_used: Optional[str] = None, execution_time_ms: float = 0):
        """Update query statistics."""
        with self._stats_lock:
            self.stats['total_queries'] += 1
            self.stats['total_execution_time_ms'] += execution_time_ms
            
            if cache_hit:
                self.stats['cache_hits'] += 1
            
            if tier_used and tier_used in self.stats['tier_usage']:
                self.stats['tier_usage'][tier_used] += 1
    
    def get_available_sensors(self, asset_id: Optional[str] = None) -> List[str]:
        """Get list of available sensors."""
        sensors = set()
        
        # Get from Azure
        if self.azure_backend:
            try:
                reader = SensorDataReader(self.azure_backend)
                azure_sensors = reader.get_available_sensors(asset_id)
                sensors.update(azure_sensors)
            except Exception as e:
                logger.warning(f"Failed to get Azure sensors: {e}")
        
        # Get from local storage
        if self.local_backend:
            try:
                reader = SensorDataReader(self.local_backend)
                local_sensors = reader.get_available_sensors(asset_id)
                sensors.update(local_sensors)
            except Exception as e:
                logger.warning(f"Failed to get local sensors: {e}")
        
        return sorted(list(sensors))
    
    def get_available_assets(self) -> List[str]:
        """Get list of available assets."""
        assets = set()
        
        # Get from Azure
        if self.azure_backend:
            try:
                reader = SensorDataReader(self.azure_backend)
                azure_assets = reader.get_available_assets()
                assets.update(azure_assets)
            except Exception as e:
                logger.warning(f"Failed to get Azure assets: {e}")
        
        # Get from local storage
        if self.local_backend:
            try:
                reader = SensorDataReader(self.local_backend)
                local_assets = reader.get_available_assets()
                assets.update(local_assets)
            except Exception as e:
                logger.warning(f"Failed to get local assets: {e}")
        
        return sorted(list(assets))
    
    def get_time_range(self, sensors: List[str], asset_ids: Optional[List[str]] = None) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get available time range for sensors."""
        min_time = None
        max_time = None
        
        # Check Azure
        if self.azure_backend:
            try:
                reader = SensorDataReader(self.azure_backend)
                azure_min, azure_max = reader.get_time_range(sensors, asset_ids)
                if azure_min:
                    min_time = azure_min if min_time is None else min(min_time, azure_min)
                if azure_max:
                    max_time = azure_max if max_time is None else max(max_time, azure_max)
            except Exception as e:
                logger.warning(f"Failed to get Azure time range: {e}")
        
        # Check local storage
        if self.local_backend:
            try:
                reader = SensorDataReader(self.local_backend)
                local_min, local_max = reader.get_time_range(sensors, asset_ids)
                if local_min:
                    min_time = local_min if min_time is None else min(min_time, local_min)
                if local_max:
                    max_time = local_max if max_time is None else max(max_time, local_max)
            except Exception as e:
                logger.warning(f"Failed to get local time range: {e}")
        
        return min_time, max_time
    
    def get_query_stats(self) -> Dict:
        """Get query execution statistics."""
        with self._stats_lock:
            stats = self.stats.copy()
        
        # Add cache statistics
        cache_stats = self.cache_manager.get_cache_stats()
        
        # Calculate derived metrics
        avg_execution_time = 0
        if stats['total_queries'] > 0:
            avg_execution_time = stats['total_execution_time_ms'] / stats['total_queries']
        
        cache_hit_rate = 0
        if stats['total_queries'] > 0:
            cache_hit_rate = stats['cache_hits'] / stats['total_queries']
        
        return {
            **stats,
            'avg_execution_time_ms': avg_execution_time,
            'cache_hit_rate': cache_hit_rate,
            'cache_stats': cache_stats
        }
    
    def clear_cache(self):
        """Clear all caches."""
        self.cache_manager.clear_all()
        
        if self.azure_backend:
            self.azure_backend.clear_cache()
        
        if self.local_backend:
            self.local_backend.clear_cache()
        
        logger.info("Cleared all caches")
    
    def health_check(self) -> Dict:
        """Perform comprehensive health check."""
        health = {
            'overall_healthy': True,
            'storage_backends': {},
            'cache_status': {},
            'query_stats': self.get_query_stats()
        }
        
        # Check Azure backend
        if self.azure_backend:
            azure_health = self.azure_backend.health_check()
            health['storage_backends']['azure'] = azure_health
            if not azure_health.get('healthy', False):
                health['overall_healthy'] = False
        
        # Check local backend
        if self.local_backend:
            local_health = self.local_backend.health_check()
            health['storage_backends']['local'] = local_health
            if not local_health.get('healthy', False) and self.config.storage_mode == StorageMode.LOCAL:
                health['overall_healthy'] = False
        
        # Check cache
        health['cache_status'] = self.cache_manager.get_cache_stats()
        
        return health
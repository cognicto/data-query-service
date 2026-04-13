"""
Smart query engine with multi-tier optimization and caching.
"""

import logging
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone
import concurrent.futures
from threading import Lock

from app.config import AppConfig, StorageMode, AggregationMethod, get_tier_for_query, calculate_optimal_interval
from app.storage.base import SensorDataReader
from app.storage.azure_storage import AzureStorageBackend, AzureAggregatedReader
from app.storage.local_storage import LocalStorageBackend, LocalAggregatedReader
from app.cache.cache_manager import SmartCacheManager
from app.aggregation.aggregator import SmartAggregationEngine

# DuckDB imports
try:
    from app.query.duckdb_engine import HybridQueryEngine
    from app.storage.duckdb_storage import HybridStorageReader
    from app.aggregation.duckdb_aggregator import EnhancedDataAggregator
    DUCKDB_AVAILABLE = True
except ImportError as e:
    logger.warning(f"DuckDB components not available: {e}")
    DUCKDB_AVAILABLE = False

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
        
        # Initialize aggregation engines
        self.aggregation_engine = SmartAggregationEngine()
        self.enhanced_aggregator = None
        self.hybrid_query_engine = None
        self.hybrid_storage_reader = None
        
        # Initialize DuckDB components if available and enabled
        self._init_duckdb_components()
        
        # Query statistics
        self.stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'tier_usage': {'raw': 0, 'aggregated': 0, 'hourly': 0, 'daily': 0},
            'total_execution_time_ms': 0,
            'duckdb_queries': 0,
            'pandas_queries': 0
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
    
    def _init_duckdb_components(self):
        """Initialize DuckDB components if available and enabled."""
        if not DUCKDB_AVAILABLE or not self.config.duckdb.enabled:
            logger.info("DuckDB disabled or not available, using pandas-only mode")
            return
        
        try:
            # Initialize enhanced aggregator
            self.enhanced_aggregator = EnhancedDataAggregator(self.config.duckdb)
            logger.info("Enhanced DuckDB aggregator initialized")
            
            # Initialize hybrid storage reader for local storage
            if self.local_backend:
                self.hybrid_storage_reader = HybridStorageReader(
                    self.config.local_storage, 
                    self.config.duckdb
                )
                logger.info("Hybrid storage reader initialized")
            
            # Initialize hybrid query engine
            primary_reader = self.local_reader if self.local_reader else self.azure_reader
            if primary_reader:
                self.hybrid_query_engine = HybridQueryEngine(self.config, primary_reader)
                logger.info("Hybrid DuckDB query engine initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB components: {e}")
            # Continue with pandas-only mode
            self.enhanced_aggregator = None
            self.hybrid_query_engine = None
            self.hybrid_storage_reader = None
    
    def _normalize_dataframe_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure consistent data types across the DataFrame."""
        if df.empty:
            return df
        
        try:
            df = df.copy()
            
            # Ensure timestamp column is properly typed
            if 'timestamp' in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
                elif df['timestamp'].dt.tz is None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                else:
                    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
            
            # Ensure numeric columns are properly typed
            for col in df.columns:
                if col not in ['timestamp', 'sensor_name', 'asset_id', 'quality']:
                    if df[col].dtype == 'object':
                        # Try to convert to numeric
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
            
        except Exception as e:
            logger.warning(f"Error normalizing DataFrame types: {e}")
            return df
    
    def query_sensor_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                         asset_ids: Optional[List[str]] = None,
                         interval_ms: Optional[int] = None,
                         max_datapoints: Optional[int] = None,
                         aggregation: Optional[str] = None,
                         use_exact_interval: bool = False) -> QueryResult:
        """Execute smart sensor data query with automatic optimization."""
        import time
        start_exec_time = time.time()
        
        # Validate and normalize parameters
        query_params = self._validate_query_params(
            sensors, start_time, end_time, asset_ids, interval_ms, max_datapoints, aggregation
        )
        query_params['use_exact_interval'] = use_exact_interval
        
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
            # Execute query using optimal tier with new file structure support
            data, tier_used = self._execute_tiered_query_v2(query_params, optimal_tier)
            
            # Apply aggregation and downsampling if needed
            if not data.empty:
                # First normalize data types for consistent processing
                data = self._normalize_dataframe_types(data)
                data = self._post_process_data_hybrid(data, query_params, duration_hours, tier_used)
            
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
            logger.debug(f"Query parameters: {query_params}")
            logger.debug(f"Stack trace:", exc_info=True)
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
        
        # Fallback to other tiers (ordered from most to least granular)
        tier_order = ['raw', 'aggregated', 'hourly', 'daily']
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
    
    def _execute_tiered_query_v2(self, params: Dict, preferred_tier: str) -> Tuple[pd.DataFrame, str]:
        """Execute query using tiered storage with new file structure support."""
        # Try to use the new optimized readers that understand the file structure
        tier_methods = {
            'raw': self._query_raw_tier_v2,
            'aggregated': self._query_aggregated_tier_v2,
            'hourly': self._query_hourly_tier_v2,
            'daily': self._query_daily_tier_v2
        }
        
        # Try preferred tier first
        if preferred_tier in tier_methods:
            try:
                data = tier_methods[preferred_tier](params)
                if not data.empty:
                    logger.debug(f"Successfully queried {preferred_tier} tier, got {len(data)} rows")
                    return data, preferred_tier
                logger.info(f"No data found in {preferred_tier} tier, trying fallbacks")
            except Exception as e:
                logger.warning(f"Error querying {preferred_tier} tier: {e}")
        
        # Fallback to other tiers (ordered from most to least granular)
        tier_order = ['raw', 'aggregated', 'hourly', 'daily']
        if preferred_tier in tier_order:
            tier_order.remove(preferred_tier)
        
        for tier in tier_order:
            try:
                data = tier_methods[tier](params)
                if not data.empty:
                    logger.info(f"Found data in {tier} tier (fallback), got {len(data)} rows")
                    return data, tier
            except Exception as e:
                logger.warning(f"Error querying {tier} tier: {e}")
        
        # No data found in any tier
        logger.warning("No data found in any tier")
        return pd.DataFrame(), 'none'
    
    def _query_raw_tier_v2(self, params: Dict) -> pd.DataFrame:
        """Query raw data tier with new file structure support."""
        results = []
        
        # Try DuckDB hybrid engine first for local storage
        if self.hybrid_query_engine and self.config.storage_mode in [StorageMode.LOCAL, StorageMode.HYBRID]:
            try:
                logger.debug("Using DuckDB hybrid engine for raw data query")
                for asset_id in params['asset_ids']:
                    for sensor in params['sensors']:
                        hybrid_data = self.hybrid_query_engine.query_sensor_data(
                            asset_id, sensor, params['start_time'], params['end_time'],
                            params['interval_ms'], params['max_datapoints'],
                            AggregationMethod(params['aggregation'])
                        )
                        if not hybrid_data.empty:
                            results.append(hybrid_data)
                            with self._stats_lock:
                                self.stats['duckdb_queries'] += 1
                
                if results:
                    combined_data = pd.concat(results, ignore_index=True)
                    combined_data = combined_data.sort_values('timestamp').reset_index(drop=True)
                    logger.info(f"DuckDB hybrid engine returned {len(combined_data)} rows")
                    return combined_data
            except Exception as e:
                logger.warning(f"DuckDB hybrid engine failed, falling back: {e}")
        
        # Try Azure first, then local
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_raw_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    logger.debug(f"Azure raw tier returned {len(azure_data)} rows")
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure raw query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_raw_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    logger.debug(f"Local raw tier returned {len(local_data)} rows")
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local raw query failed: {e}")
        
        # Combine results if multiple sources
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
                logger.debug(f"Combined raw tier data: {len(combined)} rows")
            return combined
        
        return pd.DataFrame()
    
    def _query_aggregated_tier_v2(self, params: Dict) -> pd.DataFrame:
        """Query aggregated data tier with quality metrics support."""
        results = []
        
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_aggregated_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    logger.debug(f"Azure aggregated tier returned {len(azure_data)} rows")
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure aggregated query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_aggregated_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    logger.debug(f"Local aggregated tier returned {len(local_data)} rows")
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local aggregated query failed: {e}")
        
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
                logger.debug(f"Combined aggregated tier data: {len(combined)} rows")
            return combined
        
        return pd.DataFrame()
    
    def _query_hourly_tier_v2(self, params: Dict) -> pd.DataFrame:
        """Query hourly aggregated data tier."""
        results = []
        
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_hourly_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    logger.debug(f"Azure hourly tier returned {len(azure_data)} rows")
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure hourly query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_hourly_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    logger.debug(f"Local hourly tier returned {len(local_data)} rows")
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local hourly query failed: {e}")
        
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
                logger.debug(f"Combined hourly tier data: {len(combined)} rows")
            return combined
        
        return pd.DataFrame()
    
    def _query_daily_tier_v2(self, params: Dict) -> pd.DataFrame:
        """Query daily summary tier with enhanced quality metrics."""
        results = []
        
        if self.azure_reader:
            try:
                azure_data = self.azure_reader.read_daily_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not azure_data.empty:
                    logger.debug(f"Azure daily tier returned {len(azure_data)} rows")
                    results.append(azure_data)
            except Exception as e:
                logger.warning(f"Azure daily query failed: {e}")
        
        if self.local_reader and (not results or self.config.storage_mode == StorageMode.HYBRID):
            try:
                local_data = self.local_reader.read_daily_data(
                    params['sensors'], params['start_time'], params['end_time'], params['asset_ids']
                )
                if not local_data.empty:
                    logger.debug(f"Local daily tier returned {len(local_data)} rows")
                    results.append(local_data)
            except Exception as e:
                logger.warning(f"Local daily query failed: {e}")
        
        if results:
            combined = pd.concat(results, ignore_index=True)
            if 'timestamp' in combined.columns:
                combined = combined.sort_values('timestamp').drop_duplicates()
                logger.debug(f"Combined daily tier data: {len(combined)} rows")
            return combined
        
        return pd.DataFrame()
    
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
            
            # Simple timezone-aware comparison - convert all to UTC for comparison
            start_time = params['start_time']
            end_time = params['end_time']
            
            # Convert timestamps to UTC for consistent comparison
            if data['timestamp'].dt.tz is None:
                # Data is timezone-naive, assume UTC
                data_timestamps = data['timestamp'].dt.tz_localize('UTC')
            else:
                # Data has timezone, convert to UTC
                data_timestamps = data['timestamp'].dt.tz_convert('UTC')
            
            # Convert params to UTC
            if start_time.tzinfo is None:
                start_utc = start_time.replace(tzinfo=timezone.utc)
                end_utc = end_time.replace(tzinfo=timezone.utc)
            else:
                start_utc = start_time.astimezone(timezone.utc)
                end_utc = end_time.astimezone(timezone.utc)
            
            # Apply time filter
            mask = (data_timestamps >= start_utc) & (data_timestamps < end_utc)
            data = data[mask]
        
        # Filter by sensors (in case tier query returned extra sensors)
        if 'sensor_name' in data.columns:
            data = data[data['sensor_name'].isin(params['sensors'])]
        
        # Filter by assets if specified
        if params['asset_ids'] and 'asset_id' in data.columns:
            data = data[data['asset_id'].isin(params['asset_ids'])]
        
        return data
    
    def _post_process_data_hybrid(self, data: pd.DataFrame, params: Dict, duration_hours: float, tier_used: str) -> pd.DataFrame:
        """Apply hybrid post-processing using DuckDB or pandas based on data characteristics."""
        if data.empty:
            return data
        
        try:
            # Decide whether to use DuckDB enhanced aggregator
            use_duckdb = (
                self.enhanced_aggregator and
                self.config.duckdb.enabled and
                len(data) > self.config.duckdb.min_datapoints_threshold and
                tier_used != 'cache'  # Don't re-process cached data
            )
            
            # Apply aggregation if needed
            if params['interval_ms'] > 1000:
                if params.get('use_exact_interval', False):
                    # Use exact interval aggregation for aggregated API requests
                    logger.debug(f"Applying exact interval aggregation: {params['interval_ms']}ms")
                    if use_duckdb:
                        logger.debug("Using DuckDB for exact interval aggregation")
                        data = self.enhanced_aggregator.exact_interval_aggregate(
                            data, params['interval_ms'], AggregationMethod(params['aggregation']), use_duckdb=True
                        )
                        with self._stats_lock:
                            self.stats['duckdb_queries'] += 1
                    else:
                        logger.debug("Using pandas for exact interval aggregation")
                        data = self.aggregation_engine.apply_exact_interval_aggregation(
                            data, params['interval_ms'], AggregationMethod(params['aggregation']), params['max_datapoints']
                        )
                        with self._stats_lock:
                            self.stats['pandas_queries'] += 1
                elif self.config.query.enable_smart_aggregation:
                    # Use smart aggregation for other queries (with optimization)
                    logger.debug("Applying smart aggregation with optimization")
                    if use_duckdb:
                        logger.debug("Using DuckDB enhanced aggregator for post-processing")
                        data = self.enhanced_aggregator.smart_aggregate(
                            data, params['interval_ms'], AggregationMethod(params['aggregation']), use_duckdb=True
                        )
                        with self._stats_lock:
                            self.stats['duckdb_queries'] += 1
                    else:
                        logger.debug("Using pandas aggregator for post-processing")
                        data = self.aggregation_engine.apply_smart_aggregation(
                            data, params['interval_ms'], params['max_datapoints'], duration_hours
                        )
                        with self._stats_lock:
                            self.stats['pandas_queries'] += 1
            
            # Apply time range filtering
            if 'timestamp' in data.columns:
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                
                # Simple timezone-aware comparison - convert all to UTC for comparison
                start_time = params['start_time']
                end_time = params['end_time']
                
                # Convert timestamps to UTC for consistent comparison
                if data['timestamp'].dt.tz is None:
                    # Data is timezone-naive, assume UTC
                    data_timestamps = data['timestamp'].dt.tz_localize('UTC')
                else:
                    # Data has timezone, convert to UTC
                    data_timestamps = data['timestamp'].dt.tz_convert('UTC')
                
                # Convert params to UTC
                if start_time.tzinfo is None:
                    start_utc = start_time.replace(tzinfo=timezone.utc)
                    end_utc = end_time.replace(tzinfo=timezone.utc)
                else:
                    start_utc = start_time.astimezone(timezone.utc)
                    end_utc = end_time.astimezone(timezone.utc)
                
                # Apply time filter
                mask = (data_timestamps >= start_utc) & (data_timestamps < end_utc)
                data = data[mask]
            
            # Apply sensor filtering
            if 'sensor_name' in data.columns:
                data = data[data['sensor_name'].isin(params['sensors'])]
            
            # Apply asset filtering
            if params['asset_ids'] and 'asset_id' in data.columns:
                data = data[data['asset_id'].isin(params['asset_ids'])]
            
            return data
            
        except Exception as e:
            logger.error(f"Hybrid post-processing failed: {e}")
            # Fallback to standard processing
            return self._post_process_data(data, params, duration_hours)
    
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
        
        # Add DuckDB statistics if available
        duckdb_stats = {}
        if self.enhanced_aggregator:
            duckdb_stats = self.enhanced_aggregator.get_stats()
        
        if self.hybrid_query_engine:
            duckdb_stats.update(self.hybrid_query_engine.get_performance_stats())
        
        if self.hybrid_storage_reader:
            duckdb_stats.update(self.hybrid_storage_reader.get_stats())
        
        # Calculate engine usage percentages
        total_engine_queries = stats.get('duckdb_queries', 0) + stats.get('pandas_queries', 0)
        duckdb_usage_rate = 0
        pandas_usage_rate = 0
        
        if total_engine_queries > 0:
            duckdb_usage_rate = stats.get('duckdb_queries', 0) / total_engine_queries
            pandas_usage_rate = stats.get('pandas_queries', 0) / total_engine_queries
        
        return {
            **stats,
            'avg_execution_time_ms': avg_execution_time,
            'cache_hit_rate': cache_hit_rate,
            'duckdb_usage_rate': duckdb_usage_rate,
            'pandas_usage_rate': pandas_usage_rate,
            'cache_stats': cache_stats,
            'duckdb_stats': duckdb_stats
        }
    
    def clear_cache(self):
        """Clear all caches."""
        self.cache_manager.clear_all()
        
        if self.azure_backend:
            self.azure_backend.clear_cache()
        
        if self.local_backend:
            self.local_backend.clear_cache()
        
        # Clear DuckDB caches if available
        if self.enhanced_aggregator:
            self.enhanced_aggregator.close()
            self.enhanced_aggregator = EnhancedDataAggregator(self.config.duckdb) if DUCKDB_AVAILABLE and self.config.duckdb.enabled else None
        
        if self.hybrid_storage_reader:
            self.hybrid_storage_reader.close()
            self.hybrid_storage_reader = HybridStorageReader(self.config.local_storage, self.config.duckdb) if DUCKDB_AVAILABLE and self.config.duckdb.enabled and self.local_backend else None
        
        logger.info("Cleared all caches")
    
    def close(self):
        """Close all connections and cleanup resources."""
        try:
            # Close DuckDB components
            if self.enhanced_aggregator:
                self.enhanced_aggregator.close()
            
            if self.hybrid_query_engine:
                self.hybrid_query_engine.close()
            
            if self.hybrid_storage_reader:
                self.hybrid_storage_reader.close()
            
            # Close other backends
            if self.azure_backend:
                self.azure_backend.close()
            
            if self.local_backend:
                self.local_backend.close()
            
            logger.info("SmartQueryEngine closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing SmartQueryEngine: {e}")
    
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
"""
DuckDB-powered query engine for high-performance analytics on time-series data.
"""

import logging
import duckdb
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import AppConfig, DuckDBConfig, AggregationMethod
from app.storage.base import SensorDataReader

logger = logging.getLogger(__name__)


class DuckDBQueryEngine:
    """High-performance query engine using DuckDB for columnar analytics."""
    
    def __init__(self, config: AppConfig, storage_reader: SensorDataReader):
        """Initialize DuckDB query engine."""
        self.config = config
        self.duckdb_config = config.duckdb
        self.storage_reader = storage_reader
        self.connection = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize DuckDB connection with optimized settings."""
        try:
            # Create in-memory DuckDB connection
            self.connection = duckdb.connect(':memory:')
            logger.debug("DuckDB connection created")
            
            # Configure DuckDB for optimal performance
            try:
                self.connection.execute(f"SET memory_limit='{self.duckdb_config.memory_limit}'")
                logger.debug("Memory limit set")
            except Exception as e:
                logger.warning(f"Failed to set memory limit: {e}")
            
            try:
                self.connection.execute(f"SET threads={self.duckdb_config.threads}")
                logger.debug("Thread count set")
            except Exception as e:
                logger.warning(f"Failed to set thread count: {e}")
            
            # Skip the potentially problematic max_memory setting for now
            # # Optimize for time-series workloads
            # # Use 80% of the configured memory limit
            # memory_mb = int(float(self.duckdb_config.memory_limit.replace('GB', '')) * 1024 * 0.8)
            # self.connection.execute(f"SET max_memory='{memory_mb}mb'")
            
            # Enable available optimizations (check if supported)
            try:
                self.connection.execute("SET enable_object_cache=true")
                logger.debug("Object cache enabled")
            except Exception as e:
                logger.debug(f"enable_object_cache not available: {e}")
            
            try:
                self.connection.execute("SET enable_optimizer=true")
                logger.debug("Optimizer enabled")
            except Exception as e:
                logger.debug(f"enable_optimizer not available: {e}")
            
            logger.info("DuckDB connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB: {e}")
            raise
    
    def query_sensor_data(self, asset_id: str, sensor_name: str, 
                         start_time: datetime, end_time: datetime,
                         interval_ms: Optional[int] = None,
                         max_datapoints: Optional[int] = None,
                         aggregation_method: AggregationMethod = AggregationMethod.AVG) -> pd.DataFrame:
        """
        Query sensor data using DuckDB for high-performance analytics.
        
        Args:
            asset_id: Asset identifier
            sensor_name: Sensor name
            start_time: Query start time
            end_time: Query end time
            interval_ms: Aggregation interval in milliseconds
            max_datapoints: Maximum number of data points to return
            aggregation_method: Aggregation method for downsampling
            
        Returns:
            DataFrame with queried sensor data
        """
        try:
            # Get file paths for the query
            file_paths = self._get_file_paths(asset_id, sensor_name, start_time, end_time)
            
            if not file_paths:
                logger.warning(f"No data files found for {asset_id}/{sensor_name}")
                return pd.DataFrame()
            
            # Use DuckDB to directly query Parquet files
            if len(file_paths) == 1:
                result_df = self._query_single_file(file_paths[0], start_time, end_time)
            else:
                result_df = self._query_multiple_files(file_paths, start_time, end_time)
            
            if result_df.empty:
                return result_df
            
            # Apply aggregation if needed
            if interval_ms and interval_ms > 1000:
                result_df = self._aggregate_data(result_df, interval_ms, aggregation_method)
            
            # Apply downsampling if needed
            if max_datapoints and len(result_df) > max_datapoints:
                result_df = self._downsample_data(result_df, max_datapoints, aggregation_method)
            
            logger.info(f"DuckDB query returned {len(result_df)} rows for {asset_id}/{sensor_name}")
            return result_df
            
        except Exception as e:
            logger.error(f"DuckDB query failed: {e}")
            # Fallback to storage reader
            logger.info("Falling back to pandas-based query")
            return self._fallback_query(asset_id, sensor_name, start_time, end_time, 
                                      interval_ms, max_datapoints, aggregation_method)
    
    def _get_file_paths(self, asset_id: str, sensor_name: str, 
                       start_time: datetime, end_time: datetime) -> List[str]:
        """Get list of Parquet files covering the time range."""
        file_paths = []
        
        # Generate file paths based on storage structure
        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        
        while current_time <= end_time:
            # Format: /data/asset_id/yyyy/mm/dd/hh/sensor_YYYYMMDD_HH.parquet
            file_path = (
                f"{self.config.local_storage.data_path}/"
                f"{asset_id}/"
                f"{current_time.year:04d}/"
                f"{current_time.month:02d}/"
                f"{current_time.day:02d}/"
                f"{current_time.hour:02d}/"
                f"{sensor_name}_{current_time.strftime('%Y%m%d_%H')}.parquet"
            )
            
            # Check if file exists
            if Path(file_path).exists():
                file_paths.append(file_path)
            
            current_time += timedelta(hours=1)
        
        return file_paths
    
    def _query_single_file(self, file_path: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Query a single Parquet file using DuckDB."""
        try:
            # Convert timestamps to ISO format for DuckDB
            # Ensure timezone is handled properly - convert to UTC if timezone-aware
            if start_time.tzinfo is not None:
                start_time_utc = start_time.astimezone(timezone.utc).replace(tzinfo=None)
                start_str = start_time_utc.isoformat()
            else:
                start_str = start_time.isoformat()
                
            if end_time.tzinfo is not None:
                end_time_utc = end_time.astimezone(timezone.utc).replace(tzinfo=None)
                end_str = end_time_utc.isoformat()
            else:
                end_str = end_time.isoformat()
            
            query = f"""
                SELECT timestamp, value
                FROM read_parquet('{file_path}')
                WHERE timestamp >= '{start_str}'
                  AND timestamp <= '{end_str}'
                ORDER BY timestamp
            """
            
            result = self.connection.execute(query).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error querying single file {file_path}: {e}")
            return pd.DataFrame()
    
    def _query_multiple_files(self, file_paths: List[str], 
                            start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Query multiple Parquet files using DuckDB UNION ALL."""
        try:
            # Convert timestamps to ISO format for DuckDB
            # Ensure timezone is handled properly - convert to UTC if timezone-aware
            if start_time.tzinfo is not None:
                start_time_utc = start_time.astimezone(timezone.utc).replace(tzinfo=None)
                start_str = start_time_utc.isoformat()
            else:
                start_str = start_time.isoformat()
                
            if end_time.tzinfo is not None:
                end_time_utc = end_time.astimezone(timezone.utc).replace(tzinfo=None)
                end_str = end_time_utc.isoformat()
            else:
                end_str = end_time.isoformat()
            
            # Build UNION ALL query for multiple files
            file_queries = []
            for file_path in file_paths:
                file_query = f"SELECT timestamp, value FROM read_parquet('{file_path}')"
                file_queries.append(file_query)
            
            union_query = " UNION ALL ".join(file_queries)
            
            query = f"""
                WITH combined_data AS ({union_query})
                SELECT timestamp, value
                FROM combined_data
                WHERE timestamp >= '{start_str}'
                  AND timestamp <= '{end_str}'
                ORDER BY timestamp
            """
            
            result = self.connection.execute(query).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error querying multiple files: {e}")
            return pd.DataFrame()
    
    def _aggregate_data(self, df: pd.DataFrame, interval_ms: int, 
                       method: AggregationMethod) -> pd.DataFrame:
        """Aggregate data using DuckDB SQL."""
        try:
            # Create temporary table from DataFrame
            self.connection.register('temp_data', df)
            
            # Convert interval to DuckDB interval
            interval_seconds = interval_ms / 1000
            
            # Choose aggregation function
            agg_func = self._get_duckdb_aggregation_function(method)
            
            query = f"""
                SELECT 
                    time_bucket(INTERVAL '{interval_seconds} seconds', timestamp) as timestamp,
                    {agg_func}(value) as value
                FROM temp_data
                GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)
                ORDER BY timestamp
            """
            
            result = self.connection.execute(query).fetchdf()
            
            # Clean up temporary table
            self.connection.unregister('temp_data')
            
            return result
            
        except Exception as e:
            logger.error(f"Error in DuckDB aggregation: {e}")
            # Fallback to pandas aggregation
            return self._pandas_aggregate_fallback(df, interval_ms, method)
    
    def _get_duckdb_aggregation_function(self, method: AggregationMethod) -> str:
        """Get DuckDB aggregation function for the given method."""
        method_map = {
            AggregationMethod.AVG: "avg",
            AggregationMethod.MIN: "min",
            AggregationMethod.MAX: "max",
            AggregationMethod.SUM: "sum",
            AggregationMethod.COUNT: "count",
            AggregationMethod.FIRST: "first",
            AggregationMethod.LAST: "last"
        }
        return method_map.get(method, "avg")
    
    def _downsample_data(self, df: pd.DataFrame, max_datapoints: int, 
                        method: AggregationMethod) -> pd.DataFrame:
        """Downsample data to max_datapoints using DuckDB."""
        try:
            if len(df) <= max_datapoints:
                return df
            
            # Create temporary table
            self.connection.register('temp_data', df)
            
            # Calculate step size for uniform sampling
            step_size = len(df) // max_datapoints
            
            if method == AggregationMethod.AVG and step_size > 1:
                # Use window function for averaging
                query = f"""
                    SELECT 
                        timestamp,
                        avg(value) OVER (
                            ORDER BY timestamp 
                            ROWS BETWEEN {step_size//2} PRECEDING AND {step_size//2} FOLLOWING
                        ) as value,
                        row_number() OVER (ORDER BY timestamp) as rn
                    FROM temp_data
                    WHERE (row_number() OVER (ORDER BY timestamp) - 1) % {step_size} = 0
                    LIMIT {max_datapoints}
                """
            else:
                # Simple uniform sampling
                query = f"""
                    SELECT timestamp, value
                    FROM (
                        SELECT *, row_number() OVER (ORDER BY timestamp) as rn
                        FROM temp_data
                    ) numbered
                    WHERE (rn - 1) % {step_size} = 0
                    LIMIT {max_datapoints}
                """
            
            result = self.connection.execute(query).fetchdf()
            
            # Clean up temporary table
            self.connection.unregister('temp_data')
            
            return result.drop(columns=['rn'], errors='ignore')
            
        except Exception as e:
            logger.error(f"Error in DuckDB downsampling: {e}")
            # Fallback to simple pandas sampling
            step = len(df) // max_datapoints
            return df.iloc[::step].head(max_datapoints)
    
    def _pandas_aggregate_fallback(self, df: pd.DataFrame, interval_ms: int, 
                                 method: AggregationMethod) -> pd.DataFrame:
        """Fallback aggregation using pandas."""
        try:
            if df.empty:
                return df
            
            # Ensure timestamp is datetime
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Create time buckets
            interval_str = f'{interval_ms}ms'
            df['time_bucket'] = df['timestamp'].dt.floor(interval_str)
            
            # Apply aggregation
            if method == AggregationMethod.AVG:
                result = df.groupby('time_bucket')['value'].mean().reset_index()
            elif method == AggregationMethod.MIN:
                result = df.groupby('time_bucket')['value'].min().reset_index()
            elif method == AggregationMethod.MAX:
                result = df.groupby('time_bucket')['value'].max().reset_index()
            elif method == AggregationMethod.SUM:
                result = df.groupby('time_bucket')['value'].sum().reset_index()
            elif method == AggregationMethod.COUNT:
                result = df.groupby('time_bucket')['value'].count().reset_index()
            elif method == AggregationMethod.FIRST:
                result = df.groupby('time_bucket')['value'].first().reset_index()
            elif method == AggregationMethod.LAST:
                result = df.groupby('time_bucket')['value'].last().reset_index()
            else:
                result = df.groupby('time_bucket')['value'].mean().reset_index()
            
            # Rename columns
            result = result.rename(columns={'time_bucket': 'timestamp'})
            
            return result
            
        except Exception as e:
            logger.error(f"Pandas aggregation fallback failed: {e}")
            return df
    
    def _fallback_query(self, asset_id: str, sensor_name: str, 
                       start_time: datetime, end_time: datetime,
                       interval_ms: Optional[int] = None,
                       max_datapoints: Optional[int] = None,
                       aggregation_method: AggregationMethod = AggregationMethod.AVG) -> pd.DataFrame:
        """Fallback to storage reader when DuckDB fails."""
        try:
            return self.storage_reader.read_sensor_data(
                asset_id, sensor_name, start_time, end_time,
                interval_ms, max_datapoints
            )
        except Exception as e:
            logger.error(f"Fallback query also failed: {e}")
            return pd.DataFrame()
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get DuckDB performance statistics."""
        try:
            stats = {}
            
            # Get memory usage
            memory_result = self.connection.execute("SELECT current_setting('memory_limit')").fetchone()
            if memory_result:
                stats['memory_limit'] = memory_result[0]
            
            # Get thread count
            thread_result = self.connection.execute("SELECT current_setting('threads')").fetchone()
            if thread_result:
                stats['threads'] = int(thread_result[0])
            
            # Get cache statistics if available
            try:
                cache_stats = self.connection.execute("SELECT * FROM duckdb_cache()").fetchdf()
                stats['cache_stats'] = cache_stats.to_dict('records')
            except:
                stats['cache_stats'] = []
            
            stats['connection_active'] = self.connection is not None
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting DuckDB stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close DuckDB connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                logger.info("DuckDB connection closed")
        except Exception as e:
            logger.error(f"Error closing DuckDB connection: {e}")
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.close()


class HybridQueryEngine:
    """Hybrid engine that chooses between DuckDB and pandas based on data size."""
    
    def __init__(self, config: AppConfig, storage_reader: SensorDataReader):
        """Initialize hybrid query engine."""
        self.config = config
        self.storage_reader = storage_reader
        self.duckdb_engine = None
        
        # Initialize DuckDB if enabled
        if config.duckdb.enabled:
            try:
                self.duckdb_engine = DuckDBQueryEngine(config, storage_reader)
                logger.info("Hybrid engine initialized with DuckDB support")
            except Exception as e:
                logger.error(f"Failed to initialize DuckDB engine: {e}")
                logger.info("Falling back to pandas-only mode")
        else:
            logger.info("DuckDB disabled, using pandas-only mode")
    
    def query_sensor_data(self, asset_id: str, sensor_name: str, 
                         start_time: datetime, end_time: datetime,
                         interval_ms: Optional[int] = None,
                         max_datapoints: Optional[int] = None,
                         aggregation_method: AggregationMethod = AggregationMethod.AVG) -> pd.DataFrame:
        """
        Query sensor data using optimal engine based on data characteristics.
        """
        try:
            # Estimate data size
            duration_hours = (end_time - start_time).total_seconds() / 3600
            estimated_points = duration_hours * 3600  # Assume 1 point per second
            
            # Use DuckDB for large queries if available
            if (self.duckdb_engine and 
                self.config.duckdb.enabled and 
                estimated_points > self.config.duckdb.min_datapoints_threshold):
                
                logger.debug(f"Using DuckDB engine for query with {estimated_points} estimated points")
                return self.duckdb_engine.query_sensor_data(
                    asset_id, sensor_name, start_time, end_time,
                    interval_ms, max_datapoints, aggregation_method
                )
            else:
                # Use pandas-based storage reader for smaller queries
                logger.debug(f"Using pandas engine for query with {estimated_points} estimated points")
                return self.storage_reader.read_sensor_data(
                    asset_id, sensor_name, start_time, end_time,
                    interval_ms, max_datapoints
                )
                
        except Exception as e:
            logger.error(f"Hybrid query failed: {e}")
            # Final fallback to basic storage reader
            return self.storage_reader.read_sensor_data(
                asset_id, sensor_name, start_time, end_time,
                interval_ms, max_datapoints
            )
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics from both engines."""
        stats = {
            'duckdb_enabled': self.config.duckdb.enabled,
            'duckdb_available': self.duckdb_engine is not None
        }
        
        if self.duckdb_engine:
            stats['duckdb_stats'] = self.duckdb_engine.get_performance_stats()
        
        return stats
    
    def close(self):
        """Close all connections."""
        if self.duckdb_engine:
            self.duckdb_engine.close()
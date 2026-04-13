"""
DuckDB-powered aggregation engine for high-performance time-series analytics.
"""

import logging
import duckdb
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta
from pathlib import Path

from app.config import AggregationMethod, DuckDBConfig

logger = logging.getLogger(__name__)


class DuckDBAnalyticsEngine:
    """High-performance analytics engine using DuckDB for complex aggregations."""
    
    def __init__(self, duckdb_config: DuckDBConfig):
        """Initialize DuckDB analytics engine."""
        self.config = duckdb_config
        self.connection = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize DuckDB connection with analytics optimizations."""
        try:
            # Create in-memory DuckDB connection
            self.connection = duckdb.connect(':memory:')
            
            # Configure for analytics workloads
            self.connection.execute(f"SET memory_limit='{self.config.memory_limit}'")
            self.connection.execute(f"SET threads={self.config.threads}")
            
            # Analytics-specific optimizations
            # Use 90% of the configured memory limit for analytics
            memory_mb = int(float(self.config.memory_limit.replace('GB', '')) * 1024 * 0.9)
            self.connection.execute(f"SET max_memory='{memory_mb}mb'")
            
            # Enable available optimizations (check if supported)
            try:
                self.connection.execute("SET enable_object_cache=true")
            except:
                logger.debug("enable_object_cache not available")
            
            try:
                self.connection.execute("SET enable_optimizer=true")
            except:
                logger.debug("enable_optimizer not available")
            
            # Install and load time-series extension if available
            try:
                self.connection.execute("INSTALL 'httpfs'")
                self.connection.execute("LOAD 'httpfs'")
            except:
                logger.debug("httpfs extension not available")
            
            logger.info("DuckDB analytics engine initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB analytics: {e}")
            raise
    
    def time_bucket_aggregate(self, data: Union[pd.DataFrame, List[str]], 
                             interval_seconds: int,
                             aggregation_method: AggregationMethod = AggregationMethod.AVG,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             group_by_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Perform time bucket aggregation using DuckDB's time_bucket function.
        
        Args:
            data: DataFrame or list of Parquet file paths
            interval_seconds: Bucket interval in seconds
            aggregation_method: Aggregation method to apply
            start_time: Optional start time filter
            end_time: Optional end time filter
            group_by_columns: Additional columns to group by
            
        Returns:
            Aggregated DataFrame
        """
        try:
            # Prepare data source
            if isinstance(data, pd.DataFrame):
                self.connection.register('source_data', data)
                data_source = "source_data"
            elif isinstance(data, list):
                # Multiple Parquet files
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in data if Path(path).exists()]
                if not file_queries:
                    return pd.DataFrame()
                
                union_query = " UNION ALL ".join(file_queries)
                data_source = f"({union_query})"
            else:
                raise ValueError("Data must be DataFrame or list of file paths")
            
            # Build aggregation function
            agg_func = self._get_aggregation_function(aggregation_method)
            
            # Build GROUP BY clause
            group_columns = ["time_bucket(INTERVAL '{} seconds', timestamp)".format(interval_seconds)]
            if group_by_columns:
                group_columns.extend(group_by_columns)
            
            # Build time bucketing expression based on interval
            if interval_seconds >= 3600:
                # For hour or larger intervals, use date_trunc
                trunc_unit = 'hour'
                if interval_seconds >= 86400:
                    trunc_unit = 'day'
                time_bucket_expr = f"date_trunc('{trunc_unit}', timestamp)"
            elif interval_seconds >= 60:
                # For minute intervals, use date_trunc
                time_bucket_expr = "date_trunc('minute', timestamp)"
            else:
                # For sub-minute intervals, use epoch-based bucketing
                time_bucket_expr = f"to_timestamp(floor(epoch(timestamp) / {interval_seconds}) * {interval_seconds})"
            
            # Build SELECT clause
            select_columns = [f"{time_bucket_expr} as timestamp"]
            
            if group_by_columns:
                select_columns.extend(group_by_columns)
            
            # Add aggregated value columns
            select_columns.append(f"{agg_func}(value) as value")
            
            # Build GROUP BY clause
            group_columns = [time_bucket_expr]
            if group_by_columns:
                group_columns.extend(group_by_columns)
            
            # Build WHERE clause
            where_conditions = []
            if start_time:
                where_conditions.append(f"timestamp >= '{start_time.isoformat()}'")
            if end_time:
                where_conditions.append(f"timestamp <= '{end_time.isoformat()}'")
            
            where_clause = ""
            if where_conditions:
                where_clause = f"WHERE {' AND '.join(where_conditions)}"
            
            # Build complete query
            query = f"""
                SELECT {', '.join(select_columns)}
                FROM {data_source}
                {where_clause}
                GROUP BY {', '.join(group_columns)}
                ORDER BY timestamp
            """
            
            logger.debug(f"Executing time bucket aggregation with {interval_seconds}s intervals")
            result = self.connection.execute(query).fetchdf()
            
            # Cleanup temporary table if we used one
            if isinstance(data, pd.DataFrame):
                self.connection.unregister('source_data')
            
            logger.info(f"Time bucket aggregation returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Time bucket aggregation failed: {e}")
            return pd.DataFrame()
    
    def rolling_window_aggregate(self, data: Union[pd.DataFrame, List[str]],
                               window_size: int,
                               aggregation_method: AggregationMethod = AggregationMethod.AVG,
                               step_size: Optional[int] = None) -> pd.DataFrame:
        """
        Perform rolling window aggregation using DuckDB window functions.
        
        Args:
            data: DataFrame or list of Parquet file paths
            window_size: Window size in number of rows
            aggregation_method: Aggregation method to apply
            step_size: Step size for sliding window (default: 1)
            
        Returns:
            Aggregated DataFrame with rolling statistics
        """
        try:
            # Prepare data source
            if isinstance(data, pd.DataFrame):
                self.connection.register('source_data', data)
                data_source = "source_data"
            elif isinstance(data, list):
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in data if Path(path).exists()]
                if not file_queries:
                    return pd.DataFrame()
                union_query = " UNION ALL ".join(file_queries)
                data_source = f"({union_query})"
            else:
                raise ValueError("Data must be DataFrame or list of file paths")
            
            # Build window function
            agg_func = self._get_aggregation_function(aggregation_method)
            window_spec = f"ROWS BETWEEN {window_size-1} PRECEDING AND CURRENT ROW"
            
            # Build query with window function
            query = f"""
                SELECT 
                    timestamp,
                    value as original_value,
                    {agg_func}(value) OVER (ORDER BY timestamp {window_spec}) as rolling_{aggregation_method.value},
                    row_number() OVER (ORDER BY timestamp) as row_num
                FROM {data_source}
                ORDER BY timestamp
            """
            
            # Apply step size if specified
            if step_size and step_size > 1:
                query = f"""
                    SELECT timestamp, original_value, rolling_{aggregation_method.value}
                    FROM ({query}) windowed
                    WHERE (row_num - 1) % {step_size} = 0
                """
            
            logger.debug(f"Executing rolling window aggregation with window size {window_size}")
            result = self.connection.execute(query).fetchdf()
            
            # Cleanup
            if isinstance(data, pd.DataFrame):
                self.connection.unregister('source_data')
            
            logger.info(f"Rolling window aggregation returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Rolling window aggregation failed: {e}")
            return pd.DataFrame()
    
    def multi_metric_aggregation(self, data: Union[pd.DataFrame, List[str]],
                               interval_seconds: int,
                               metrics: List[AggregationMethod] = None) -> pd.DataFrame:
        """
        Compute multiple aggregation metrics in a single pass.
        
        Args:
            data: DataFrame or list of Parquet file paths
            interval_seconds: Time bucket interval in seconds
            metrics: List of aggregation methods to compute
            
        Returns:
            DataFrame with multiple metric columns
        """
        try:
            if metrics is None:
                metrics = [AggregationMethod.AVG, AggregationMethod.MIN, AggregationMethod.MAX, AggregationMethod.COUNT]
            
            # Prepare data source
            if isinstance(data, pd.DataFrame):
                self.connection.register('source_data', data)
                data_source = "source_data"
            elif isinstance(data, list):
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in data if Path(path).exists()]
                if not file_queries:
                    return pd.DataFrame()
                union_query = " UNION ALL ".join(file_queries)
                data_source = f"({union_query})"
            else:
                raise ValueError("Data must be DataFrame or list of file paths")
            
            # Build SELECT clause with multiple aggregations
            select_columns = [
                f"time_bucket(INTERVAL '{interval_seconds} seconds', timestamp) as timestamp"
            ]
            
            for metric in metrics:
                agg_func = self._get_aggregation_function(metric)
                select_columns.append(f"{agg_func}(value) as {metric.value}")
            
            # Build query
            query = f"""
                SELECT {', '.join(select_columns)}
                FROM {data_source}
                GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)
                ORDER BY timestamp
            """
            
            logger.debug(f"Executing multi-metric aggregation with {len(metrics)} metrics")
            result = self.connection.execute(query).fetchdf()
            
            # Cleanup
            if isinstance(data, pd.DataFrame):
                self.connection.unregister('source_data')
            
            logger.info(f"Multi-metric aggregation returned {len(result)} rows with {len(metrics)} metrics")
            return result
            
        except Exception as e:
            logger.error(f"Multi-metric aggregation failed: {e}")
            return pd.DataFrame()
    
    def percentile_analysis(self, data: Union[pd.DataFrame, List[str]],
                          interval_seconds: int,
                          percentiles: List[float] = None) -> pd.DataFrame:
        """
        Compute percentile statistics using DuckDB's quantile functions.
        
        Args:
            data: DataFrame or list of Parquet file paths
            interval_seconds: Time bucket interval in seconds
            percentiles: List of percentiles to compute (0.0 to 1.0)
            
        Returns:
            DataFrame with percentile statistics
        """
        try:
            if percentiles is None:
                percentiles = [0.25, 0.5, 0.75, 0.95, 0.99]
            
            # Prepare data source
            if isinstance(data, pd.DataFrame):
                self.connection.register('source_data', data)
                data_source = "source_data"
            elif isinstance(data, list):
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in data if Path(path).exists()]
                if not file_queries:
                    return pd.DataFrame()
                union_query = " UNION ALL ".join(file_queries)
                data_source = f"({union_query})"
            else:
                raise ValueError("Data must be DataFrame or list of file paths")
            
            # Build SELECT clause with percentile functions
            select_columns = [
                f"time_bucket(INTERVAL '{interval_seconds} seconds', timestamp) as timestamp",
                "count(value) as count",
                "avg(value) as mean",
                "stddev(value) as stddev"
            ]
            
            for p in percentiles:
                select_columns.append(f"quantile(value, {p}) as p{int(p*100)}")
            
            # Build query
            query = f"""
                SELECT {', '.join(select_columns)}
                FROM {data_source}
                GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)
                ORDER BY timestamp
            """
            
            logger.debug(f"Executing percentile analysis with {len(percentiles)} percentiles")
            result = self.connection.execute(query).fetchdf()
            
            # Cleanup
            if isinstance(data, pd.DataFrame):
                self.connection.unregister('source_data')
            
            logger.info(f"Percentile analysis returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Percentile analysis failed: {e}")
            return pd.DataFrame()
    
    def anomaly_detection(self, data: Union[pd.DataFrame, List[str]],
                         window_size: int = 50,
                         z_threshold: float = 3.0) -> pd.DataFrame:
        """
        Detect anomalies using statistical z-score analysis.
        
        Args:
            data: DataFrame or list of Parquet file paths
            window_size: Rolling window size for statistics
            z_threshold: Z-score threshold for anomaly detection
            
        Returns:
            DataFrame with anomaly flags and z-scores
        """
        try:
            # Prepare data source
            if isinstance(data, pd.DataFrame):
                self.connection.register('source_data', data)
                data_source = "source_data"
            elif isinstance(data, list):
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in data if Path(path).exists()]
                if not file_queries:
                    return pd.DataFrame()
                union_query = " UNION ALL ".join(file_queries)
                data_source = f"({union_query})"
            else:
                raise ValueError("Data must be DataFrame or list of file paths")
            
            # Build query with rolling statistics and anomaly detection
            query = f"""
                WITH rolling_stats AS (
                    SELECT 
                        timestamp,
                        value,
                        avg(value) OVER (ORDER BY timestamp ROWS BETWEEN {window_size-1} PRECEDING AND CURRENT ROW) as rolling_mean,
                        stddev(value) OVER (ORDER BY timestamp ROWS BETWEEN {window_size-1} PRECEDING AND CURRENT ROW) as rolling_stddev
                    FROM {data_source}
                    ORDER BY timestamp
                ),
                anomaly_scores AS (
                    SELECT 
                        timestamp,
                        value,
                        rolling_mean,
                        rolling_stddev,
                        CASE 
                            WHEN rolling_stddev > 0 THEN abs(value - rolling_mean) / rolling_stddev
                            ELSE 0
                        END as z_score
                    FROM rolling_stats
                )
                SELECT 
                    timestamp,
                    value,
                    rolling_mean,
                    rolling_stddev,
                    z_score,
                    CASE WHEN z_score > {z_threshold} THEN true ELSE false END as is_anomaly
                FROM anomaly_scores
                ORDER BY timestamp
            """
            
            logger.debug(f"Executing anomaly detection with window size {window_size}")
            result = self.connection.execute(query).fetchdf()
            
            # Cleanup
            if isinstance(data, pd.DataFrame):
                self.connection.unregister('source_data')
            
            anomaly_count = result['is_anomaly'].sum() if 'is_anomaly' in result.columns else 0
            logger.info(f"Anomaly detection returned {len(result)} rows with {anomaly_count} anomalies")
            return result
            
        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            return pd.DataFrame()
    
    def _get_aggregation_function(self, method: AggregationMethod) -> str:
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
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get analytics engine performance statistics."""
        try:
            stats = {
                'connection_active': self.connection is not None,
                'memory_limit': self.config.memory_limit,
                'threads': self.config.threads
            }
            
            if self.connection:
                # Get current memory usage
                try:
                    memory_result = self.connection.execute("SELECT current_setting('memory_limit')").fetchone()
                    if memory_result:
                        stats['current_memory_limit'] = memory_result[0]
                except:
                    pass
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting analytics stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close DuckDB connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                logger.info("DuckDB analytics connection closed")
        except Exception as e:
            logger.error(f"Error closing DuckDB analytics: {e}")
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.close()


class EnhancedDataAggregator:
    """Enhanced aggregator that combines pandas and DuckDB capabilities."""
    
    def __init__(self, duckdb_config: DuckDBConfig):
        """Initialize enhanced aggregator."""
        self.duckdb_config = duckdb_config
        self.analytics_engine = None
        
        # Initialize DuckDB analytics if enabled
        if duckdb_config.enabled:
            try:
                self.analytics_engine = DuckDBAnalyticsEngine(duckdb_config)
                logger.info("Enhanced aggregator initialized with DuckDB analytics")
            except Exception as e:
                logger.error(f"Failed to initialize DuckDB analytics: {e}")
                logger.info("Falling back to pandas-only mode")
    
    def smart_aggregate(self, df: pd.DataFrame, 
                       interval_ms: int,
                       method: AggregationMethod = AggregationMethod.AVG,
                       use_duckdb: Optional[bool] = None) -> pd.DataFrame:
        """
        Smart aggregation that chooses optimal engine based on data characteristics.
        """
        try:
            # Auto-decide engine if not specified
            if use_duckdb is None:
                use_duckdb = (
                    self.analytics_engine and
                    self.duckdb_config.enabled and
                    len(df) > self.duckdb_config.min_datapoints_threshold
                )
            
            if use_duckdb and self.analytics_engine:
                logger.debug(f"Using DuckDB for aggregation of {len(df)} rows")
                interval_seconds = interval_ms / 1000
                return self.analytics_engine.time_bucket_aggregate(
                    df, interval_seconds, method
                )
            else:
                logger.debug(f"Using pandas for aggregation of {len(df)} rows")
                return self._pandas_aggregate(df, interval_ms, method)
                
        except Exception as e:
            logger.error(f"Smart aggregation failed: {e}")
            # Fallback to pandas
            return self._pandas_aggregate(df, interval_ms, method)
    
    def exact_interval_aggregate(self, df: pd.DataFrame, 
                               interval_ms: int,
                               method: AggregationMethod = AggregationMethod.AVG,
                               use_duckdb: Optional[bool] = None) -> pd.DataFrame:
        """
        Apply exact interval aggregation as requested by user.
        Always uses the exact interval specified, no optimization override.
        """
        try:
            # Auto-decide engine if not specified  
            if use_duckdb is None:
                use_duckdb = (
                    self.analytics_engine and
                    self.duckdb_config.enabled and
                    len(df) > self.duckdb_config.min_datapoints_threshold
                )
            
            if use_duckdb and self.analytics_engine:
                logger.debug(f"Using DuckDB for exact interval aggregation: {interval_ms}ms, {method.value}")
                interval_seconds = interval_ms / 1000
                return self.analytics_engine.time_bucket_aggregate(
                    df, interval_seconds, method
                )
            else:
                logger.debug(f"Using pandas for exact interval aggregation: {interval_ms}ms, {method.value}")
                return self._pandas_aggregate(df, interval_ms, method)
                
        except Exception as e:
            logger.error(f"Exact interval aggregation failed: {e}")
            # Fallback to pandas
            return self._pandas_aggregate(df, interval_ms, method)
    
    def _pandas_aggregate(self, df: pd.DataFrame, interval_ms: int, 
                         method: AggregationMethod) -> pd.DataFrame:
        """Fallback pandas aggregation."""
        try:
            if df.empty:
                return df
            
            # Ensure timestamp is datetime
            df = df.copy()
            if 'timestamp' in df.columns:
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
                return result.sort_values('timestamp').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Pandas aggregation failed: {e}")
            return df
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregator statistics."""
        stats = {
            'duckdb_enabled': self.duckdb_config.enabled,
            'analytics_engine_available': self.analytics_engine is not None
        }
        
        if self.analytics_engine:
            stats['analytics_stats'] = self.analytics_engine.get_performance_stats()
        
        return stats
    
    def close(self):
        """Close all connections."""
        if self.analytics_engine:
            self.analytics_engine.close()
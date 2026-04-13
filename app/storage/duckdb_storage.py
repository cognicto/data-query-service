"""
DuckDB-based storage backend for high-performance sensor data access.
"""

import logging
import os
import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta, timezone
import concurrent.futures
from threading import Lock
import json

from app.config import LocalStorageConfig, DuckDBConfig
from app.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class DuckDBStorageBackend(StorageBackend):
    """DuckDB-based storage backend for reading sensor data with zero-copy Parquet access."""
    
    def __init__(self, local_config: LocalStorageConfig, duckdb_config: DuckDBConfig):
        """Initialize DuckDB storage backend."""
        self.local_config = local_config
        self.duckdb_config = duckdb_config
        self.data_path = local_config.data_path
        self.connection = None
        
        # Cache for file listings and metadata
        self._file_cache = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300  # 5 minutes for file listings
        
        # Initialize DuckDB connection
        self._initialize_connection()
        
        logger.info(f"Initialized DuckDB storage backend at: {self.data_path}")
    
    def _initialize_connection(self):
        """Initialize DuckDB connection with optimal settings."""
        try:
            # Create in-memory DuckDB connection
            self.connection = duckdb.connect(':memory:')
            
            # Configure DuckDB for optimal performance
            self.connection.execute(f"SET memory_limit='{self.duckdb_config.memory_limit}'")
            self.connection.execute(f"SET threads={self.duckdb_config.threads}")
            
            # Time-series specific optimizations
            # Use 80% of the configured memory limit
            memory_mb = int(float(self.duckdb_config.memory_limit.replace('GB', '')) * 1024 * 0.8)
            self.connection.execute(f"SET max_memory='{memory_mb}mb'")
            
            # Enable available optimizations (check if supported)
            try:
                self.connection.execute("SET enable_object_cache=true")
            except:
                logger.debug("enable_object_cache not available")
            
            try:
                self.connection.execute("SET enable_http_metadata_cache=true")
            except:
                logger.debug("enable_http_metadata_cache not available")
            
            logger.info("DuckDB storage connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB connection: {e}")
            raise
    
    def list_files(self, prefix: str = "") -> List[str]:
        """List Parquet files in local storage with caching."""
        try:
            with self._cache_lock:
                cache_key = f"list_files_{prefix}"
                cached = self._file_cache.get(cache_key)
                
                if cached and (datetime.utcnow() - cached['timestamp']).seconds < self._cache_ttl:
                    return cached['files']
            
            files = []
            search_path = self.data_path / prefix if prefix else self.data_path
            
            if search_path.exists():
                for file_path in search_path.rglob("*.parquet"):
                    # Get relative path from data root
                    rel_path = file_path.relative_to(self.data_path)
                    files.append(str(rel_path))
            
            # Cache the results
            with self._cache_lock:
                self._file_cache[cache_key] = {
                    'files': files,
                    'timestamp': datetime.utcnow()
                }
            
            logger.debug(f"Found {len(files)} Parquet files with prefix '{prefix}'")
            return files
            
        except Exception as e:
            logger.error(f"Error listing files with prefix '{prefix}': {e}")
            return []
    
    def read_parquet(self, file_path: str) -> pd.DataFrame:
        """Read a single Parquet file using DuckDB."""
        return self.read_single_file(file_path)
    
    def read_parquet_files(self, file_paths: List[str], 
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Read multiple Parquet files using DuckDB with optional filtering."""
        try:
            if not file_paths:
                return pd.DataFrame()
            
            # Convert relative paths to absolute paths
            absolute_paths = []
            for file_path in file_paths:
                if not Path(file_path).is_absolute():
                    abs_path = self.data_path / file_path
                else:
                    abs_path = Path(file_path)
                
                if abs_path.exists():
                    absolute_paths.append(str(abs_path))
                else:
                    logger.warning(f"File not found: {abs_path}")
            
            if not absolute_paths:
                logger.warning("No valid files found for reading")
                return pd.DataFrame()
            
            # Build DuckDB query
            query_parts = []
            
            # Handle single vs multiple files
            if len(absolute_paths) == 1:
                file_query = f"SELECT * FROM read_parquet('{absolute_paths[0]}')"
            else:
                # Use UNION ALL for multiple files
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in absolute_paths]
                file_query = " UNION ALL ".join(file_queries)
            
            # Build main query with filters
            query = f"SELECT"
            
            # Column selection
            if columns:
                query += f" {', '.join(columns)}"
            else:
                query += " *"
            
            query += f" FROM ({file_query}) data"
            
            # Time range filtering
            where_conditions = []
            if start_time:
                start_str = start_time.isoformat()
                where_conditions.append(f"timestamp >= '{start_str}'")
            
            if end_time:
                end_str = end_time.isoformat()
                where_conditions.append(f"timestamp <= '{end_str}'")
            
            if where_conditions:
                query += f" WHERE {' AND '.join(where_conditions)}"
            
            # Order by timestamp for time-series data
            query += " ORDER BY timestamp"
            
            logger.debug(f"Executing DuckDB query on {len(absolute_paths)} files")
            start_query_time = datetime.utcnow()
            
            # Execute query and return DataFrame
            result = self.connection.execute(query).fetchdf()
            
            query_duration = (datetime.utcnow() - start_query_time).total_seconds()
            logger.info(f"DuckDB query completed in {query_duration:.2f}s, returned {len(result)} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Error reading Parquet files with DuckDB: {e}")
            # Fallback to pandas
            return self._fallback_read_parquet(file_paths, start_time, end_time, columns)
    
    def read_single_file(self, file_path: str, 
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Read a single Parquet file using DuckDB."""
        return self.read_parquet_files([file_path], start_time, end_time, columns)
    
    def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata from a Parquet file using DuckDB."""
        try:
            # Convert to absolute path
            if not Path(file_path).is_absolute():
                abs_path = self.data_path / file_path
            else:
                abs_path = Path(file_path)
            
            if not abs_path.exists():
                logger.warning(f"File not found: {abs_path}")
                return {}
            
            # Use DuckDB to get file metadata
            query = f"""
                SELECT 
                    count(*) as row_count,
                    min(timestamp) as min_timestamp,
                    max(timestamp) as max_timestamp,
                    count(DISTINCT timestamp) as unique_timestamps
                FROM read_parquet('{abs_path}')
            """
            
            result = self.connection.execute(query).fetchone()
            
            if result:
                return {
                    'row_count': result[0],
                    'min_timestamp': result[1],
                    'max_timestamp': result[2],
                    'unique_timestamps': result[3],
                    'file_size': abs_path.stat().st_size if abs_path.exists() else 0
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting metadata for {file_path}: {e}")
            return {}
    
    def execute_aggregation_query(self, file_paths: List[str], 
                                 aggregation_sql: str,
                                 start_time: Optional[datetime] = None,
                                 end_time: Optional[datetime] = None) -> pd.DataFrame:
        """Execute custom aggregation query on multiple files."""
        try:
            if not file_paths:
                return pd.DataFrame()
            
            # Convert to absolute paths
            absolute_paths = []
            for file_path in file_paths:
                if not Path(file_path).is_absolute():
                    abs_path = self.data_path / file_path
                else:
                    abs_path = Path(file_path)
                
                if abs_path.exists():
                    absolute_paths.append(str(abs_path))
            
            if not absolute_paths:
                return pd.DataFrame()
            
            # Create a view combining all files
            if len(absolute_paths) == 1:
                view_query = f"CREATE OR REPLACE VIEW combined_data AS SELECT * FROM read_parquet('{absolute_paths[0]}')"
            else:
                file_queries = [f"SELECT * FROM read_parquet('{path}')" for path in absolute_paths]
                union_query = " UNION ALL ".join(file_queries)
                view_query = f"CREATE OR REPLACE VIEW combined_data AS {union_query}"
            
            self.connection.execute(view_query)
            
            # Build final query with time filters
            where_conditions = []
            if start_time:
                start_str = start_time.isoformat()
                where_conditions.append(f"timestamp >= '{start_str}'")
            
            if end_time:
                end_str = end_time.isoformat()
                where_conditions.append(f"timestamp <= '{end_str}'")
            
            # Modify aggregation SQL to include WHERE clause if needed
            final_sql = aggregation_sql
            if where_conditions and "WHERE" not in aggregation_sql.upper():
                final_sql += f" WHERE {' AND '.join(where_conditions)}"
            
            logger.debug(f"Executing aggregation query on {len(absolute_paths)} files")
            result = self.connection.execute(final_sql).fetchdf()
            
            # Clean up view
            self.connection.execute("DROP VIEW IF EXISTS combined_data")
            
            logger.info(f"Aggregation query returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Error executing aggregation query: {e}")
            return pd.DataFrame()
    
    def _fallback_read_parquet(self, file_paths: List[str],
                              start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None,
                              columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Fallback to pandas for reading Parquet files."""
        try:
            dataframes = []
            
            for file_path in file_paths:
                if not Path(file_path).is_absolute():
                    abs_path = self.data_path / file_path
                else:
                    abs_path = Path(file_path)
                
                if abs_path.exists():
                    try:
                        df = pd.read_parquet(abs_path, columns=columns)
                        
                        # Apply time filtering if needed
                        if start_time or end_time:
                            if 'timestamp' in df.columns:
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                
                                # Convert all to UTC for consistent comparison
                                if df['timestamp'].dt.tz is None:
                                    df_timestamps = df['timestamp'].dt.tz_localize('UTC')
                                else:
                                    df_timestamps = df['timestamp'].dt.tz_convert('UTC')
                                
                                if start_time:
                                    start_utc = start_time.replace(tzinfo=timezone.utc) if start_time.tzinfo is None else start_time.astimezone(timezone.utc)
                                    df = df[df_timestamps >= start_utc]
                                
                                if end_time:
                                    end_utc = end_time.replace(tzinfo=timezone.utc) if end_time.tzinfo is None else end_time.astimezone(timezone.utc)
                                    df = df[df_timestamps <= end_utc]
                        
                        if not df.empty:
                            dataframes.append(df)
                            
                    except Exception as e:
                        logger.warning(f"Error reading {abs_path}: {e}")
            
            if dataframes:
                result = pd.concat(dataframes, ignore_index=True)
                if 'timestamp' in result.columns:
                    result = result.sort_values('timestamp').reset_index(drop=True)
                return result
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Fallback pandas read failed: {e}")
            return pd.DataFrame()
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get DuckDB connection statistics."""
        try:
            stats = {
                'connection_active': self.connection is not None,
                'memory_limit': self.duckdb_config.memory_limit,
                'threads': self.duckdb_config.threads
            }
            
            if self.connection:
                # Get current settings
                try:
                    memory_result = self.connection.execute("SELECT current_setting('memory_limit')").fetchone()
                    if memory_result:
                        stats['current_memory_limit'] = memory_result[0]
                    
                    thread_result = self.connection.execute("SELECT current_setting('threads')").fetchone()
                    if thread_result:
                        stats['current_threads'] = int(thread_result[0])
                        
                except Exception as e:
                    logger.debug(f"Could not get current settings: {e}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting connection stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close DuckDB connection and cleanup."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                logger.info("DuckDB storage connection closed")
                
            # Clear caches
            with self._cache_lock:
                self._file_cache.clear()
                
        except Exception as e:
            logger.error(f"Error closing DuckDB storage: {e}")
    
    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists in local storage."""
        if not Path(file_path).is_absolute():
            abs_path = self.data_path / file_path
        else:
            abs_path = Path(file_path)
        return abs_path.exists()
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get file information."""
        if not Path(file_path).is_absolute():
            abs_path = self.data_path / file_path
        else:
            abs_path = Path(file_path)
        
        if not abs_path.exists():
            return {}
        
        stat = abs_path.stat()
        return {
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'path': str(abs_path)
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Check health of DuckDB storage backend."""
        try:
            health = {
                'healthy': True,
                'connection_active': self.connection is not None,
                'data_path_exists': self.data_path.exists(),
                'data_path_readable': os.access(self.data_path, os.R_OK) if self.data_path.exists() else False
            }
            
            if self.connection:
                # Test DuckDB connection
                test_result = self.connection.execute("SELECT 1").fetchone()
                health['duckdb_test'] = test_result[0] == 1
            else:
                health['healthy'] = False
                health['duckdb_test'] = False
            
            return health
            
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e)
            }
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.close()


class HybridStorageReader:
    """Hybrid storage reader that uses DuckDB for large queries and pandas for small ones."""
    
    def __init__(self, local_config: LocalStorageConfig, duckdb_config: DuckDBConfig):
        """Initialize hybrid storage reader."""
        self.local_config = local_config
        self.duckdb_config = duckdb_config
        
        # Initialize backends
        self.duckdb_backend = None
        self.pandas_available = True
        
        # Try to initialize DuckDB backend
        if duckdb_config.enabled:
            try:
                self.duckdb_backend = DuckDBStorageBackend(local_config, duckdb_config)
                logger.info("Hybrid storage reader initialized with DuckDB support")
            except Exception as e:
                logger.error(f"Failed to initialize DuckDB backend: {e}")
                logger.info("Falling back to pandas-only mode")
        else:
            logger.info("DuckDB disabled, using pandas-only mode")
    
    def read_sensor_data(self, file_paths: List[str],
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        columns: Optional[List[str]] = None,
                        estimated_rows: Optional[int] = None) -> pd.DataFrame:
        """Read sensor data using optimal backend based on data characteristics."""
        try:
            # Decide which backend to use
            use_duckdb = (
                self.duckdb_backend and
                self.duckdb_config.enabled and
                (
                    (estimated_rows and estimated_rows > self.duckdb_config.min_datapoints_threshold) or
                    len(file_paths) > 1 or
                    (estimated_rows is None and len(file_paths) > 0)  # Default to DuckDB for multi-file
                )
            )
            
            if use_duckdb:
                logger.debug(f"Using DuckDB backend for {len(file_paths)} files")
                return self.duckdb_backend.read_parquet_files(file_paths, start_time, end_time, columns)
            else:
                logger.debug(f"Using pandas backend for {len(file_paths)} files")
                return self._read_with_pandas(file_paths, start_time, end_time, columns)
                
        except Exception as e:
            logger.error(f"Hybrid read failed: {e}")
            # Final fallback to pandas
            return self._read_with_pandas(file_paths, start_time, end_time, columns)
    
    def _read_with_pandas(self, file_paths: List[str],
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Read data using pandas fallback."""
        try:
            dataframes = []
            
            for file_path in file_paths:
                if not Path(file_path).is_absolute():
                    abs_path = self.local_config.data_path / file_path
                else:
                    abs_path = Path(file_path)
                
                if abs_path.exists():
                    try:
                        df = pd.read_parquet(abs_path, columns=columns)
                        
                        # Apply time filtering
                        if start_time or end_time:
                            if 'timestamp' in df.columns:
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                
                                # Convert all to UTC for consistent comparison
                                if df['timestamp'].dt.tz is None:
                                    df_timestamps = df['timestamp'].dt.tz_localize('UTC')
                                else:
                                    df_timestamps = df['timestamp'].dt.tz_convert('UTC')
                                
                                if start_time:
                                    start_utc = start_time.replace(tzinfo=timezone.utc) if start_time.tzinfo is None else start_time.astimezone(timezone.utc)
                                    df = df[df_timestamps >= start_utc]
                                
                                if end_time:
                                    end_utc = end_time.replace(tzinfo=timezone.utc) if end_time.tzinfo is None else end_time.astimezone(timezone.utc)
                                    df = df[df_timestamps <= end_utc]
                        
                        if not df.empty:
                            dataframes.append(df)
                            
                    except Exception as e:
                        logger.warning(f"Error reading {abs_path}: {e}")
            
            if dataframes:
                result = pd.concat(dataframes, ignore_index=True)
                if 'timestamp' in result.columns:
                    result = result.sort_values('timestamp').reset_index(drop=True)
                return result
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Pandas read failed: {e}")
            return pd.DataFrame()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all backends."""
        stats = {
            'duckdb_enabled': self.duckdb_config.enabled,
            'duckdb_available': self.duckdb_backend is not None,
            'pandas_available': self.pandas_available
        }
        
        if self.duckdb_backend:
            stats['duckdb_stats'] = self.duckdb_backend.get_connection_stats()
        
        return stats
    
    def close(self):
        """Close all backends."""
        if self.duckdb_backend:
            self.duckdb_backend.close()
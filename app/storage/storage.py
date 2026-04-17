"""
Simplified unified storage backend using DuckDB for both local and Azure ADLS Gen2.
"""

import logging
import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta
from enum import Enum

from app.config import AppConfig, StorageMode

class DataTier(Enum):
    """Available data tiers for optimization."""
    RAW = "raw"
    MINUTE = "minute"
    HOURLY = "hourly" 
    DAILY = "daily"

logger = logging.getLogger(__name__)


class UnifiedStorageBackend:
    """Unified storage backend using DuckDB for both local and Azure ADLS Gen2."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection = duckdb.connect(':memory:')
        self._setup_duckdb()
        self._setup_authentication()
    
    def _setup_duckdb(self):
        """Configure DuckDB with optimal settings."""
        self.connection.execute(f"SET memory_limit='{self.config.duckdb.memory_limit}'")
        self.connection.execute(f"SET threads={self.config.duckdb.threads}")
        
        # Install and load Azure extension if needed
        if self.config.storage_mode == StorageMode.AZURE:
            self.connection.execute("INSTALL azure")
            self.connection.execute("LOAD azure")
            logger.info("Azure extension loaded")
    
    def _setup_authentication(self):
        """Setup authentication based on storage mode."""
        if self.config.storage_mode == StorageMode.AZURE:
            # Create Azure secret with SAS token
            sas_token = self.config.azure.sas_token.lstrip('?')
            
            self.connection.execute(f"""
                CREATE SECRET azure_secret (
                    TYPE AZURE,
                    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName={self.config.azure.account_name};SharedAccessSignature={sas_token};EndpointSuffix=core.windows.net'
                )
            """)
            logger.info("Azure authentication configured")
    
    def query_sensor_data(self, 
                         sensors: List[str],
                         start_time: datetime,
                         end_time: datetime, 
                         asset_ids: List[str],
                         interval_ms: Optional[int] = None,
                         max_datapoints: Optional[int] = None,
                         aggregation_method: str = "avg") -> pd.DataFrame:
        """Query sensor data with automatic aggregation."""
        
        # Select optimal data tier
        selected_tier = self._select_data_tier(interval_ms)
        logger.info(f"Selected data tier: {selected_tier.value} for interval_ms={interval_ms}")
        
        # Build file paths based on selected tier
        file_paths = self._build_file_paths(sensors, start_time, end_time, asset_ids, selected_tier)
        
        if not file_paths:
            logger.warning("No files found for query")
            return pd.DataFrame()
        
        # Build and execute DuckDB query
        query = self._build_query(file_paths, start_time, end_time, interval_ms, aggregation_method)
        
        try:
            logger.debug(f"Executing query: {query}")
            result = self.connection.execute(query).fetchdf()
            
            # Apply max datapoints limit
            if max_datapoints and len(result) > max_datapoints:
                result = result.head(max_datapoints)
                logger.info(f"Truncated result to {max_datapoints} rows")
            
            logger.info(f"Query returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def _build_file_paths(self, 
                         sensors: List[str],
                         start_time: datetime, 
                         end_time: datetime,
                         asset_ids: List[str],
                         tier: DataTier) -> List[str]:
        """Build file paths based on storage mode."""
        paths = []
        
        logger.info(f"Building file paths: sensors={sensors}, asset_ids={asset_ids}, tier={tier}, start={start_time}, end={end_time}")
        
        for asset_id in asset_ids:
            for sensor in sensors:
                if self.config.storage_mode == StorageMode.AZURE:
                    # Use ADLS Gen2 with hierarchical namespace
                    paths.extend(self._build_azure_paths(asset_id, sensor, start_time, end_time, tier))
                else:
                    # Local file paths
                    paths.extend(self._build_local_paths(asset_id, sensor, start_time, end_time, tier))
        
        return paths
    
    def _build_azure_paths(self, asset_id: str, sensor: str, 
                          start_time: datetime, end_time: datetime, tier: DataTier) -> List[str]:
        """Build Azure ADLS Gen2 paths with glob patterns for efficiency."""
        
        base_url = self.config.azure.abfss_endpoint
        prefix = f"/{self.config.azure.data_prefix}" if self.config.azure.data_prefix else ""
        
        if tier == DataTier.RAW:
            return self._build_raw_azure_paths(base_url, prefix, asset_id, sensor, start_time, end_time)
        elif tier == DataTier.MINUTE:
            return self._build_minute_azure_paths(base_url, prefix, asset_id, sensor, start_time, end_time)
        elif tier == DataTier.HOURLY:
            return self._build_hourly_azure_paths(base_url, prefix, asset_id, sensor, start_time, end_time)
        elif tier == DataTier.DAILY:
            return self._build_daily_azure_paths(base_url, prefix, asset_id, sensor, start_time, end_time)
        
        return []
    
    def _build_raw_azure_paths(self, base_url: str, prefix: str, asset_id: str, sensor: str, 
                              start_time: datetime, end_time: datetime) -> List[str]:
        """Build paths for raw data tier."""
        paths = []
        current_date = start_time.date()
        
        while current_date <= end_time.date():
            year, month, day = current_date.year, current_date.month, current_date.day
            
            if current_date == start_time.date() == end_time.date():
                # Same day - specific hours
                for hour in range(start_time.hour, end_time.hour + 1):
                    path = f"{base_url}{prefix}/{asset_id}/{year:04d}/{month:02d}/{day:02d}/{hour:02d}/{sensor}_{year:04d}{month:02d}{day:02d}_{hour:02d}.parquet"
                    paths.append(path)
            else:
                # Multiple days - full day ranges
                start_hour = start_time.hour if current_date == start_time.date() else 0
                end_hour = end_time.hour if current_date == end_time.date() else 23
                
                for hour in range(start_hour, end_hour + 1):
                    path = f"{base_url}{prefix}/{asset_id}/{year:04d}/{month:02d}/{day:02d}/{hour:02d}/{sensor}_{year:04d}{month:02d}{day:02d}_{hour:02d}.parquet"
                    paths.append(path)
            
            current_date += timedelta(days=1)
        
        return paths
    
    def _build_minute_azure_paths(self, base_url: str, prefix: str, asset_id: str, sensor: str,
                                 start_time: datetime, end_time: datetime) -> List[str]:
        """Build paths for minute aggregated data tier."""
        paths = []
        current_date = start_time.date()
        
        while current_date <= end_time.date():
            year, month, day = current_date.year, current_date.month, current_date.day
            
            if current_date == start_time.date() == end_time.date():
                # Same day - specific hours
                for hour in range(start_time.hour, end_time.hour + 1):
                    path = f"{base_url}{prefix}/aggregated/{asset_id}/{year:04d}/{month:02d}/{day:02d}/{hour:02d}/{sensor}_minute.parquet"
                    paths.append(path)
            else:
                # Multiple days
                start_hour = start_time.hour if current_date == start_time.date() else 0
                end_hour = end_time.hour if current_date == end_time.date() else 23
                
                for hour in range(start_hour, end_hour + 1):
                    path = f"{base_url}{prefix}/aggregated/{asset_id}/{year:04d}/{month:02d}/{day:02d}/{hour:02d}/{sensor}_minute.parquet"
                    paths.append(path)
            
            current_date += timedelta(days=1)
        
        return paths
    
    def _build_hourly_azure_paths(self, base_url: str, prefix: str, asset_id: str, sensor: str,
                                 start_time: datetime, end_time: datetime) -> List[str]:
        """Build paths for hourly aggregated data tier."""
        paths = []
        current_date = start_time.date()
        
        while current_date <= end_time.date():
            year, month, day = current_date.year, current_date.month, current_date.day
            path = f"{base_url}{prefix}/aggregated/{asset_id}/{year:04d}/{month:02d}/{day:02d}/{sensor}_hour.parquet"
            paths.append(path)
            current_date += timedelta(days=1)
        
        return paths
    
    def _build_daily_azure_paths(self, base_url: str, prefix: str, asset_id: str, sensor: str,
                                start_time: datetime, end_time: datetime) -> List[str]:
        """Build paths for daily aggregated data tier.""" 
        paths = []
        current_date = start_time.date()
        
        while current_date <= end_time.date():
            year, month = current_date.year, current_date.month
            path = f"{base_url}{prefix}/daily/{asset_id}/{year:04d}/{month:02d}/{sensor}_day.parquet"
            paths.append(path)
            # Move to next month for daily aggregation
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1, day=1)
            
            # Avoid infinite loop
            if current_date > end_time.date():
                break
        
        return paths
    
    def _build_local_paths(self, asset_id: str, sensor: str,
                          start_time: datetime, end_time: datetime, tier: DataTier) -> List[str]:
        """Build local file paths."""
        
        base_path = Path(self.config.local.data_path)
        
        if tier == DataTier.RAW:
            return self._build_raw_local_paths(base_path, asset_id, sensor, start_time, end_time)
        elif tier == DataTier.MINUTE:
            return self._build_minute_local_paths(base_path, asset_id, sensor, start_time, end_time)
        elif tier == DataTier.HOURLY:
            return self._build_hourly_local_paths(base_path, asset_id, sensor, start_time, end_time)
        elif tier == DataTier.DAILY:
            return self._build_daily_local_paths(base_path, asset_id, sensor, start_time, end_time)
        
        return []
    
    def _build_raw_local_paths(self, base_path: Path, asset_id: str, sensor: str,
                              start_time: datetime, end_time: datetime) -> List[str]:
        """Build local paths for raw data tier."""
        paths = []
        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        
        logger.info(f"Building raw local paths: base_path={base_path}, asset_id={asset_id}, sensor={sensor}, start={start_time}, end={end_time}")
        logger.info(f"Current working directory: {Path.cwd()}")
        
        while current_time <= end_time:
            year, month, day, hour = current_time.year, current_time.month, current_time.day, current_time.hour
            file_path = base_path / asset_id / f"{year:04d}" / f"{month:02d}" / f"{day:02d}" / f"{hour:02d}" / f"{sensor}_{year:04d}{month:02d}{day:02d}_{hour:02d}.parquet"
            
            logger.info(f"Checking path: {file_path}, exists: {file_path.exists()}")
            
            if file_path.exists():
                paths.append(str(file_path))
            
            current_time += timedelta(hours=1)
        
        logger.info(f"Found {len(paths)} raw local paths")
        return paths
    
    def _build_minute_local_paths(self, base_path: Path, asset_id: str, sensor: str,
                                 start_time: datetime, end_time: datetime) -> List[str]:
        """Build local paths for minute aggregated data tier."""
        paths = []
        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        
        while current_time <= end_time:
            year, month, day, hour = current_time.year, current_time.month, current_time.day, current_time.hour
            file_path = base_path / "aggregated" / asset_id / f"{year:04d}" / f"{month:02d}" / f"{day:02d}" / f"{hour:02d}" / f"{sensor}_minute.parquet"
            
            if file_path.exists():
                paths.append(str(file_path))
            
            current_time += timedelta(hours=1)
        
        return paths
    
    def _build_hourly_local_paths(self, base_path: Path, asset_id: str, sensor: str,
                                 start_time: datetime, end_time: datetime) -> List[str]:
        """Build local paths for hourly aggregated data tier."""
        paths = []
        current_date = start_time.date()
        
        while current_date <= end_time.date():
            year, month, day = current_date.year, current_date.month, current_date.day
            file_path = base_path / "aggregated" / asset_id / f"{year:04d}" / f"{month:02d}" / f"{day:02d}" / f"{sensor}_hour.parquet"
            
            if file_path.exists():
                paths.append(str(file_path))
            
            current_date += timedelta(days=1)
        
        return paths
    
    def _build_daily_local_paths(self, base_path: Path, asset_id: str, sensor: str,
                                start_time: datetime, end_time: datetime) -> List[str]:
        """Build local paths for daily aggregated data tier."""
        paths = []
        current_date = start_time.date()
        
        while current_date <= end_time.date():
            year, month = current_date.year, current_date.month
            file_path = base_path / "daily" / asset_id / f"{year:04d}" / f"{month:02d}" / f"{sensor}_day.parquet"
            
            if file_path.exists():
                paths.append(str(file_path))
            
            # Move to next month for daily aggregation
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1, day=1)
            
            if current_date > end_time.date():
                break
        
        return paths
    
    def _select_data_tier(self, interval_ms: Optional[int]) -> DataTier:
        """Select optimal data tier based on aggregation interval."""
        if not interval_ms:
            return DataTier.RAW
        
        # Convert interval to minutes
        interval_minutes = interval_ms / (1000 * 60)
        
        # Apply tier selection logic based on configuration
        if interval_minutes < self.config.tiers.raw_threshold_minutes:
            return DataTier.RAW
        elif interval_minutes < self.config.tiers.minute_threshold_minutes:
            return DataTier.MINUTE
        else:
            # For intervals >= 60 minutes, decide between hourly and daily
            # Convert threshold from days to minutes for comparison
            daily_threshold_minutes = self.config.tiers.hourly_threshold_days * 24 * 60  # 30 days * 24h * 60m = 43,200 minutes
            
            if interval_minutes < daily_threshold_minutes:
                return DataTier.HOURLY
            else:
                return DataTier.DAILY
    
    def _extract_asset_id_from_path(self, file_path: str) -> str:
        """Extract asset_id from file path."""
        # Path format: .../asset_001/yyyy/mm/dd/hh/sensor_name.parquet
        # or: .../aggregated/asset_001/yyyy/mm/dd/hh/sensor_name.parquet
        # or: .../daily/asset_001/yyyy/mm/sensor_name.parquet
        parts = file_path.split('/')
        # Find asset_xxx pattern
        for part in parts:
            if part.startswith('asset_'):
                return part
        return 'unknown'
    
    def _extract_sensor_name_from_path(self, file_path: str) -> str:
        """Extract sensor_name from file path."""
        # File format: sensor_name_yyyymmdd_hh.parquet or sensor_name_minute.parquet etc.
        filename = file_path.split('/')[-1]  # Get filename
        filename_no_ext = filename.replace('.parquet', '')
        
        # Handle different file patterns
        if '_minute.parquet' in filename or '_hour.parquet' in filename or '_day.parquet' in filename:
            # Aggregated files: sensor_name_minute/hour/day.parquet
            return filename_no_ext.rsplit('_', 1)[0]
        else:
            # Raw files: sensor_name_yyyymmdd_hh.parquet
            parts = filename_no_ext.split('_')
            if len(parts) >= 3:
                # Take everything except last two parts (date and hour)
                return '_'.join(parts[:-2])
            return filename_no_ext
    
    def _build_query(self, file_paths: List[str], start_time: datetime, 
                    end_time: datetime, interval_ms: Optional[int] = None, 
                    aggregation_method: str = "avg") -> str:
        """Build DuckDB query with optional aggregation."""
        
        # Create file reading clause with asset_id and sensor_name from file paths
        if len(file_paths) == 1:
            # Extract asset_id and sensor_name from file path
            asset_id = self._extract_asset_id_from_path(file_paths[0])
            sensor_name = self._extract_sensor_name_from_path(file_paths[0])
            files_clause = f"SELECT *, '{asset_id}' as asset_id, '{sensor_name}' as sensor_name FROM read_parquet('{file_paths[0]}')"
        else:
            file_selects = []
            for path in file_paths:
                asset_id = self._extract_asset_id_from_path(path)
                sensor_name = self._extract_sensor_name_from_path(path)
                file_selects.append(f"SELECT *, '{asset_id}' as asset_id, '{sensor_name}' as sensor_name FROM read_parquet('{path}')")
            files_clause = f"({' UNION ALL '.join(file_selects)})"
        
        # Base query with time filtering
        if interval_ms and interval_ms > 1000:
            # Aggregated query using DuckDB INTERVAL syntax
            interval_seconds = int(interval_ms / 1000)
            
            # Map aggregation methods to SQL functions
            agg_functions = {
                'avg': 'AVG',
                'min': 'MIN', 
                'max': 'MAX',
                'sum': 'SUM',
                'count': 'COUNT'
            }
            agg_func = agg_functions.get(aggregation_method.lower(), 'AVG')
            
            if len(file_paths) == 1:
                # For single file aggregated query
                query = f"""
                    SELECT 
                        CAST(EXTRACT(epoch FROM time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)) * 1000 AS BIGINT) as timestamp_ms,
                        {agg_func}(value) as value,
                        first(sensor_name) as sensor_name,
                        first(asset_id) as asset_id
                    FROM ({files_clause})
                    WHERE timestamp >= '{start_time.isoformat()}'
                      AND timestamp <= '{end_time.isoformat()}'
                    GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)
                    ORDER BY timestamp_ms
                """
            else:
                # For multiple files aggregated query
                query = f"""
                    SELECT 
                        CAST(EXTRACT(epoch FROM time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)) * 1000 AS BIGINT) as timestamp_ms,
                        {agg_func}(value) as value,
                        first(sensor_name) as sensor_name,
                        first(asset_id) as asset_id
                    FROM {files_clause}
                    WHERE timestamp >= '{start_time.isoformat()}'
                      AND timestamp <= '{end_time.isoformat()}'
                    GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', timestamp)
                    ORDER BY timestamp_ms
                """
        else:
            # Raw query
            if len(file_paths) == 1:
                # For single file, use the files_clause directly
                query = f"""
                    {files_clause}
                """
                # Add WHERE clause if needed for time filtering
                query += f"""
                    WHERE timestamp >= '{start_time.isoformat()}'
                      AND timestamp <= '{end_time.isoformat()}'
                    ORDER BY timestamp
                """
            else:
                # For multiple files, wrap in FROM clause
                query = f"""
                    SELECT *
                    FROM {files_clause}
                    WHERE timestamp >= '{start_time.isoformat()}'
                      AND timestamp <= '{end_time.isoformat()}'
                    ORDER BY timestamp
                """
        
        return query
    
    def get_available_assets(self) -> List[str]:
        """Get list of available assets."""
        try:
            if self.config.storage_mode == StorageMode.AZURE:
                # Use Azure blob listing
                base_url = self.config.azure.abfss_endpoint
                prefix = f"/{self.config.azure.data_prefix}" if self.config.azure.data_prefix else ""
                
                query = f"""
                    SELECT DISTINCT regexp_extract(file, '([^/]+)/', 1) as asset_id
                    FROM glob('{base_url}{prefix}/*/') 
                    WHERE asset_id IS NOT NULL
                    ORDER BY asset_id
                """
            else:
                # Local directory listing
                query = f"""
                    SELECT DISTINCT replace(replace(file, '{self.config.local.data_path}/', ''), '/', '') as asset_id
                    FROM glob('{self.config.local.data_path}/*/') 
                    WHERE asset_id IS NOT NULL AND asset_id NOT LIKE '%metadata%' AND asset_id NOT LIKE '%aggregated%' AND asset_id NOT LIKE '%daily%'
                    ORDER BY asset_id
                """
            
            result = self.connection.execute(query).fetchall()
            return [row[0] for row in result if row[0]]
            
        except Exception as e:
            logger.error(f"Failed to list assets: {e}")
            return []
    
    def get_available_sensors(self, asset_id: Optional[str] = None) -> List[str]:
        """Get list of available sensors for an asset."""
        try:
            if self.config.storage_mode == StorageMode.AZURE:
                base_url = self.config.azure.abfss_endpoint
                prefix = f"/{self.config.azure.data_prefix}" if self.config.azure.data_prefix else ""
                
                if asset_id:
                    pattern = f"{base_url}{prefix}/{asset_id}/**/*.parquet"
                else:
                    pattern = f"{base_url}{prefix}/**/*.parquet"
            else:
                if asset_id:
                    pattern = f"{self.config.local.data_path}/{asset_id}/**/*.parquet"
                else:
                    pattern = f"{self.config.local.data_path}/**/*.parquet"
            
            query = f"""
                SELECT DISTINCT regexp_extract(file, '([^/]+)_\\\\d{{8}}_\\\\d{{2}}\\\\.parquet$', 1) as sensor_name
                FROM glob('{pattern}')
                WHERE sensor_name IS NOT NULL
                ORDER BY sensor_name
            """
            
            result = self.connection.execute(query).fetchall()
            return [row[0] for row in result if row[0]]
            
        except Exception as e:
            logger.error(f"Failed to list sensors: {e}")
            return []
    
    def health_check(self) -> dict:
        """Check storage backend health."""
        try:
            # Test basic DuckDB query
            test_result = self.connection.execute("SELECT 1").fetchone()
            
            health = {
                'healthy': test_result[0] == 1,
                'storage_mode': self.config.storage_mode.value,
                'duckdb_version': self.connection.execute("SELECT version()").fetchone()[0]
            }
            
            # Test storage access
            if self.config.storage_mode == StorageMode.AZURE:
                health['azure_configured'] = bool(
                    self.config.azure.account_name and 
                    self.config.azure.container_name and 
                    self.config.azure.sas_token
                )
            else:
                health['local_path_exists'] = Path(self.config.local.data_path).exists()
            
            return health
            
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e)
            }
    
    def close(self):
        """Clean up resources."""
        if self.connection:
            self.connection.close()
            logger.info("Storage backend closed")
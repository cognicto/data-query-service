"""
Local file system storage backend for sensor data access.
"""

import logging
from typing import List, Dict, Optional
import pandas as pd
from pathlib import Path
from datetime import datetime
import concurrent.futures
from threading import Lock

from app.config import LocalStorageConfig
from app.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class LocalStorageBackend(StorageBackend):
    """Local file system storage backend for reading sensor data."""
    
    def __init__(self, config: LocalStorageConfig):
        """Initialize local storage backend."""
        self.config = config
        self.data_path = config.data_path
        
        if not self.data_path.exists():
            logger.warning(f"Local storage path does not exist: {self.data_path}")
        
        self._file_cache = {}
        self._cache_lock = Lock()
        self._cache_ttl = 60  # 1 minute (shorter than Azure since local is fast)
        
        logger.info(f"Initialized local storage backend at: {self.data_path}")
    
    def list_files(self, prefix: str = "") -> List[str]:
        """List files in local storage with optional prefix filter."""
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
                    # Convert to relative path from data_path
                    relative_path = file_path.relative_to(self.data_path)
                    files.append(str(relative_path).replace('\\', '/'))  # Normalize path separators
            
            # Cache results
            with self._cache_lock:
                self._file_cache[cache_key] = {
                    'files': files,
                    'timestamp': datetime.utcnow()
                }
            
            logger.debug(f"Listed {len(files)} files with prefix '{prefix}'")
            return files
            
        except Exception as e:
            logger.error(f"Error listing local files: {e}")
            return []
    
    def read_parquet(self, file_path: str) -> pd.DataFrame:
        """Read a Parquet file from local storage."""
        try:
            full_path = self.data_path / file_path
            
            if not full_path.exists():
                logger.warning(f"File not found: {full_path}")
                return pd.DataFrame()
            
            df = pd.read_parquet(full_path)
            logger.debug(f"Read {len(df)} rows from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return pd.DataFrame()
    
    def read_multiple_parquet(self, file_paths: List[str], max_workers: int = 4) -> pd.DataFrame:
        """Read multiple Parquet files in parallel."""
        if not file_paths:
            return pd.DataFrame()
        
        dataframes = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all read tasks
            future_to_file = {
                executor.submit(self.read_parquet, file_path): file_path
                for file_path in file_paths
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    df = future.result()
                    if not df.empty:
                        dataframes.append(df)
                except Exception as e:
                    logger.error(f"Error reading {file_path} in parallel: {e}")
        
        if not dataframes:
            return pd.DataFrame()
        
        # Combine all dataframes
        try:
            combined_df = pd.concat(dataframes, ignore_index=True)
            logger.info(f"Combined {len(dataframes)} files into {len(combined_df)} rows")
            return combined_df
        except Exception as e:
            logger.error(f"Error combining dataframes: {e}")
            return pd.DataFrame()
    
    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists in local storage."""
        try:
            full_path = self.data_path / file_path
            return full_path.exists()
        except Exception:
            return False
    
    def get_file_info(self, file_path: str) -> Dict:
        """Get file metadata from local storage."""
        try:
            full_path = self.data_path / file_path
            
            if not full_path.exists():
                return {'error': 'File not found'}
            
            stat = full_path.stat()
            
            return {
                'name': file_path,
                'size': stat.st_size,
                'size_mb': stat.st_size / (1024 * 1024),
                'last_modified': datetime.fromtimestamp(stat.st_mtime),
                'created': datetime.fromtimestamp(stat.st_ctime),
                'full_path': str(full_path)
            }
            
        except Exception as e:
            logger.error(f"Error getting file info for {file_path}: {e}")
            return {'error': str(e)}
    
    def health_check(self) -> Dict:
        """Perform health check on local storage."""
        try:
            # Check if data path exists and is accessible
            path_exists = self.data_path.exists()
            is_directory = self.data_path.is_dir() if path_exists else False
            
            # Count files
            file_count = 0
            if path_exists and is_directory:
                try:
                    file_count = len(list(self.data_path.rglob("*.parquet")))
                except Exception:
                    file_count = -1
            
            # Check available space
            available_space_gb = 0
            try:
                if path_exists:
                    stat = self.data_path.stat() if self.data_path.exists() else None
                    if stat:
                        # This is a simple check - might not work on all systems
                        available_space_gb = "unknown"  # Would need platform-specific code
            except Exception:
                available_space_gb = "unknown"
            
            return {
                'healthy': path_exists and is_directory,
                'path_exists': path_exists,
                'is_directory': is_directory,
                'data_path': str(self.data_path),
                'file_count': file_count,
                'available_space_gb': available_space_gb,
                'cache_entries': len(self._file_cache)
            }
            
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'data_path': str(self.data_path),
                'cache_entries': len(self._file_cache)
            }
    
    def clear_cache(self):
        """Clear file listing cache."""
        with self._cache_lock:
            self._file_cache.clear()
        logger.info("Cleared local storage cache")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        with self._cache_lock:
            return {
                'cache_entries': len(self._file_cache),
                'cache_keys': list(self._file_cache.keys())
            }


class LocalAggregatedReader:
    """Specialized reader for aggregated data tiers in local storage."""
    
    def __init__(self, local_backend: LocalStorageBackend):
        """Initialize with local backend."""
        self.local = local_backend
        self.data_path = local_backend.data_path
    
    def read_raw_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                     asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read raw data (1-second precision) from local storage."""
        file_paths = self._get_raw_file_paths(sensors, start_time, end_time, asset_ids)
        return self.local.read_multiple_parquet(file_paths)
    
    def read_aggregated_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                           asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read pre-aggregated data (1-minute precision) from local storage."""
        file_paths = self._get_aggregated_file_paths(sensors, start_time, end_time, asset_ids)
        return self.local.read_multiple_parquet(file_paths)
    
    def read_daily_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                       asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read daily summary data (hourly precision) from local storage."""
        file_paths = self._get_daily_file_paths(sensors, start_time, end_time, asset_ids)
        return self.local.read_multiple_parquet(file_paths)
    
    def _get_available_assets(self) -> List[str]:
        """Get available asset IDs from directory structure."""
        assets = []
        try:
            if self.data_path.exists():
                for item in self.data_path.iterdir():
                    if item.is_dir() and not item.name.startswith('.'):
                        # Skip aggregation directories
                        if item.name not in ['aggregated', 'daily', 'cache']:
                            assets.append(item.name)
        except Exception as e:
            logger.error(f"Error getting available assets: {e}")
        return assets
    
    def _get_raw_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                           asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for raw data tier."""
        # Raw data: asset_id/yyyy/mm/dd/hh/sensor.parquet
        return self._build_hierarchical_paths("", sensors, start_time, end_time, asset_ids)
    
    def _get_aggregated_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                                  asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for aggregated data tier."""
        # Aggregated data: aggregated/asset_id/yyyy/mm/dd/sensor.parquet
        return self._build_hierarchical_paths("aggregated", sensors, start_time, end_time, asset_ids, include_hour=False)
    
    def _get_daily_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                             asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for daily data tier."""
        # Daily data: daily/asset_id/yyyy/mm/sensor.parquet
        return self._build_hierarchical_paths("daily", sensors, start_time, end_time, asset_ids, include_day=False, include_hour=False)
    
    def _build_hierarchical_paths(self, prefix: str, sensors: List[str], start_time: datetime, end_time: datetime,
                                 asset_ids: Optional[List[str]] = None, include_day: bool = True, include_hour: bool = True) -> List[str]:
        """Build hierarchical file paths for time range."""
        paths = []
        
        # Get available assets if not specified
        if asset_ids is None:
            asset_ids = self._get_available_assets()
        
        # Generate paths for time range
        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        
        while current_time < end_time:
            for asset_id in asset_ids:
                for sensor in sensors:
                    # Build path based on tier
                    path_parts = []
                    
                    if prefix:
                        path_parts.append(prefix)
                    
                    path_parts.extend([
                        asset_id,
                        f"{current_time.year:04d}",
                        f"{current_time.month:02d}"
                    ])
                    
                    if include_day:
                        path_parts.append(f"{current_time.day:02d}")
                    
                    if include_hour:
                        path_parts.append(f"{current_time.hour:02d}")
                    
                    path_parts.append(f"{sensor}.parquet")
                    
                    file_path = "/".join(path_parts)
                    
                    # Check if file exists before adding
                    if self.local.file_exists(file_path):
                        paths.append(file_path)
            
            # Increment time based on tier granularity
            if include_hour:
                current_time = current_time.replace(hour=current_time.hour + 1)
                if current_time.hour == 0:
                    current_time = current_time.replace(day=current_time.day + 1, hour=0)
                    if current_time.day == 1 and current_time != start_time.replace(minute=0, second=0, microsecond=0, hour=0, day=1):
                        # Handle month rollover
                        if current_time.month == 13:
                            current_time = current_time.replace(year=current_time.year + 1, month=1)
            elif include_day:
                current_time = current_time.replace(day=current_time.day + 1)
                if current_time.day == 1 and current_time != start_time.replace(minute=0, second=0, microsecond=0, hour=0, day=1):
                    # Handle month rollover
                    if current_time.month == 13:
                        current_time = current_time.replace(year=current_time.year + 1, month=1)
            else:
                # Monthly increment
                if current_time.month == 12:
                    current_time = current_time.replace(year=current_time.year + 1, month=1)
                else:
                    current_time = current_time.replace(month=current_time.month + 1)
        
        return paths
    
    def create_aggregated_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                              interval_minutes: int = 1) -> bool:
        """Create aggregated data from raw data."""
        try:
            logger.info(f"Creating aggregated data for {len(sensors)} sensors from {start_time} to {end_time}")
            
            # Read raw data
            raw_data = self.read_raw_data(sensors, start_time, end_time)
            
            if raw_data.empty:
                logger.warning("No raw data found for aggregation")
                return False
            
            # Group by time intervals and aggregate
            raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])
            
            # Create time buckets
            raw_data['time_bucket'] = raw_data['timestamp'].dt.floor(f'{interval_minutes}min')
            
            # Aggregate numeric columns
            numeric_columns = raw_data.select_dtypes(include=['number']).columns
            
            aggregated = raw_data.groupby(['time_bucket', 'sensor_name', 'asset_id']).agg({
                **{col: ['mean', 'min', 'max'] for col in numeric_columns if col != 'timestamp'},
                'timestamp': 'first'  # Keep first timestamp in bucket
            }).reset_index()
            
            # Flatten column names
            aggregated.columns = ['_'.join(col).strip('_') for col in aggregated.columns.values]
            
            # Save aggregated data
            aggregated_path = self.data_path / "aggregated"
            aggregated_path.mkdir(exist_ok=True)
            
            # Group by asset and sensor for saving
            for (asset_id, sensor_name), group in aggregated.groupby(['asset_id', 'sensor_name']):
                # Create directory structure
                asset_path = aggregated_path / asset_id / f"{start_time.year:04d}" / f"{start_time.month:02d}" / f"{start_time.day:02d}"
                asset_path.mkdir(parents=True, exist_ok=True)
                
                # Save file
                file_path = asset_path / f"{sensor_name}.parquet"
                group.to_parquet(file_path, index=False)
            
            logger.info(f"Created aggregated data for {len(aggregated)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error creating aggregated data: {e}")
            return False
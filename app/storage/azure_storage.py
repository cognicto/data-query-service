"""
Azure Blob Storage backend for sensor data access.
"""

import logging
from typing import List, Dict, Optional
import pandas as pd
from io import BytesIO
from datetime import datetime
import concurrent.futures
from threading import Lock

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import AzureError, ResourceNotFoundError

from app.config import AzureConfig
from app.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class AzureStorageBackend(StorageBackend):
    """Azure Blob Storage backend for reading sensor data."""
    
    def __init__(self, config: AzureConfig):
        """Initialize Azure storage backend with SAS token authentication only."""
        self.config = config
        
        # Validate required SAS token configuration
        if not config.blob_endpoint or not config.sas_token or not config.container_name:
            raise ValueError(
                "Azure SAS token authentication requires: AZURE_BLOB_ENDPOINT, AZURE_SAS_TOKEN, and AZURE_CONTAINER_NAME"
            )
        
        # Clean up SAS token (remove leading ?)
        sas_token = config.sas_token.lstrip('?')
        
        # Create container URL with SAS token
        container_url = f"{config.blob_endpoint}/{config.container_name}?{sas_token}"
        
        # Use ContainerClient directly with SAS token
        self.container_client = ContainerClient.from_container_url(container_url)
        
        # Create BlobServiceClient for file listing
        self.blob_service_client = BlobServiceClient(
            account_url=f"{config.blob_endpoint}?{sas_token}"
        )
        
        self.container_name = config.container_name
        self._file_cache = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300  # 5 minutes
        
        logger.info(f"Initialized Azure storage backend with SAS token for container: {self.container_name}")
    
    def list_files(self, prefix: str = "") -> List[str]:
        """List files in Azure container with optional prefix filter."""
        try:
            with self._cache_lock:
                cache_key = f"list_files_{prefix}"
                cached = self._file_cache.get(cache_key)
                
                if cached and (datetime.utcnow() - cached['timestamp']).seconds < self._cache_ttl:
                    return cached['files']
            
            container_client = self.blob_service_client.get_container_client(self.container_name)
            files = []
            
            # List blobs with prefix filter
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blob_list:
                if blob.name.endswith('.parquet'):
                    files.append(blob.name)
            
            # Cache results
            with self._cache_lock:
                self._file_cache[cache_key] = {
                    'files': files,
                    'timestamp': datetime.utcnow()
                }
            
            logger.debug(f"Listed {len(files)} files with prefix '{prefix}'")
            return files
            
        except AzureError as e:
            logger.error(f"Failed to list files from Azure: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing files: {e}")
            return []
    
    def read_parquet(self, file_path: str) -> pd.DataFrame:
        """Read a Parquet file from Azure Blob Storage."""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=file_path
            )
            
            # Download blob content
            blob_data = blob_client.download_blob()
            
            # Read into pandas DataFrame
            with BytesIO() as buffer:
                blob_data.readinto(buffer)
                buffer.seek(0)
                df = pd.read_parquet(buffer)
            
            # Handle new optimized schema that only contains timestamp/value
            if len(df.columns) == 2 and 'timestamp' in df.columns and 'value' in df.columns:
                # Extract metadata from file path for new schema
                metadata = self._extract_metadata_from_path(file_path)
                if metadata['asset_id'] != 'unknown':
                    df['asset_id'] = metadata['asset_id']
                if metadata['sensor_name'] != 'unknown':
                    df['sensor_name'] = metadata['sensor_name']
            
            # Map daqid to asset_id if daqid exists (for TimescaleDB data structure)
            if 'daqid' in df.columns and 'asset_id' not in df.columns:
                df['asset_id'] = df['daqid']
            
            logger.debug(f"Read {len(df)} rows from {file_path}")
            return df
            
        except ResourceNotFoundError:
            logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
        except AzureError as e:
            logger.error(f"Azure error reading {file_path}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return pd.DataFrame()
    
    def _extract_metadata_from_path(self, file_path: str) -> Dict[str, str]:
        """Extract asset_id and sensor_name from file path for new optimized schema."""
        try:
            # Handle different path formats:
            # Raw: asset_id/yyyy/mm/dd/hh/sensor_YYYYMMDD_HH.parquet
            # Aggregated: aggregated/asset_id/yyyy/mm/dd/hh/sensor_minute.parquet
            # Daily: daily/asset_id/yyyy/mm/sensor_day.parquet
            
            parts = file_path.split('/')
            
            if '/aggregated/' in file_path:
                # aggregated/asset_id/yyyy/mm/dd/hh/sensor_minute.parquet
                agg_parts = file_path.split('/aggregated/')
                if len(agg_parts) > 1:
                    remaining = agg_parts[1].split('/')
                    if len(remaining) >= 1:
                        asset_id = remaining[0]
                        filename = remaining[-1]
                        # Handle both minute and hourly aggregations
                        sensor_name = filename.replace('_minute.parquet', '').replace('_hour.parquet', '').replace('.parquet', '')
                        return {'asset_id': asset_id, 'sensor_name': sensor_name}
            elif '/daily/' in file_path:
                # daily/asset_id/yyyy/mm/sensor_day.parquet
                daily_parts = file_path.split('/daily/')
                if len(daily_parts) > 1:
                    remaining = daily_parts[1].split('/')
                    if len(remaining) >= 1:
                        asset_id = remaining[0]
                        filename = remaining[-1]
                        sensor_name = filename.replace('_day.parquet', '').replace('.parquet', '')
                        return {'asset_id': asset_id, 'sensor_name': sensor_name}
            else:
                # Raw data: asset_id/yyyy/mm/dd/hh/sensor_YYYYMMDD_HH.parquet
                if len(parts) >= 6:
                    asset_id = parts[0] if parts[0] else parts[1]  # Handle leading slash
                    filename = parts[-1]
                    # Extract sensor name from new format: sensor_YYYYMMDD_HH.parquet
                    if '_' in filename:
                        sensor_name = filename.rsplit('_', 2)[0]  # Get everything before last two underscores
                    else:
                        sensor_name = filename.replace('.parquet', '')
                    return {'asset_id': asset_id, 'sensor_name': sensor_name}
            
            # Fallback
            return {'asset_id': 'unknown', 'sensor_name': 'unknown'}
            
        except Exception as e:
            logger.error(f"Error extracting metadata from path {file_path}: {e}")
            return {'asset_id': 'unknown', 'sensor_name': 'unknown'}
    
    def read_multiple_parquet(self, file_paths: List[str]) -> pd.DataFrame:
        """Read multiple Parquet files in parallel."""
        if not file_paths:
            return pd.DataFrame()
        
        dataframes = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
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
        """Check if a file exists in Azure Blob Storage."""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=file_path
            )
            return blob_client.exists()
            
        except Exception as e:
            logger.debug(f"Error checking file existence {file_path}: {e}")
            return False
    
    def get_file_info(self, file_path: str) -> Dict:
        """Get file metadata from Azure Blob Storage."""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=file_path
            )
            
            properties = blob_client.get_blob_properties()
            
            return {
                'name': file_path,
                'size': properties.size,
                'size_mb': properties.size / (1024 * 1024),
                'last_modified': properties.last_modified,
                'content_type': properties.content_settings.content_type if properties.content_settings else None,
                'etag': properties.etag
            }
            
        except Exception as e:
            logger.error(f"Error getting file info for {file_path}: {e}")
            return {'error': str(e)}
    
    def health_check(self) -> Dict:
        """Perform health check on Azure storage."""
        try:
            # Test connection by listing container
            container_client = self.blob_service_client.get_container_client(self.container_name)
            container_properties = container_client.get_container_properties()
            
            # Test read access
            blobs = list(container_client.list_blobs(max_results=1))
            
            return {
                'healthy': True,
                'container_exists': True,
                'container_name': self.container_name,
                'last_modified': container_properties.last_modified,
                'sample_files_accessible': len(blobs) > 0,
                'cache_entries': len(self._file_cache)
            }
            
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'container_name': self.container_name,
                'cache_entries': len(self._file_cache)
            }
    
    def clear_cache(self):
        """Clear file listing cache."""
        with self._cache_lock:
            self._file_cache.clear()
        logger.info("Cleared Azure storage cache")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        with self._cache_lock:
            return {
                'cache_entries': len(self._file_cache),
                'cache_keys': list(self._file_cache.keys())
            }


class AzureAggregatedReader:
    """Specialized reader for aggregated data tiers in Azure."""
    
    def __init__(self, azure_backend: AzureStorageBackend):
        """Initialize with Azure backend."""
        self.azure = azure_backend
    
    def read_raw_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                     asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read raw data (1-second precision) from Azure."""
        file_paths = self._get_raw_file_paths(sensors, start_time, end_time, asset_ids)
        return self.azure.read_multiple_parquet(file_paths)
    
    def read_aggregated_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                           asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read pre-aggregated data (1-minute precision) from Azure."""
        # Look for aggregated files in 'aggregated' prefix
        file_paths = self._get_aggregated_file_paths(sensors, start_time, end_time, asset_ids)
        return self.azure.read_multiple_parquet(file_paths)
    
    def read_hourly_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                        asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read hourly aggregated data from Azure."""
        # Look for hourly aggregated files in 'aggregated' prefix
        file_paths = self._get_hourly_file_paths(sensors, start_time, end_time, asset_ids)
        return self.azure.read_multiple_parquet(file_paths)
    
    def read_daily_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                       asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read daily summary data from Azure."""
        # Look for daily summary files in 'daily' prefix
        file_paths = self._get_daily_file_paths(sensors, start_time, end_time, asset_ids)
        return self.azure.read_multiple_parquet(file_paths)
    
    def _get_raw_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                           asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for raw data tier."""
        # Raw data: [data_prefix/]asset_id/yyyy/mm/dd/hh/sensor_YYYYMMDD_HH.parquet
        return self._build_hierarchical_paths(self.azure.config.data_prefix, sensors, start_time, end_time, asset_ids)
    
    def _get_aggregated_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                                  asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for aggregated (minute-level) data tier."""
        # Aggregated data: [data_prefix/]aggregated/asset_id/yyyy/mm/dd/hh/sensor_minute.parquet
        prefix = f"{self.azure.config.data_prefix}aggregated/" if self.azure.config.data_prefix else "aggregated/"
        return self._build_hierarchical_paths(prefix, sensors, start_time, end_time, asset_ids, include_day=True, include_hour=True)
    
    def _get_hourly_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                              asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for hourly aggregated data tier."""
        # Hourly data: [data_prefix/]aggregated/asset_id/yyyy/mm/dd/sensor_hour.parquet
        prefix = f"{self.azure.config.data_prefix}aggregated/" if self.azure.config.data_prefix else "aggregated/"
        return self._build_hierarchical_paths(prefix, sensors, start_time, end_time, asset_ids, include_day=True, include_hour=False)
    
    def _get_daily_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                             asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for daily data tier."""
        # Daily data: [data_prefix/]daily/asset_id/yyyy/mm/sensor_day.parquet
        prefix = f"{self.azure.config.data_prefix}daily/" if self.azure.config.data_prefix else "daily/"
        return self._build_hierarchical_paths(prefix, sensors, start_time, end_time, asset_ids, include_day=False, include_hour=False)
    
    def _build_hierarchical_paths(self, prefix: str, sensors: List[str], start_time: datetime, end_time: datetime,
                                 asset_ids: Optional[List[str]] = None, include_day: bool = True, include_hour: bool = True) -> List[str]:
        """Build hierarchical file paths for time range."""
        paths = []
        
        # If no specific asset_ids provided, try to find all available assets
        if asset_ids is None:
            # Get available assets from file listing
            all_files = self.azure.list_files(prefix)
            asset_ids = set()
            for file_path in all_files:
                parts = file_path.split('/')
                if len(parts) > 1:
                    if prefix:
                        # Skip prefix in parts
                        prefix_parts = prefix.rstrip('/').split('/')
                        if len(parts) > len(prefix_parts):
                            asset_id = parts[len(prefix_parts)]
                            asset_ids.add(asset_id)
                    else:
                        asset_id = parts[0]
                        asset_ids.add(asset_id)
            asset_ids = list(asset_ids)
        
        # Generate paths for time range
        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        
        while current_time < end_time:
            for asset_id in asset_ids:
                for sensor in sensors:
                    # Build path based on tier
                    path_parts = [prefix.rstrip('/') if prefix else None, asset_id, 
                                f"{current_time.year:04d}", f"{current_time.month:02d}"]
                    
                    # Remove None parts
                    path_parts = [p for p in path_parts if p is not None]
                    
                    if include_day:
                        path_parts.append(f"{current_time.day:02d}")
                    
                    if include_hour:
                        path_parts.append(f"{current_time.hour:02d}")
                    
                    # Use new file naming convention based on prefix and granularity
                    if prefix.startswith("aggregated"):
                        if include_hour:
                            # For minute-level aggregations: sensor_minute.parquet
                            filename = f"{sensor}_minute.parquet"
                        else:
                            # For hourly aggregations: sensor_hour.parquet
                            filename = f"{sensor}_hour.parquet"
                    elif prefix.startswith("daily"):
                        # For daily: sensor_day.parquet  
                        filename = f"{sensor}_day.parquet"
                    else:
                        # For raw data: sensor_YYYYMMDD_HH.parquet
                        date_str = f"{current_time.year:04d}{current_time.month:02d}{current_time.day:02d}_{current_time.hour:02d}"
                        filename = f"{sensor}_{date_str}.parquet"
                    
                    path_parts.append(filename)
                    file_path = "/".join(path_parts)
                    paths.append(file_path)
            
            # Increment time based on tier granularity
            if include_hour:
                current_time = current_time.replace(hour=current_time.hour + 1)
                if current_time.hour == 0:
                    current_time = current_time.replace(day=current_time.day + 1, hour=0)
            elif include_day:
                from calendar import monthrange
                if current_time.day == monthrange(current_time.year, current_time.month)[1]:
                    if current_time.month == 12:
                        current_time = current_time.replace(year=current_time.year + 1, month=1, day=1)
                    else:
                        current_time = current_time.replace(month=current_time.month + 1, day=1)
                else:
                    current_time = current_time.replace(day=current_time.day + 1)
            else:
                # Monthly increment
                if current_time.month == 12:
                    current_time = current_time.replace(year=current_time.year + 1, month=1)
                else:
                    current_time = current_time.replace(month=current_time.month + 1)
        
        return paths
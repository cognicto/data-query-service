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
        """Initialize Azure storage backend."""
        self.config = config
        self.container_client = None
        
        # Check if using new blob_endpoint + sas_token pattern
        if config.blob_endpoint and config.sas_token:
            # Clean up SAS token (remove leading ?)
            sas_token = config.sas_token.lstrip('?')
            
            # Create container URL with SAS token
            container_url = f"{config.blob_endpoint}/{config.container_name}?{sas_token}"
            
            # Use ContainerClient directly with SAS token
            self.container_client = ContainerClient.from_container_url(container_url)
            
            # Also create BlobServiceClient for compatibility
            self.blob_service_client = BlobServiceClient(
                account_url=f"{config.blob_endpoint}?{sas_token}"
            )
        # Fall back to old method using storage_account and storage_key
        elif config.storage_account and config.storage_key:
            # Check if using SAS token (starts with 'sv=')
            if config.storage_key and config.storage_key.startswith('sv='):
                # Using SAS token
                self.blob_service_client = BlobServiceClient(
                    account_url=f"https://{config.storage_account}.blob.core.windows.net?{config.storage_key}"
                )
            else:
                # Using storage key
                self.blob_service_client = BlobServiceClient(
                    account_url=f"https://{config.storage_account}.blob.core.windows.net",
                    credential=config.storage_key
                )
            
            self.container_client = self.blob_service_client.get_container_client(config.container_name)
        else:
            raise ValueError("Azure credentials not configured - either provide blob_endpoint + sas_token or storage_account + storage_key")
        
        self.container_name = config.container_name
        self._file_cache = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300  # 5 minutes
        
        logger.info(f"Initialized Azure storage backend for container: {self.container_name}")
    
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
    
    def read_daily_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                       asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read daily summary data (hourly precision) from Azure."""
        # Look for daily summary files in 'daily' prefix
        file_paths = self._get_daily_file_paths(sensors, start_time, end_time, asset_ids)
        return self.azure.read_multiple_parquet(file_paths)
    
    def _get_raw_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                           asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for raw data tier."""
        # Raw data is in the root level: asset_id/yyyy/mm/dd/hh/sensor.parquet
        return self._build_hierarchical_paths("", sensors, start_time, end_time, asset_ids)
    
    def _get_aggregated_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                                  asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for aggregated data tier."""
        # Aggregated data: aggregated/asset_id/yyyy/mm/dd/sensor.parquet (no hour level)
        return self._build_hierarchical_paths("aggregated/", sensors, start_time, end_time, asset_ids, include_hour=False)
    
    def _get_daily_file_paths(self, sensors: List[str], start_time: datetime, end_time: datetime,
                             asset_ids: Optional[List[str]] = None) -> List[str]:
        """Get file paths for daily data tier."""
        # Daily data: daily/asset_id/yyyy/mm/sensor.parquet (no day level)
        return self._build_hierarchical_paths("daily/", sensors, start_time, end_time, asset_ids, include_day=False, include_hour=False)
    
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
                    
                    path_parts.append(f"{sensor}.parquet")
                    
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
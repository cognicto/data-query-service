"""
Base storage interface for sensor data access.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from pathlib import Path
import pandas as pd


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def list_files(self, prefix: str = "") -> List[str]:
        """List files with optional prefix filter."""
        pass
    
    @abstractmethod
    def read_parquet(self, file_path: str) -> pd.DataFrame:
        """Read a Parquet file and return as DataFrame."""
        pass
    
    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists."""
        pass
    
    @abstractmethod
    def get_file_info(self, file_path: str) -> Dict:
        """Get file metadata (size, modified time, etc.)."""
        pass
    
    @abstractmethod
    def health_check(self) -> Dict:
        """Perform health check on storage backend."""
        pass


class SensorDataReader:
    """High-level interface for reading sensor data from storage backends."""
    
    def __init__(self, storage_backend: StorageBackend):
        """Initialize with a storage backend."""
        self.storage = storage_backend
        self._file_cache = {}  # Simple file listing cache
        
    def get_available_sensors(self, asset_id: Optional[str] = None) -> List[str]:
        """Get list of available sensors, optionally filtered by asset."""
        try:
            # List all files and extract sensor names
            files = self.storage.list_files()
            sensors = set()
            
            for file_path in files:
                if file_path.endswith('.parquet'):
                    # Parse new file path format: asset_id/yyyy/mm/dd/hh/tablename_YYYYMMDD_HH.parquet
                    # Skip aggregated directories to avoid duplicates
                    if '/aggregated/' in file_path or '/daily/' in file_path:
                        continue
                        
                    parts = file_path.split('/')
                    if len(parts) >= 6:
                        file_asset_id = parts[0] if parts[0] else parts[1]  # Handle leading slash
                        if file_asset_id and (asset_id is None or file_asset_id == asset_id):
                            sensor_file = parts[-1]  # Last part is filename
                            # Extract sensor name from new filename format: tablename_YYYYMMDD_HH.parquet
                            if '_' in sensor_file:
                                # Split by underscore and remove last two parts (date and hour)
                                sensor_name = sensor_file.rsplit('_', 2)[0]
                            else:
                                sensor_name = sensor_file.replace('.parquet', '')
                            sensors.add(sensor_name)
            
            return sorted(list(sensors))
            
        except Exception as e:
            print(f"Error getting available sensors: {e}")
            return []
    
    def get_available_assets(self) -> List[str]:
        """Get list of available assets."""
        try:
            files = self.storage.list_files()
            assets = set()
            
            for file_path in files:
                if file_path.endswith('.parquet'):
                    # Skip aggregated and daily directories to avoid counting them as assets
                    if '/aggregated/' in file_path or '/daily/' in file_path:
                        # For aggregated data, extract asset from: aggregated/asset_id/...
                        if '/aggregated/' in file_path:
                            parts = file_path.split('/aggregated/')
                            if len(parts) > 1:
                                asset_parts = parts[1].split('/')
                                if len(asset_parts) >= 1:
                                    assets.add(asset_parts[0])
                        elif '/daily/' in file_path:
                            parts = file_path.split('/daily/')
                            if len(parts) > 1:
                                asset_parts = parts[1].split('/')
                                if len(asset_parts) >= 1:
                                    assets.add(asset_parts[0])
                        continue
                        
                    # Parse raw data file path to extract asset_id
                    parts = file_path.split('/')
                    if len(parts) >= 6:
                        asset_id = parts[0] if parts[0] else parts[1]  # Handle leading slash
                        if asset_id and asset_id not in ['aggregated', 'daily']:
                            assets.add(asset_id)
            
            return sorted(list(assets))
            
        except Exception as e:
            print(f"Error getting available assets: {e}")
            return []
    
    def get_time_range(self, sensors: List[str], asset_ids: Optional[List[str]] = None) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the available time range for given sensors."""
        try:
            files = self._get_relevant_files(sensors, asset_ids)
            
            if not files:
                return None, None
            
            min_date = None
            max_date = None
            
            for file_path in files:
                # Extract date from file path: asset_id/yyyy/mm/dd/hh/sensor.parquet
                parts = file_path.split('/')
                if len(parts) >= 6:
                    try:
                        year = int(parts[-5])
                        month = int(parts[-4])
                        day = int(parts[-3])
                        hour = int(parts[-2])
                        
                        file_date = datetime(year, month, day, hour)
                        
                        if min_date is None or file_date < min_date:
                            min_date = file_date
                        if max_date is None or file_date > max_date:
                            max_date = file_date
                            
                    except (ValueError, IndexError):
                        continue
            
            return min_date, max_date
            
        except Exception as e:
            print(f"Error getting time range: {e}")
            return None, None
    
    def _get_relevant_files(self, sensors: List[str], asset_ids: Optional[List[str]] = None, 
                           start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[str]:
        """Get list of files relevant to the query parameters."""
        try:
            all_files = self.storage.list_files()
            relevant_files = []
            
            for file_path in all_files:
                if not file_path.endswith('.parquet'):
                    continue
                
                # Parse file path
                parts = file_path.split('/')
                if len(parts) < 6:
                    continue
                
                try:
                    # Handle different directory structures
                    if '/aggregated/' in file_path or '/daily/' in file_path:
                        # Extract metadata from aggregated/daily paths
                        metadata = self._extract_metadata_from_path(file_path)
                        asset_id = metadata['asset_id']
                        sensor_name = metadata['sensor_name']
                        
                        # Extract time info from path
                        if '/aggregated/' in file_path:
                            # aggregated/asset_id/yyyy/mm/dd/hh/sensor_minute.parquet
                            agg_parts = file_path.split('/aggregated/')[1].split('/')
                            if len(agg_parts) >= 5:
                                year = int(agg_parts[1])
                                month = int(agg_parts[2])
                                day = int(agg_parts[3])
                                hour = int(agg_parts[4])
                            else:
                                continue
                        elif '/daily/' in file_path:
                            # daily/asset_id/yyyy/mm/sensor_day.parquet
                            daily_parts = file_path.split('/daily/')[1].split('/')
                            if len(daily_parts) >= 3:
                                year = int(daily_parts[1])
                                month = int(daily_parts[2])
                                day = 1  # Daily files cover entire month
                                hour = 0
                            else:
                                continue
                    else:
                        # Raw data: asset_id/yyyy/mm/dd/hh/sensor_YYYYMMDD_HH.parquet
                        asset_id = parts[0] if parts[0] else parts[1]
                        year = int(parts[-5])
                        month = int(parts[-4])
                        day = int(parts[-3])
                        hour = int(parts[-2])
                        sensor_file = parts[-1]
                        # Extract sensor name from new filename format: sensor_YYYYMMDD_HH.parquet
                        if '_' in sensor_file:
                            sensor_name = sensor_file.rsplit('_', 2)[0]  # Get everything before last two underscores
                        else:
                            sensor_name = sensor_file.replace('.parquet', '')
                    
                    # Filter by asset_id
                    if asset_ids and asset_id not in asset_ids:
                        continue
                    
                    # Filter by sensor
                    if sensor_name not in sensors:
                        continue
                    
                    # Filter by time range
                    if start_time or end_time:
                        file_time = datetime(year, month, day, hour)
                        
                        if start_time and file_time < start_time:
                            continue
                        if end_time and file_time >= end_time:
                            continue
                    
                    relevant_files.append(file_path)
                    
                except (ValueError, IndexError):
                    continue
            
            return sorted(relevant_files)
            
        except Exception as e:
            print(f"Error getting relevant files: {e}")
            return []
    
    def read_sensor_data(self, sensors: List[str], start_time: datetime, end_time: datetime,
                        asset_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Read sensor data for given parameters."""
        try:
            relevant_files = self._get_relevant_files(sensors, asset_ids, start_time, end_time)
            
            if not relevant_files:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=['timestamp', 'value', 'sensor_name', 'asset_id'])
            
            dataframes = []
            
            for file_path in relevant_files:
                try:
                    df = self.storage.read_parquet(file_path)
                    
                    if df.empty:
                        continue
                    
                    # Extract metadata from file path since new schema only has timestamp/value
                    metadata = self._extract_metadata_from_path(file_path)
                    
                    # Add metadata columns if they don't exist (new 2-column schema)
                    if 'asset_id' not in df.columns and metadata['asset_id']:
                        df['asset_id'] = metadata['asset_id']
                    if 'sensor_name' not in df.columns and metadata['sensor_name']:
                        df['sensor_name'] = metadata['sensor_name']
                    
                    # Handle daqid -> asset_id mapping for backward compatibility
                    if 'daqid' in df.columns and 'asset_id' not in df.columns:
                        df['asset_id'] = df['daqid']
                    
                    # Ensure timestamp column is datetime
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        
                        # Filter by time range (file-level filtering might not be enough)
                        mask = (df['timestamp'] >= start_time) & (df['timestamp'] < end_time)
                        df = df[mask]
                    
                    if not df.empty:
                        dataframes.append(df)
                        
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                    continue
            
            if not dataframes:
                return pd.DataFrame(columns=['timestamp', 'value', 'sensor_name', 'asset_id'])
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Sort by timestamp
            if 'timestamp' in combined_df.columns:
                combined_df = combined_df.sort_values('timestamp')
            
            return combined_df
            
        except Exception as e:
            print(f"Error reading sensor data: {e}")
            return pd.DataFrame(columns=['timestamp', 'value', 'sensor_name', 'asset_id'])
    
    def _extract_metadata_from_path(self, file_path: str) -> Dict[str, str]:
        """Extract asset_id and sensor_name from file path since new schema only stores timestamp/value."""
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
                        sensor_name = filename.replace('_minute.parquet', '').replace('.parquet', '')
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
            print(f"Error extracting metadata from path {file_path}: {e}")
            return {'asset_id': 'unknown', 'sensor_name': 'unknown'}
    
    def get_storage_stats(self) -> Dict:
        """Get storage statistics."""
        try:
            files = self.storage.list_files()
            parquet_files = [f for f in files if f.endswith('.parquet')]
            
            # Count by sensor and asset
            sensor_counts = {}
            asset_counts = {}
            
            for file_path in parquet_files:
                parts = file_path.split('/')
                if len(parts) >= 6:
                    try:
                        asset_id = parts[0] if parts[0] else parts[1]
                        sensor_file = parts[-1]
                        # Extract table name from filename: tablename_YYYYMMDD_HH.parquet
                        if '_' in sensor_file:
                            sensor_name = sensor_file.rsplit('_', 2)[0]  # Get everything before last two underscores
                        else:
                            sensor_name = sensor_file.replace('.parquet', '')
                        
                        sensor_counts[sensor_name] = sensor_counts.get(sensor_name, 0) + 1
                        asset_counts[asset_id] = asset_counts.get(asset_id, 0) + 1
                        
                    except (ValueError, IndexError):
                        continue
            
            return {
                'total_files': len(parquet_files),
                'sensors': sensor_counts,
                'assets': asset_counts,
                'storage_backend': self.storage.__class__.__name__
            }
            
        except Exception as e:
            print(f"Error getting storage stats: {e}")
            return {'error': str(e)}
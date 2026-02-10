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
                    # Parse file path: asset_id/yyyy/mm/dd/hh/tablename_YYYYMMDD_HH.parquet
                    parts = file_path.split('/')
                    if len(parts) >= 6:
                        file_asset_id = parts[0] if parts[0] else parts[1]  # Handle leading slash
                        if file_asset_id and (asset_id is None or file_asset_id == asset_id):
                            sensor_file = parts[-1]  # Last part is filename
                            # Extract table name from filename: tablename_YYYYMMDD_HH.parquet
                            if '_' in sensor_file:
                                sensor_name = sensor_file.rsplit('_', 2)[0]  # Get everything before last two underscores
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
                    # Parse file path to extract asset_id
                    parts = file_path.split('/')
                    if len(parts) >= 6:
                        asset_id = parts[0] if parts[0] else parts[1]  # Handle leading slash
                        if asset_id:
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
                    asset_id = parts[0] if parts[0] else parts[1]
                    year = int(parts[-5])
                    month = int(parts[-4])
                    day = int(parts[-3])
                    hour = int(parts[-2])
                    sensor_file = parts[-1]
                    # Extract table name from filename: tablename_YYYYMMDD_HH.parquet
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
                return pd.DataFrame(columns=['timestamp', 'sensor_name', 'asset_id'])
            
            dataframes = []
            
            for file_path in relevant_files:
                try:
                    df = self.storage.read_parquet(file_path)
                    
                    if df.empty:
                        continue
                    
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
                return pd.DataFrame(columns=['timestamp', 'sensor_name', 'asset_id'])
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Sort by timestamp
            if 'timestamp' in combined_df.columns:
                combined_df = combined_df.sort_values('timestamp')
            
            return combined_df
            
        except Exception as e:
            print(f"Error reading sensor data: {e}")
            return pd.DataFrame(columns=['timestamp', 'sensor_name', 'asset_id'])
    
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
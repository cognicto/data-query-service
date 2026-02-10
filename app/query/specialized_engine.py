"""
Specialized query engines for raw and aggregated data APIs.
"""

import logging
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import time

from app.config import AppConfig, AggregationMethod
from app.query.engine import SmartQueryEngine
from app.aggregation.aggregator import SmartAggregationEngine

logger = logging.getLogger(__name__)


class RawDataEngine:
    """Specialized engine for raw data queries (1-second interval)."""
    
    def __init__(self, base_engine: SmartQueryEngine, config: AppConfig):
        """Initialize raw data engine."""
        self.base_engine = base_engine
        self.config = config
        self.max_datapoints = config.query.max_absolute_datapoints
        
    def query_raw_data(self, sensor_types: List[str], start_date: datetime, 
                      end_date: datetime) -> Dict:
        """Query raw sensor data with 1-second precision."""
        start_exec_time = time.time()
        
        try:
            # Calculate expected data points for validation
            duration_seconds = (end_date - start_date).total_seconds()
            expected_points = duration_seconds * len(sensor_types)  # 1 point per second per sensor
            
            # Check if query exceeds max datapoints limit
            truncated = False
            actual_end_date = end_date
            
            if expected_points > self.max_datapoints:
                # Calculate truncated end time
                max_duration_seconds = self.max_datapoints / len(sensor_types)
                actual_end_date = start_date + timedelta(seconds=max_duration_seconds)
                truncated = True
                logger.info(f"Truncating raw query: {expected_points} points > {self.max_datapoints} limit")
            
            # Force raw tier usage by using short duration
            result = self.base_engine.query_sensor_data(
                sensors=sensor_types,
                start_time=start_date,
                end_time=actual_end_date,
                asset_ids=None,
                interval_ms=1000,  # Force 1-second intervals
                max_datapoints=self.max_datapoints,
                aggregation='last'  # Use 'last' to preserve original values
            )
            
            # Convert DataFrame to list of dictionaries
            if not result.data.empty:
                # Rename columns to match API contract
                data_dict = result.data.copy()
                if 'sensor_name' in data_dict.columns:
                    data_dict = data_dict.rename(columns={'sensor_name': 'sensor_type'})
                
                data_list = data_dict.to_dict('records')
                
                # Ensure timestamps are properly formatted
                for record in data_list:
                    if 'timestamp' in record and pd.notna(record['timestamp']):
                        record['timestamp'] = pd.to_datetime(record['timestamp']).isoformat()
            else:
                data_list = []
            
            execution_time_ms = (time.time() - start_exec_time) * 1000
            
            return {
                'data': data_list,
                'metadata': {
                    'total_data_points': len(data_list),
                    'truncated': truncated,
                    'actual_end_date': actual_end_date if truncated else None,
                    'max_datapoints_limit': self.max_datapoints,
                    'interval_ms_used': 1000,  # Always 1 second for raw data
                    'cache_hit': result.cache_hit,
                    'execution_time_ms': execution_time_ms,
                    'tier_used': result.tier_used
                }
            }
            
        except Exception as e:
            logger.error(f"Raw data query failed: {e}")
            execution_time_ms = (time.time() - start_exec_time) * 1000
            
            return {
                'data': [],
                'metadata': {
                    'total_data_points': 0,
                    'truncated': False,
                    'actual_end_date': None,
                    'max_datapoints_limit': self.max_datapoints,
                    'interval_ms_used': 1000,
                    'cache_hit': False,
                    'execution_time_ms': execution_time_ms,
                    'tier_used': 'error'
                }
            }


class AggregatedDataEngine:
    """Specialized engine for aggregated data queries with smart interval calculation."""
    
    def __init__(self, base_engine: SmartQueryEngine, config: AppConfig):
        """Initialize aggregated data engine."""
        self.base_engine = base_engine
        self.config = config
        self.max_datapoints = config.query.max_absolute_datapoints
        self.aggregation_engine = SmartAggregationEngine()
        
    def query_aggregated_data(self, sensor_types: List[str], start_date: datetime, 
                            end_date: datetime, interval_ms: Optional[int], 
                            aggregation_type: str) -> Dict:
        """Query aggregated sensor data with smart optimization."""
        start_exec_time = time.time()
        
        try:
            # Calculate duration for smart interval calculation
            duration_hours = (end_date - start_date).total_seconds() / 3600
            
            # Calculate optimal interval if not provided
            calculated_interval_ms = interval_ms
            if interval_ms is None:
                calculated_interval_ms = self._calculate_optimal_interval(
                    duration_hours, len(sensor_types), self.max_datapoints
                )
                logger.info(f"Auto-calculated interval: {calculated_interval_ms}ms for {duration_hours:.1f}h duration")
            
            # Map aggregation type to internal enum
            agg_method = self._map_aggregation_type(aggregation_type)
            
            # Try to use pre-computed aggregations first
            result = self._get_precomputed_aggregated_data(
                sensor_types, start_date, end_date, calculated_interval_ms, agg_method
            )
            
            # Fallback to base engine if no pre-computed data available
            if result is None:
                logger.debug("No suitable pre-computed aggregations found, using base engine")
                result = self.base_engine.query_sensor_data(
                    sensors=sensor_types,
                    start_time=start_date,
                    end_time=end_date,
                    asset_ids=None,
                    interval_ms=calculated_interval_ms,
                    max_datapoints=self.max_datapoints,
                    aggregation=agg_method
                )
            
            # Check if we need to truncate based on max_datapoints
            truncated = result.truncated
            actual_end_date = result.actual_end_time if truncated else None
            
            # Convert DataFrame to list of dictionaries
            if not result.data.empty:
                # Rename columns to match API contract
                data_dict = result.data.copy()
                if 'sensor_name' in data_dict.columns:
                    data_dict = data_dict.rename(columns={'sensor_name': 'sensor_type'})
                
                data_list = data_dict.to_dict('records')
                
                # Ensure timestamps are properly formatted
                for record in data_list:
                    if 'timestamp' in record and pd.notna(record['timestamp']):
                        record['timestamp'] = pd.to_datetime(record['timestamp']).isoformat()
            else:
                data_list = []
            
            execution_time_ms = (time.time() - start_exec_time) * 1000
            
            return {
                'data': data_list,
                'metadata': {
                    'total_data_points': len(data_list),
                    'truncated': truncated,
                    'actual_end_date': actual_end_date,
                    'max_datapoints_limit': self.max_datapoints,
                    'interval_ms_used': calculated_interval_ms,
                    'cache_hit': result.cache_hit,
                    'execution_time_ms': execution_time_ms,
                    'tier_used': result.tier_used
                }
            }
            
        except Exception as e:
            logger.error(f"Aggregated data query failed: {e}")
            execution_time_ms = (time.time() - start_exec_time) * 1000
            
            return {
                'data': [],
                'metadata': {
                    'total_data_points': 0,
                    'truncated': False,
                    'actual_end_date': None,
                    'max_datapoints_limit': self.max_datapoints,
                    'interval_ms_used': interval_ms or 60000,
                    'cache_hit': False,
                    'execution_time_ms': execution_time_ms,
                    'tier_used': 'error'
                }
            }
    
    def _calculate_optimal_interval(self, duration_hours: float, num_sensors: int, 
                                   max_datapoints: int) -> int:
        """Calculate optimal interval to stay under max_datapoints."""
        # Total duration in milliseconds
        duration_ms = duration_hours * 3600 * 1000
        
        # Calculate points per sensor to stay under limit
        max_points_per_sensor = max_datapoints // max(1, num_sensors)
        
        # Calculate minimum interval needed
        min_interval_ms = duration_ms / max_points_per_sensor
        
        # Round up to nearest standard interval for better caching
        standard_intervals = [
            1000,      # 1 second
            5000,      # 5 seconds  
            10000,     # 10 seconds
            30000,     # 30 seconds
            60000,     # 1 minute
            300000,    # 5 minutes
            600000,    # 10 minutes
            1800000,   # 30 minutes
            3600000,   # 1 hour
            7200000,   # 2 hours
            14400000,  # 4 hours
            21600000,  # 6 hours
            43200000,  # 12 hours
            86400000   # 24 hours
        ]
        
        for interval in standard_intervals:
            if interval >= min_interval_ms:
                return interval
        
        # If no standard interval is large enough, use custom interval
        return max(int(min_interval_ms), 60000)  # At least 1 minute
    
    def _map_aggregation_type(self, aggregation_type: str) -> str:
        """Map API aggregation type to internal enum."""
        mapping = {
            'min': 'min',
            'max': 'max', 
            'mean': 'avg'  # Map 'mean' to internal 'avg'
        }
        return mapping.get(aggregation_type.lower(), 'avg')
    
    def estimate_datapoints(self, sensor_types: List[str], start_date: datetime,
                          end_date: datetime, interval_ms: int) -> int:
        """Estimate number of data points for given parameters."""
        duration_ms = (end_date - start_date).total_seconds() * 1000
        points_per_sensor = duration_ms / interval_ms
        return int(points_per_sensor * len(sensor_types))
    
    def get_recommended_interval(self, sensor_types: List[str], start_date: datetime,
                               end_date: datetime, target_points: Optional[int] = None) -> Dict:
        """Get recommended interval for optimal performance."""
        duration_hours = (end_date - start_date).total_seconds() / 3600
        target = target_points or (self.max_datapoints // 2)  # Use half of max for better UX
        
        optimal_interval = self._calculate_optimal_interval(
            duration_hours, len(sensor_types), target
        )
        
        estimated_points = self.estimate_datapoints(
            sensor_types, start_date, end_date, optimal_interval
        )
        
        return {
            'recommended_interval_ms': optimal_interval,
            'estimated_datapoints': estimated_points,
            'duration_hours': duration_hours,
            'max_datapoints_limit': self.max_datapoints
        }
    
    def _get_precomputed_aggregated_data(self, sensor_types: List[str], start_date: datetime,
                                       end_date: datetime, interval_ms: int, agg_method: str):
        """Attempt to get pre-computed aggregated data from storage service."""
        try:
            duration_hours = (end_date - start_date).total_seconds() / 3600
            
            # Determine which pre-computed aggregation tier to use
            if interval_ms >= 3600000 or duration_hours > 168:  # 1+ hours or 7+ days
                tier = "daily"
                data = self._get_precomputed_daily_data(sensor_types, start_date, end_date, agg_method)
            elif interval_ms >= 60000 or duration_hours > 24:  # 1+ minutes or 1+ days
                tier = "hourly"
                data = self._get_precomputed_hourly_data(sensor_types, start_date, end_date, agg_method)
            elif interval_ms >= 60000:  # 1+ minutes
                tier = "minute"
                data = self._get_precomputed_minute_data(sensor_types, start_date, end_date, agg_method)
            else:
                return None  # No suitable pre-computed data for sub-minute intervals
            
            if data is not None and not data.empty:
                logger.info(f"Using pre-computed {tier} aggregations for query")
                
                # Create a mock result object similar to base_engine response
                class MockResult:
                    def __init__(self, df, tier_name):
                        self.data = df
                        self.cache_hit = False  # Pre-computed data is always fresh
                        self.tier_used = f"precomputed_{tier_name}"
                        self.truncated = len(df) >= self.max_datapoints if hasattr(self, 'max_datapoints') else False
                        self.actual_end_time = None
                
                return MockResult(data, tier)
            
            return None
            
        except Exception as e:
            logger.error(f"Error accessing pre-computed aggregations: {e}")
            return None
    
    def _get_precomputed_minute_data(self, sensor_types: List[str], start_date: datetime,
                                   end_date: datetime, agg_method: str) -> Optional[pd.DataFrame]:
        """Get minute-level pre-computed aggregations."""
        try:
            # Look for minute aggregation files from storage service
            # Format: aggregated/asset_id/yyyy/mm/dd/hh/sensor_minute.parquet
            
            data_frames = []
            current_time = start_date.replace(minute=0, second=0, microsecond=0)
            
            # Get available storage backends
            storage_backends = []
            if hasattr(self.base_engine, 'local_backend') and self.base_engine.local_backend:
                storage_backends.append(('local', self.base_engine.local_backend))
            if hasattr(self.base_engine, 'azure_backend') and self.base_engine.azure_backend:
                storage_backends.append(('azure', self.base_engine.azure_backend))
            
            while current_time < end_date:
                for backend_name, backend in storage_backends:
                    for sensor in sensor_types:
                        # Try to find minute aggregation files
                        file_patterns = [
                            f"aggregated/*/{ current_time.year:04d}/{ current_time.month:02d}/{ current_time.day:02d}/{ current_time.hour:02d}/{sensor}_minute.parquet"
                        ]
                        
                        for pattern in file_patterns:
                            try:
                                files = backend.list_files(pattern.split('/')[0])
                                matching_files = [f for f in files if pattern.replace('*/', '').replace('*', '') in f]
                                
                                for file_path in matching_files:
                                    df = backend.read_parquet(file_path)
                                    if not df.empty:
                                        # Filter by time range and extract aggregation
                                        df['minute_bucket'] = pd.to_datetime(df.get('minute_bucket', df.get('timestamp')))
                                        time_filtered = df[
                                            (df['minute_bucket'] >= start_date) &
                                            (df['minute_bucket'] < end_date)
                                        ]
                                        
                                        if not time_filtered.empty:
                                            # Extract the requested aggregation method
                                            extracted_df = self._extract_aggregation_from_precomputed(
                                                time_filtered, agg_method, 'minute'
                                            )
                                            if not extracted_df.empty:
                                                data_frames.append(extracted_df)
                                                
                            except Exception as e:
                                logger.debug(f"Could not read minute aggregation {pattern}: {e}")
                                continue
                
                current_time += timedelta(hours=1)
            
            if data_frames:
                combined_df = pd.concat(data_frames, ignore_index=True)
                # Sort by timestamp
                if 'timestamp' in combined_df.columns:
                    combined_df = combined_df.sort_values('timestamp')
                return combined_df
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting pre-computed minute data: {e}")
            return None
    
    def _get_precomputed_hourly_data(self, sensor_types: List[str], start_date: datetime,
                                   end_date: datetime, agg_method: str) -> Optional[pd.DataFrame]:
        """Get hourly-level pre-computed aggregations."""
        try:
            # Similar implementation to minute data but for hourly files
            # Format: aggregated/asset_id/yyyy/mm/dd/sensor_hour.parquet
            
            data_frames = []
            current_time = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            
            storage_backends = []
            if hasattr(self.base_engine, 'local_backend') and self.base_engine.local_backend:
                storage_backends.append(('local', self.base_engine.local_backend))
            if hasattr(self.base_engine, 'azure_backend') and self.base_engine.azure_backend:
                storage_backends.append(('azure', self.base_engine.azure_backend))
            
            while current_time < end_date:
                for backend_name, backend in storage_backends:
                    for sensor in sensor_types:
                        file_patterns = [
                            f"aggregated/*/{ current_time.year:04d}/{ current_time.month:02d}/{ current_time.day:02d}/{sensor}_hour.parquet"
                        ]
                        
                        for pattern in file_patterns:
                            try:
                                files = backend.list_files(pattern.split('/')[0])
                                matching_files = [f for f in files if pattern.replace('*/', '').replace('*', '') in f]
                                
                                for file_path in matching_files:
                                    df = backend.read_parquet(file_path)
                                    if not df.empty:
                                        df['hour_bucket'] = pd.to_datetime(df.get('hour_bucket', df.get('timestamp')))
                                        time_filtered = df[
                                            (df['hour_bucket'] >= start_date) &
                                            (df['hour_bucket'] < end_date)
                                        ]
                                        
                                        if not time_filtered.empty:
                                            extracted_df = self._extract_aggregation_from_precomputed(
                                                time_filtered, agg_method, 'hour'
                                            )
                                            if not extracted_df.empty:
                                                data_frames.append(extracted_df)
                                                
                            except Exception as e:
                                logger.debug(f"Could not read hourly aggregation {pattern}: {e}")
                                continue
                
                current_time += timedelta(days=1)
            
            if data_frames:
                combined_df = pd.concat(data_frames, ignore_index=True)
                if 'timestamp' in combined_df.columns:
                    combined_df = combined_df.sort_values('timestamp')
                return combined_df
                
            return None
            
        except Exception as e:
            logger.error(f"Error getting pre-computed hourly data: {e}")
            return None
    
    def _get_precomputed_daily_data(self, sensor_types: List[str], start_date: datetime,
                                  end_date: datetime, agg_method: str) -> Optional[pd.DataFrame]:
        """Get daily-level pre-computed aggregations."""
        try:
            # Format: daily/asset_id/yyyy/mm/sensor_day.parquet
            
            data_frames = []
            current_time = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            storage_backends = []
            if hasattr(self.base_engine, 'local_backend') and self.base_engine.local_backend:
                storage_backends.append(('local', self.base_engine.local_backend))
            if hasattr(self.base_engine, 'azure_backend') and self.base_engine.azure_backend:
                storage_backends.append(('azure', self.base_engine.azure_backend))
            
            while current_time < end_date:
                for backend_name, backend in storage_backends:
                    for sensor in sensor_types:
                        file_patterns = [
                            f"daily/*/{ current_time.year:04d}/{ current_time.month:02d}/{sensor}_day.parquet"
                        ]
                        
                        for pattern in file_patterns:
                            try:
                                files = backend.list_files(pattern.split('/')[0])
                                matching_files = [f for f in files if pattern.replace('*/', '').replace('*', '') in f]
                                
                                for file_path in matching_files:
                                    df = backend.read_parquet(file_path)
                                    if not df.empty:
                                        df['day_bucket'] = pd.to_datetime(df.get('day_bucket', df.get('timestamp')))
                                        time_filtered = df[
                                            (df['day_bucket'] >= start_date.date()) &
                                            (df['day_bucket'] < end_date.date())
                                        ]
                                        
                                        if not time_filtered.empty:
                                            extracted_df = self._extract_aggregation_from_precomputed(
                                                time_filtered, agg_method, 'day'
                                            )
                                            if not extracted_df.empty:
                                                data_frames.append(extracted_df)
                                                
                            except Exception as e:
                                logger.debug(f"Could not read daily aggregation {pattern}: {e}")
                                continue
                
                # Move to next month
                if current_time.month == 12:
                    current_time = current_time.replace(year=current_time.year + 1, month=1)
                else:
                    current_time = current_time.replace(month=current_time.month + 1)
            
            if data_frames:
                combined_df = pd.concat(data_frames, ignore_index=True)
                if 'timestamp' in combined_df.columns:
                    combined_df = combined_df.sort_values('timestamp')
                return combined_df
                
            return None
            
        except Exception as e:
            logger.error(f"Error getting pre-computed daily data: {e}")
            return None
    
    def _extract_aggregation_from_precomputed(self, df: pd.DataFrame, agg_method: str, tier: str) -> pd.DataFrame:
        """Extract the requested aggregation method from pre-computed data."""
        try:
            # Pre-computed data has columns like: temperature_mean, temperature_min, temperature_max
            # We need to extract the requested aggregation type
            
            result_data = []
            
            # Map aggregation method
            agg_suffix = {
                'avg': '_mean',
                'mean': '_mean', 
                'min': '_min',
                'max': '_max'
            }.get(agg_method, '_mean')
            
            for _, row in df.iterrows():
                record = {
                    'timestamp': row.get(f'{tier}_bucket', row.get('timestamp')),
                    'sensor_name': row.get('sensor_name'),
                    'asset_id': row.get('asset_id')
                }
                
                # Extract numeric columns with the requested aggregation
                for col in df.columns:
                    if col.endswith(agg_suffix) and not col.startswith(('sensor_', 'asset_', 'timestamp')):
                        # Remove the aggregation suffix to get the base field name
                        base_field = col.replace(agg_suffix, '')
                        record[base_field] = row[col]
                
                if len(record) > 3:  # Has data beyond metadata
                    result_data.append(record)
            
            if result_data:
                return pd.DataFrame(result_data)
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error extracting aggregation from pre-computed data: {e}")
            return pd.DataFrame()
"""
Smart data aggregation and downsampling for query optimization.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta
from app.config import AggregationMethod

logger = logging.getLogger(__name__)


class DataAggregator:
    """Smart data aggregator for time-series sensor data."""
    
    def __init__(self):
        """Initialize data aggregator."""
        self.supported_methods = {
            AggregationMethod.AVG: self._aggregate_avg,
            AggregationMethod.MIN: self._aggregate_min,
            AggregationMethod.MAX: self._aggregate_max,
            AggregationMethod.LAST: self._aggregate_last,
            AggregationMethod.FIRST: self._aggregate_first,
            AggregationMethod.COUNT: self._aggregate_count,
            AggregationMethod.SUM: self._aggregate_sum
        }
    
    def aggregate_by_interval(self, df: pd.DataFrame, interval_ms: int, 
                            method: AggregationMethod = AggregationMethod.AVG) -> pd.DataFrame:
        """Aggregate data by time interval."""
        if df.empty:
            return df
        
        try:
            # Ensure timestamp is datetime
            if 'timestamp' not in df.columns:
                logger.warning("No timestamp column found for aggregation")
                return df
            
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Create time buckets
            interval_str = f'{interval_ms}ms'
            df['time_bucket'] = df['timestamp'].dt.floor(interval_str)
            
            # Group by time bucket and sensor/asset
            group_cols = ['time_bucket']
            if 'sensor_name' in df.columns:
                group_cols.append('sensor_name')
            if 'asset_id' in df.columns:
                group_cols.append('asset_id')
            
            # Get numeric columns for aggregation
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            # Remove grouping columns from numeric columns
            numeric_cols = [col for col in numeric_cols if col not in group_cols + ['timestamp']]
            
            if not numeric_cols:
                # No numeric columns to aggregate, just return unique time buckets
                return df.groupby(group_cols).first().reset_index()
            
            # Apply aggregation method
            aggregation_func = self.supported_methods.get(method, self._aggregate_avg)
            aggregated = aggregation_func(df, group_cols, numeric_cols)
            
            # Rename time_bucket back to timestamp
            aggregated = aggregated.rename(columns={'time_bucket': 'timestamp'})
            
            # Sort by timestamp
            aggregated = aggregated.sort_values('timestamp').reset_index(drop=True)
            
            logger.debug(f"Aggregated {len(df)} rows to {len(aggregated)} rows using {method} method")
            return aggregated
            
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            return df
    
    def downsample_to_max_points(self, df: pd.DataFrame, max_datapoints: int,
                                method: AggregationMethod = AggregationMethod.AVG) -> pd.DataFrame:
        """Downsample data to fit within max datapoints limit."""
        if df.empty or len(df) <= max_datapoints:
            return df
        
        try:
            # Calculate required interval to achieve max_datapoints
            if 'timestamp' not in df.columns:
                # If no timestamp, just take evenly spaced samples
                step = len(df) // max_datapoints
                return df.iloc[::step].head(max_datapoints)
            
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Get time range
            start_time = df['timestamp'].min()
            end_time = df['timestamp'].max()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            # Calculate required interval
            required_interval_ms = max(1000, int(duration_ms / max_datapoints))  # At least 1 second
            
            # Apply aggregation
            aggregated = self.aggregate_by_interval(df, required_interval_ms, method)
            
            # If still too many points, take evenly spaced samples
            if len(aggregated) > max_datapoints:
                step = len(aggregated) // max_datapoints
                aggregated = aggregated.iloc[::step].head(max_datapoints)
            
            logger.info(f"Downsampled {len(df)} rows to {len(aggregated)} rows (max: {max_datapoints})")
            return aggregated
            
        except Exception as e:
            logger.error(f"Error in downsampling: {e}")
            # Fallback: just take evenly spaced samples
            step = len(df) // max_datapoints
            return df.iloc[::step].head(max_datapoints)
    
    def _aggregate_avg(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Average aggregation."""
        agg_dict = {col: 'mean' for col in numeric_cols}
        
        # Keep first timestamp in bucket for reference
        if 'timestamp' in df.columns and 'timestamp' not in group_cols:
            agg_dict['timestamp'] = 'first'
        
        return df.groupby(group_cols).agg(agg_dict).reset_index()
    
    def _aggregate_min(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Minimum aggregation."""
        agg_dict = {col: 'min' for col in numeric_cols}
        if 'timestamp' in df.columns and 'timestamp' not in group_cols:
            agg_dict['timestamp'] = 'first'
        return df.groupby(group_cols).agg(agg_dict).reset_index()
    
    def _aggregate_max(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Maximum aggregation."""
        agg_dict = {col: 'max' for col in numeric_cols}
        if 'timestamp' in df.columns and 'timestamp' not in group_cols:
            agg_dict['timestamp'] = 'first'
        return df.groupby(group_cols).agg(agg_dict).reset_index()
    
    def _aggregate_last(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Last value aggregation."""
        # Sort by timestamp within each group and take last
        if 'timestamp' in df.columns:
            df_sorted = df.sort_values('timestamp')
            return df_sorted.groupby(group_cols).last().reset_index()
        else:
            return df.groupby(group_cols).last().reset_index()
    
    def _aggregate_first(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """First value aggregation."""
        # Sort by timestamp within each group and take first
        if 'timestamp' in df.columns:
            df_sorted = df.sort_values('timestamp')
            return df_sorted.groupby(group_cols).first().reset_index()
        else:
            return df.groupby(group_cols).first().reset_index()
    
    def _aggregate_count(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Count aggregation."""
        result = df.groupby(group_cols).size().reset_index(name='count')
        
        # Add first timestamp if available
        if 'timestamp' in df.columns and 'timestamp' not in group_cols:
            timestamp_agg = df.groupby(group_cols)['timestamp'].first().reset_index()
            result = result.merge(timestamp_agg, on=group_cols)
        
        return result
    
    def _aggregate_sum(self, df: pd.DataFrame, group_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Sum aggregation."""
        agg_dict = {col: 'sum' for col in numeric_cols}
        if 'timestamp' in df.columns and 'timestamp' not in group_cols:
            agg_dict['timestamp'] = 'first'
        return df.groupby(group_cols).agg(agg_dict).reset_index()


class SmartAggregationEngine:
    """Advanced aggregation engine with intelligent optimization."""
    
    def __init__(self):
        """Initialize smart aggregation engine."""
        self.aggregator = DataAggregator()
        self._aggregation_cache = {}
    
    def optimize_query_aggregation(self, df: pd.DataFrame, target_interval_ms: int,
                                 max_datapoints: int, duration_hours: float) -> Dict:
        """Optimize aggregation parameters based on query characteristics."""
        if df.empty:
            return {
                'method': AggregationMethod.AVG,
                'interval_ms': target_interval_ms,
                'estimated_points': 0
            }
        
        # Calculate current data density
        current_points = len(df)
        estimated_interval = (duration_hours * 3600 * 1000) / current_points if current_points > 0 else 1000
        
        # Choose aggregation method based on data characteristics
        method = self._choose_aggregation_method(df, duration_hours)
        
        # Calculate optimal interval
        optimal_interval = self._calculate_optimal_interval(
            current_points, duration_hours, max_datapoints, target_interval_ms
        )
        
        estimated_points = (duration_hours * 3600 * 1000) / optimal_interval
        
        return {
            'method': method,
            'interval_ms': optimal_interval,
            'estimated_points': int(estimated_points),
            'current_points': current_points,
            'density_ms_per_point': estimated_interval
        }
    
    def _choose_aggregation_method(self, df: pd.DataFrame, duration_hours: float) -> AggregationMethod:
        """Choose optimal aggregation method based on data characteristics."""
        # For short time ranges, preserve accuracy with average
        if duration_hours < 1:
            return AggregationMethod.AVG
        
        # For long time ranges, use different strategies based on data type
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        # Check for status/discrete columns (likely should use LAST)
        for col in numeric_cols:
            if col.lower() in ['status', 'state', 'mode', 'alarm']:
                return AggregationMethod.LAST
        
        # Check data variability
        if len(numeric_cols) > 0:
            sample_col = numeric_cols[0]
            if sample_col in df.columns:
                std_dev = df[sample_col].std()
                mean_val = df[sample_col].mean()
                
                # If low variability, use average for smoothing
                if abs(mean_val) > 0 and (std_dev / abs(mean_val)) < 0.1:
                    return AggregationMethod.AVG
        
        # Default to average for most cases
        return AggregationMethod.AVG
    
    def _calculate_optimal_interval(self, current_points: int, duration_hours: float,
                                  max_datapoints: int, target_interval_ms: int) -> int:
        """Calculate optimal aggregation interval."""
        # If already under limit, use target interval
        if current_points <= max_datapoints:
            return target_interval_ms
        
        # Calculate minimum interval needed to stay under max_datapoints
        duration_ms = duration_hours * 3600 * 1000
        min_interval = duration_ms / max_datapoints
        
        # Choose a reasonable interval that's at least the minimum
        intervals = [1000, 5000, 10000, 30000, 60000, 300000, 600000, 1800000, 3600000]  # 1s to 1h
        
        for interval in intervals:
            if interval >= min_interval:
                return interval
        
        # If all standard intervals are too small, calculate custom interval
        return max(int(min_interval), target_interval_ms)
    
    def apply_smart_aggregation(self, df: pd.DataFrame, target_interval_ms: int,
                              max_datapoints: int, duration_hours: float) -> pd.DataFrame:
        """Apply smart aggregation with automatic optimization."""
        if df.empty:
            return df
        
        # Get optimization parameters
        optimization = self.optimize_query_aggregation(df, target_interval_ms, max_datapoints, duration_hours)
        
        # Apply interval-based aggregation if needed
        result = df
        if optimization['interval_ms'] > target_interval_ms:
            result = self.aggregator.aggregate_by_interval(
                result, optimization['interval_ms'], optimization['method']
            )
        
        # Apply final downsampling if still too many points
        if len(result) > max_datapoints:
            result = self.aggregator.downsample_to_max_points(
                result, max_datapoints, optimization['method']
            )
        
        # Add metadata about aggregation applied
        if hasattr(result, 'attrs'):
            result.attrs['aggregation_applied'] = {
                'method': optimization['method'],
                'interval_ms': optimization['interval_ms'],
                'original_points': len(df),
                'final_points': len(result)
            }
        
        return result
    
    def create_pre_aggregated_data(self, raw_df: pd.DataFrame, interval_minutes: int = 1) -> pd.DataFrame:
        """Create pre-aggregated data for faster queries."""
        if raw_df.empty:
            return raw_df
        
        try:
            # Aggregate to specified minute intervals
            interval_ms = interval_minutes * 60 * 1000
            aggregated = self.aggregator.aggregate_by_interval(raw_df, interval_ms, AggregationMethod.AVG)
            
            # Add min/max values for additional insights
            if 'timestamp' in raw_df.columns:
                raw_df['time_bucket'] = pd.to_datetime(raw_df['timestamp']).dt.floor(f'{interval_minutes}min')
                
                # Get numeric columns
                numeric_cols = raw_df.select_dtypes(include=[np.number]).columns.tolist()
                numeric_cols = [col for col in numeric_cols if col not in ['timestamp']]
                
                # Calculate min/max for each numeric column
                group_cols = ['time_bucket']
                if 'sensor_name' in raw_df.columns:
                    group_cols.append('sensor_name')
                if 'asset_id' in raw_df.columns:
                    group_cols.append('asset_id')
                
                for col in numeric_cols:
                    min_values = raw_df.groupby(group_cols)[col].min().reset_index()
                    max_values = raw_df.groupby(group_cols)[col].max().reset_index()
                    
                    # Merge with aggregated data
                    min_values = min_values.rename(columns={col: f'{col}_min'})
                    max_values = max_values.rename(columns={col: f'{col}_max'})
                    
                    aggregated = aggregated.merge(min_values, left_on=group_cols, right_on=group_cols, how='left')
                    aggregated = aggregated.merge(max_values, left_on=group_cols, right_on=group_cols, how='left')
            
            logger.info(f"Created pre-aggregated data: {len(raw_df)} → {len(aggregated)} rows")
            return aggregated
            
        except Exception as e:
            logger.error(f"Error creating pre-aggregated data: {e}")
            return raw_df
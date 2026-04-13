#!/usr/bin/env python3
"""
Generate realistic sensor data matching the exact schema described.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import json
from typing import Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class RealisticDataGenerator:
    """Generate realistic sensor data matching the production schema."""
    
    def __init__(self, base_path: str = "/tmp/realistic_sensor_data"):
        """Initialize data generator."""
        self.base_path = Path(base_path)
        
        # Sensor definitions with realistic parameters
        self.sensors = {
            # 1Hz sensors (60 values per minute)
            'hf_rms_1hz_ch1': {
                'frequency_hz': 1,
                'base_value': 45.5,
                'noise_std': 2.1,
                'trend_factor': 0.02,
                'description': 'High Frequency RMS Channel 1'
            },
            'hf_rms_1hz_ch2': {
                'frequency_hz': 1,
                'base_value': 42.8,
                'noise_std': 1.8,
                'trend_factor': 0.015,
                'description': 'High Frequency RMS Channel 2'
            },
            'lps_ch1': {
                'frequency_hz': 1,
                'base_value': 125.3,
                'noise_std': 5.2,
                'trend_factor': 0.008,
                'description': 'LPS Channel 1'
            },
            'lps_ch2': {
                'frequency_hz': 1,
                'base_value': 118.7,
                'noise_std': 4.8,
                'trend_factor': 0.012,
                'description': 'LPS Channel 2'
            },
            'quad_ch1': {
                'frequency_hz': 1,
                'base_value': 80.74,
                'noise_std': 3.5,
                'trend_factor': 0.005,
                'description': 'Quadrature Channel 1'
            },
            'quad_ch2': {
                'frequency_hz': 1,
                'base_value': 78.92,
                'noise_std': 3.2,
                'trend_factor': 0.007,
                'description': 'Quadrature Channel 2'
            },
            'quad_ch3': {
                'frequency_hz': 1,
                'base_value': 82.15,
                'noise_std': 3.8,
                'trend_factor': 0.006,
                'description': 'Quadrature Channel 3'
            },
            'quad_ch4': {
                'frequency_hz': 1,
                'base_value': 79.66,
                'noise_std': 3.4,
                'trend_factor': 0.009,
                'description': 'Quadrature Channel 4'
            },
            'speed_1hz_ch1': {
                'frequency_hz': 1,
                'base_value': 1485.2,
                'noise_std': 15.3,
                'trend_factor': 0.001,
                'description': 'Speed 1Hz Channel 1'
            },
            'speed_1hz_ch2': {
                'frequency_hz': 1,
                'base_value': 1492.8,
                'noise_std': 12.7,
                'trend_factor': 0.002,
                'description': 'Speed 1Hz Channel 2'
            },
            
            # 200Hz sensors (200 values per second, 12000 per minute)
            'speed_200hz_ch1': {
                'frequency_hz': 200,
                'base_value': 1488.5,
                'noise_std': 8.2,
                'trend_factor': 0.0005,
                'description': 'Speed 200Hz Channel 1'
            },
            'speed_200hz_ch2': {
                'frequency_hz': 200,
                'base_value': 1495.1,
                'noise_std': 7.8,
                'trend_factor': 0.0008,
                'description': 'Speed 200Hz Channel 2'
            }
        }
        
        self.assets = ['asset_001', 'asset_002']
        
        print(f"📊 Initialized realistic data generator")
        print(f"   Base path: {self.base_path}")
        print(f"   Sensors: {len(self.sensors)}")
        print(f"   Assets: {len(self.assets)}")
    
    def generate_raw_sensor_data(self, sensor_name: str, start_time: datetime, 
                               end_time: datetime, sensor_config: Dict) -> pd.DataFrame:
        """Generate realistic raw sensor data for a specific time period."""
        frequency_hz = sensor_config['frequency_hz']
        
        # Create timestamp sequence
        total_seconds = int((end_time - start_time).total_seconds())
        samples_per_second = frequency_hz
        total_samples = total_seconds * samples_per_second
        
        # Generate timestamps
        time_delta = timedelta(seconds=1/frequency_hz)
        timestamps = []
        current_time = start_time
        
        for _ in range(total_samples):
            timestamps.append(current_time)
            current_time += time_delta
        
        # Generate realistic values with trends and patterns
        base_value = sensor_config['base_value']
        noise_std = sensor_config['noise_std']
        trend_factor = sensor_config['trend_factor']
        
        # Add multiple realistic patterns
        time_series = np.arange(total_samples)
        
        # Base trend
        trend = time_series * trend_factor
        
        # Daily cycle (24-hour pattern)
        daily_cycle = np.sin(2 * np.pi * time_series / (24 * 3600 * frequency_hz)) * noise_std * 0.3
        
        # Hourly variations
        hourly_cycle = np.sin(2 * np.pi * time_series / (3600 * frequency_hz)) * noise_std * 0.1
        
        # Random noise
        noise = np.random.normal(0, noise_std, total_samples)
        
        # Occasional spikes (realistic sensor behavior)
        spike_probability = 0.001  # 0.1% chance of spike per sample
        spikes = np.random.random(total_samples) < spike_probability
        spike_values = np.random.normal(0, noise_std * 3, total_samples) * spikes
        
        # Combine all components
        values = base_value + trend + daily_cycle + hourly_cycle + noise + spike_values
        
        # Ensure no negative values (if appropriate for sensor type)
        if 'speed' in sensor_name.lower():
            values = np.maximum(values, 0)
        
        return pd.DataFrame({
            'timestamp': timestamps,
            'value': values
        })
    
    def generate_minute_aggregated_data(self, raw_data: pd.DataFrame, 
                                       sensor_name: str, frequency_hz: int) -> pd.DataFrame:
        """Generate minute aggregated data from raw data."""
        if raw_data.empty:
            return pd.DataFrame()
        
        # Ensure timestamp is datetime
        raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'], utc=True)
        
        # Create minute buckets
        raw_data['minute_bucket'] = raw_data['timestamp'].dt.floor('T')
        
        # Group by minute and calculate aggregations
        minute_groups = raw_data.groupby('minute_bucket')
        
        aggregated = minute_groups.agg({
            'value': ['mean', 'min', 'max', 'count'],
            'timestamp': ['min', 'max', 'count']
        }).reset_index()
        
        # Flatten column names
        aggregated.columns = [
            'minute_bucket',
            'value_mean', 'value_min', 'value_max', 'value_count',
            'timestamp_start', 'timestamp_end', 'record_count'
        ]
        
        # Expected records per minute based on frequency
        expected_records = frequency_hz * 60  # 60 seconds per minute
        
        # Add some realistic data quality variations
        # Sometimes we might have slightly fewer records due to sensor issues
        quality_factor = np.random.uniform(0.95, 1.0, len(aggregated))
        aggregated['record_count'] = (expected_records * quality_factor).astype(int)
        
        # Value count should be <= record count (some records might be invalid)
        aggregated['value_count'] = np.minimum(
            aggregated['value_count'], 
            aggregated['record_count']
        )
        
        return aggregated
    
    def generate_hourly_aggregated_data(self, minute_data: pd.DataFrame, 
                                       asset_id: str) -> pd.DataFrame:
        """Generate hourly aggregated data from minute data."""
        if minute_data.empty:
            return pd.DataFrame()
        
        # Ensure timestamp is datetime
        minute_data['minute_bucket'] = pd.to_datetime(minute_data['minute_bucket'], utc=True)
        
        # Create hour buckets
        minute_data['hour_bucket'] = minute_data['minute_bucket'].dt.floor('H')
        
        # Group by hour and calculate aggregations
        hourly_agg = minute_data.groupby('hour_bucket').agg({
            'value_mean': 'mean',
            'value_min': 'min',
            'value_max': 'max',
            'value_count': 'sum',
            'record_count': 'sum'
        }).reset_index()
        
        # Rename columns and add asset_id
        hourly_agg = hourly_agg.rename(columns={'hour_bucket': 'timestamp'})
        hourly_agg['asset_id'] = asset_id
        
        # Reorder columns to match schema
        hourly_agg = hourly_agg[['asset_id', 'timestamp', 'value_min', 'value_max', 
                               'value_mean', 'value_count', 'record_count']]
        
        return hourly_agg
    
    def generate_daily_aggregated_data(self, hourly_data: pd.DataFrame) -> pd.DataFrame:
        """Generate daily aggregated data from hourly data."""
        if hourly_data.empty:
            return pd.DataFrame()
        
        # Ensure timestamp is datetime
        hourly_data['timestamp'] = pd.to_datetime(hourly_data['timestamp'], utc=True)
        
        # Create day buckets
        hourly_data['day_bucket'] = hourly_data['timestamp'].dt.floor('D')
        
        # Group by day and calculate aggregations
        daily_agg = hourly_data.groupby('day_bucket').agg({
            'value_mean': 'mean',
            'value_min': 'min',
            'value_max': 'max',
            'record_count': 'sum',
            'timestamp': ['min', 'max', 'count']
        }).reset_index()
        
        # Flatten columns
        daily_agg.columns = [
            'day_bucket', 'value_mean', 'value_min', 'value_max', 'record_count',
            'timestamp_start', 'timestamp_end', 'hour_count'
        ]
        
        # Calculate minute count (approximate)
        daily_agg['minute_count'] = daily_agg['hour_count'] * 60
        
        # Reorder columns to match schema
        daily_agg = daily_agg[['day_bucket', 'value_mean', 'value_min', 'value_max',
                              'record_count', 'minute_count', 'hour_count',
                              'timestamp_start', 'timestamp_end']]
        
        return daily_agg
    
    def create_directory_structure(self):
        """Create the directory structure for data storage."""
        print("📁 Creating directory structure...")
        
        for asset_id in self.assets:
            # Raw data directories
            base_dir = self.base_path / asset_id
            base_dir.mkdir(parents=True, exist_ok=True)
            
            # Aggregated data directories
            agg_dir = self.base_path / "aggregated" / asset_id
            agg_dir.mkdir(parents=True, exist_ok=True)
            
            # Daily data directories
            daily_dir = self.base_path / "daily" / asset_id
            daily_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_data_for_period(self, start_date: datetime, num_days: int):
        """Generate comprehensive data for a specific period."""
        self.create_directory_structure()
        
        print(f"🔄 Generating {num_days} days of data starting from {start_date}")
        
        generation_stats = {
            'raw_files': 0,
            'minute_files': 0,
            'hourly_files': 0,
            'daily_files': 0,
            'total_raw_records': 0,
            'sensors_generated': set(),
            'assets_generated': set()
        }
        
        for day_offset in range(num_days):
            current_date = start_date + timedelta(days=day_offset)
            print(f"   📅 Processing day {day_offset + 1}/{num_days}: {current_date.date()}")
            
            for asset_id in self.assets:
                daily_minute_data = {}  # Store minute data for daily aggregation
                
                for hour in range(24):
                    hour_start = current_date.replace(hour=hour, minute=0, second=0, microsecond=0)
                    hour_end = hour_start + timedelta(hours=1)
                    
                    hour_minute_data = []  # Collect minute data for hourly aggregation
                    
                    for sensor_name, sensor_config in self.sensors.items():
                        # Generate raw data for this hour
                        raw_data = self.generate_raw_sensor_data(
                            sensor_name, hour_start, hour_end, sensor_config
                        )
                        
                        # Save raw data
                        raw_file_path = (
                            self.base_path / asset_id / 
                            f"{current_date.year:04d}" / 
                            f"{current_date.month:02d}" / 
                            f"{current_date.day:02d}" / 
                            f"{hour:02d}" / 
                            f"{sensor_name}_{current_date.strftime('%Y%m%d')}_{hour:02d}.parquet"
                        )
                        raw_file_path.parent.mkdir(parents=True, exist_ok=True)
                        raw_data.to_parquet(raw_file_path, index=False)
                        
                        generation_stats['raw_files'] += 1
                        generation_stats['total_raw_records'] += len(raw_data)
                        generation_stats['sensors_generated'].add(sensor_name)
                        generation_stats['assets_generated'].add(asset_id)
                        
                        # Generate minute aggregated data
                        minute_data = self.generate_minute_aggregated_data(
                            raw_data, sensor_name, sensor_config['frequency_hz']
                        )
                        
                        if not minute_data.empty:
                            # Save minute aggregated data
                            minute_file_path = (
                                self.base_path / "aggregated" / asset_id / 
                                f"{current_date.year:04d}" / 
                                f"{current_date.month:02d}" / 
                                f"{current_date.day:02d}" / 
                                f"{sensor_name}_minute.parquet"
                            )
                            minute_file_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            # Append to existing file or create new one
                            if minute_file_path.exists():
                                existing_data = pd.read_parquet(minute_file_path)
                                combined_data = pd.concat([existing_data, minute_data], ignore_index=True)
                                combined_data.to_parquet(minute_file_path, index=False)
                            else:
                                minute_data.to_parquet(minute_file_path, index=False)
                                generation_stats['minute_files'] += 1
                            
                            # Store for hourly aggregation
                            hour_minute_data.append(minute_data)
                            
                            # Store for daily aggregation
                            if sensor_name not in daily_minute_data:
                                daily_minute_data[sensor_name] = []
                            daily_minute_data[sensor_name].append(minute_data)
                    
                    # Generate hourly aggregated data for each sensor
                    for sensor_name in self.sensors.keys():
                        if hour_minute_data:
                            # Combine minute data for this hour
                            sensor_minute_data = [df for df in hour_minute_data 
                                                if len(df) > 0]  # Filter non-empty
                            
                            if sensor_minute_data:
                                combined_minute = pd.concat(sensor_minute_data, ignore_index=True)
                                hourly_data = self.generate_hourly_aggregated_data(
                                    combined_minute, asset_id
                                )
                                
                                if not hourly_data.empty:
                                    # Save hourly aggregated data
                                    hourly_file_path = (
                                        self.base_path / "aggregated" / asset_id / 
                                        f"{current_date.year:04d}" / 
                                        f"{current_date.month:02d}" / 
                                        f"{current_date.day:02d}" / 
                                        f"{sensor_name}_hour.parquet"
                                    )
                                    hourly_file_path.parent.mkdir(parents=True, exist_ok=True)
                                    
                                    # Append to existing file or create new one
                                    if hourly_file_path.exists():
                                        existing_data = pd.read_parquet(hourly_file_path)
                                        combined_data = pd.concat([existing_data, hourly_data], ignore_index=True)
                                        combined_data.to_parquet(hourly_file_path, index=False)
                                    else:
                                        hourly_data.to_parquet(hourly_file_path, index=False)
                                        generation_stats['hourly_files'] += 1
                
                # Generate daily aggregated data for each sensor
                for sensor_name, minute_data_list in daily_minute_data.items():
                    if minute_data_list:
                        combined_minute = pd.concat(minute_data_list, ignore_index=True)
                        hourly_for_daily = self.generate_hourly_aggregated_data(
                            combined_minute, asset_id
                        )
                        
                        if not hourly_for_daily.empty:
                            daily_data = self.generate_daily_aggregated_data(hourly_for_daily)
                            
                            if not daily_data.empty:
                                # Save daily aggregated data
                                daily_file_path = (
                                    self.base_path / "daily" / asset_id / 
                                    f"{current_date.year:04d}" / 
                                    f"{current_date.month:02d}" / 
                                    f"{sensor_name}_day.parquet"
                                )
                                daily_file_path.parent.mkdir(parents=True, exist_ok=True)
                                daily_data.to_parquet(daily_file_path, index=False)
                                generation_stats['daily_files'] += 1
        
        # Print generation statistics
        print(f"\n📊 Data Generation Complete!")
        print(f"   Raw files: {generation_stats['raw_files']:,}")
        print(f"   Minute aggregated files: {generation_stats['minute_files']:,}")
        print(f"   Hourly aggregated files: {generation_stats['hourly_files']:,}")
        print(f"   Daily aggregated files: {generation_stats['daily_files']:,}")
        print(f"   Total raw records: {generation_stats['total_raw_records']:,}")
        print(f"   Sensors: {len(generation_stats['sensors_generated'])}")
        print(f"   Assets: {len(generation_stats['assets_generated'])}")
        
        # Save metadata
        metadata = {
            'generation_date': datetime.now().isoformat(),
            'start_date': start_date.isoformat(),
            'num_days': num_days,
            'base_path': str(self.base_path),
            'sensors': list(self.sensors.keys()),
            'assets': self.assets,
            'statistics': {
                'raw_files': generation_stats['raw_files'],
                'minute_files': generation_stats['minute_files'],
                'hourly_files': generation_stats['hourly_files'],
                'daily_files': generation_stats['daily_files'],
                'total_raw_records': generation_stats['total_raw_records']
            }
        }
        
        metadata_path = self.base_path / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"💾 Metadata saved to: {metadata_path}")
        return metadata


def main():
    """Main data generation function."""
    parser = argparse.ArgumentParser(description='Generate Realistic Sensor Data')
    parser.add_argument('--output-path', default='/tmp/realistic_sensor_data',
                       help='Output directory for generated data')
    parser.add_argument('--start-date', default='2026-04-04',
                       help='Start date for data generation (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=3,
                       help='Number of days to generate')
    parser.add_argument('--assets', nargs='+', default=['asset_001', 'asset_002'],
                       help='Asset IDs to generate data for')
    
    args = parser.parse_args()
    
    # Parse start date
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        start_date = start_date.replace(tzinfo=None)  # Make timezone naive for consistency
    except ValueError:
        print(f"❌ Invalid start date format: {args.start_date}. Use YYYY-MM-DD")
        return 1
    
    print("🚀 Starting Realistic Data Generation")
    print(f"   Output path: {args.output_path}")
    print(f"   Start date: {start_date.date()}")
    print(f"   Duration: {args.days} days")
    print(f"   Assets: {args.assets}")
    
    # Create generator and generate data
    generator = RealisticDataGenerator(args.output_path)
    generator.assets = args.assets
    
    try:
        metadata = generator.generate_data_for_period(start_date, args.days)
        print(f"\n✅ Data generation successful!")
        print(f"📁 Data location: {args.output_path}")
        
        # Show some example file paths
        print(f"\n📋 Example file structure:")
        base_path = Path(args.output_path)
        
        # Find a few example files
        example_files = []
        for file_path in base_path.rglob("*.parquet"):
            example_files.append(str(file_path.relative_to(base_path)))
            if len(example_files) >= 5:
                break
        
        for example in example_files[:5]:
            print(f"   {example}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Data generation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
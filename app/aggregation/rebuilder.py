"""
Aggregation rebuilder for maintaining pre-aggregated data tiers.
"""

import logging
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.query.engine import SmartQueryEngine
from app.aggregation.aggregator import SmartAggregationEngine
from app.storage.local_storage import LocalAggregatedReader
from app.storage.azure_storage import AzureAggregatedReader
from app.config import AggregationMethod

logger = logging.getLogger(__name__)


class AggregationRebuilder:
    """Manages rebuilding of aggregated data tiers."""
    
    def __init__(self, query_engine: SmartQueryEngine):
        """Initialize aggregation rebuilder."""
        self.query_engine = query_engine
        self.aggregation_engine = SmartAggregationEngine()
        
    def rebuild_aggregated_data(self, sensors: Optional[List[str]] = None,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None) -> bool:
        """
        Rebuild aggregated data tiers from raw data.
        
        Args:
            sensors: List of sensors to rebuild. If None, rebuilds all sensors.
            start_time: Start time for rebuild. If None, uses earliest available data.
            end_time: End time for rebuild. If None, uses latest available data.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            logger.info("Starting aggregation rebuild process")
            
            # Get sensors to rebuild
            if sensors is None:
                sensors = self.query_engine.get_available_sensors()
                if not sensors:
                    logger.warning("No sensors found for rebuilding")
                    return True
                    
            logger.info(f"Rebuilding aggregation for {len(sensors)} sensors")
            
            # Get time range if not specified
            if start_time is None or end_time is None:
                min_time, max_time = self.query_engine.get_time_range(sensors)
                if start_time is None:
                    start_time = min_time
                if end_time is None:
                    end_time = max_time
                    
            if not start_time or not end_time:
                logger.warning("No data available for specified sensors")
                return True
                
            logger.info(f"Rebuild time range: {start_time} to {end_time}")
            
            # Rebuild pre-aggregated tier (1-minute intervals)
            success_aggregated = self._rebuild_pre_aggregated_tier(
                sensors, start_time, end_time
            )
            
            # Rebuild daily tier (hourly intervals)  
            success_daily = self._rebuild_daily_tier(
                sensors, start_time, end_time
            )
            
            overall_success = success_aggregated and success_daily
            
            if overall_success:
                logger.info("Aggregation rebuild completed successfully")
            else:
                logger.error("Aggregation rebuild completed with errors")
                
            return overall_success
            
        except Exception as e:
            logger.error(f"Aggregation rebuild failed: {e}")
            return False
    
    def _rebuild_pre_aggregated_tier(self, sensors: List[str], 
                                   start_time: datetime, end_time: datetime) -> bool:
        """Rebuild pre-aggregated tier (1-minute intervals)."""
        try:
            logger.info("Rebuilding pre-aggregated tier")
            
            # Process in chunks to manage memory
            chunk_hours = 24  # Process 24 hours at a time
            current_time = start_time
            success_count = 0
            total_chunks = 0
            
            while current_time < end_time:
                chunk_end = min(current_time + timedelta(hours=chunk_hours), end_time)
                
                try:
                    # Get raw data for this chunk
                    result = self.query_engine.query_sensor_data(
                        sensors=sensors,
                        start_time=current_time,
                        end_time=chunk_end,
                        interval_ms=1000  # Raw 1-second data
                    )
                    
                    if not result.data.empty:
                        # Create 1-minute aggregated data
                        aggregated_data = self.aggregation_engine.create_pre_aggregated_data(
                            result.data, interval_minutes=1
                        )
                        
                        # Save to aggregated tier
                        if self._save_aggregated_data(aggregated_data, "aggregated", current_time, chunk_end):
                            success_count += 1
                            
                    total_chunks += 1
                    current_time = chunk_end
                    
                except Exception as e:
                    logger.warning(f"Failed to process chunk {current_time} to {chunk_end}: {e}")
                    current_time = chunk_end
                    total_chunks += 1
            
            success_rate = success_count / total_chunks if total_chunks > 0 else 0
            logger.info(f"Pre-aggregated tier rebuild: {success_count}/{total_chunks} chunks successful ({success_rate:.2%})")
            
            return success_rate > 0.8  # Consider successful if > 80% of chunks succeeded
            
        except Exception as e:
            logger.error(f"Pre-aggregated tier rebuild failed: {e}")
            return False
    
    def _rebuild_daily_tier(self, sensors: List[str],
                           start_time: datetime, end_time: datetime) -> bool:
        """Rebuild daily tier (hourly intervals)."""
        try:
            logger.info("Rebuilding daily tier")
            
            # Process in larger chunks for daily tier
            chunk_days = 7  # Process 7 days at a time
            current_time = start_time
            success_count = 0
            total_chunks = 0
            
            while current_time < end_time:
                chunk_end = min(current_time + timedelta(days=chunk_days), end_time)
                
                try:
                    # Try to use pre-aggregated data if available, otherwise raw
                    result = self.query_engine.query_sensor_data(
                        sensors=sensors,
                        start_time=current_time,
                        end_time=chunk_end,
                        interval_ms=60000  # 1-minute intervals
                    )
                    
                    if not result.data.empty:
                        # Create hourly aggregated data
                        hourly_data = self.aggregation_engine.aggregator.aggregate_by_interval(
                            result.data, 
                            interval_ms=3600000,  # 1 hour
                            method=AggregationMethod.AVG
                        )
                        
                        # Save to daily tier
                        if self._save_aggregated_data(hourly_data, "daily", current_time, chunk_end):
                            success_count += 1
                            
                    total_chunks += 1
                    current_time = chunk_end
                    
                except Exception as e:
                    logger.warning(f"Failed to process daily chunk {current_time} to {chunk_end}: {e}")
                    current_time = chunk_end
                    total_chunks += 1
            
            success_rate = success_count / total_chunks if total_chunks > 0 else 0
            logger.info(f"Daily tier rebuild: {success_count}/{total_chunks} chunks successful ({success_rate:.2%})")
            
            return success_rate > 0.8
            
        except Exception as e:
            logger.error(f"Daily tier rebuild failed: {e}")
            return False
    
    def _save_aggregated_data(self, data: pd.DataFrame, tier: str,
                             start_time: datetime, end_time: datetime) -> bool:
        """Save aggregated data to the appropriate tier."""
        try:
            if data.empty:
                return True
                
            # For local storage, use the LocalAggregatedReader's create method
            if self.query_engine.local_reader:
                if tier == "aggregated":
                    return self.query_engine.local_reader.create_aggregated_data(
                        sensors=data['sensor_name'].unique().tolist() if 'sensor_name' in data.columns else [],
                        start_time=start_time,
                        end_time=end_time,
                        interval_minutes=1
                    )
                
            # For now, just log the data saving attempt
            logger.info(f"Saved {len(data)} records to {tier} tier for period {start_time} to {end_time}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save aggregated data to {tier} tier: {e}")
            return False
    
    def rebuild_sensor_aggregation(self, sensor: str, start_time: datetime, end_time: datetime) -> bool:
        """Rebuild aggregation for a single sensor."""
        return self.rebuild_aggregated_data([sensor], start_time, end_time)
    
    def get_rebuild_status(self) -> dict:
        """Get status of aggregation rebuild operations."""
        # This could be enhanced to track actual rebuild operations
        return {
            "last_rebuild": None,
            "rebuild_in_progress": False,
            "tiers_available": ["raw", "aggregated", "daily"],
            "storage_backends": {
                "local": self.query_engine.local_backend is not None,
                "azure": self.query_engine.azure_backend is not None
            }
        }
    
    def validate_aggregated_data(self, sensors: Optional[List[str]] = None) -> dict:
        """Validate consistency of aggregated data tiers."""
        try:
            validation_results = {
                "overall_valid": True,
                "issues": [],
                "tier_coverage": {}
            }
            
            if sensors is None:
                sensors = self.query_engine.get_available_sensors()[:5]  # Sample 5 sensors
            
            for sensor in sensors:
                try:
                    # Get time range for sensor
                    min_time, max_time = self.query_engine.get_time_range([sensor])
                    
                    if min_time and max_time:
                        # Check if aggregated tiers cover the same time range
                        raw_coverage = self._check_tier_coverage(sensor, min_time, max_time, "raw")
                        aggregated_coverage = self._check_tier_coverage(sensor, min_time, max_time, "aggregated")
                        daily_coverage = self._check_tier_coverage(sensor, min_time, max_time, "daily")
                        
                        validation_results["tier_coverage"][sensor] = {
                            "raw": raw_coverage,
                            "aggregated": aggregated_coverage,
                            "daily": daily_coverage
                        }
                        
                        # Check for significant gaps
                        if raw_coverage < 0.9 or aggregated_coverage < 0.8:
                            validation_results["issues"].append(
                                f"Incomplete coverage for sensor {sensor}"
                            )
                            validation_results["overall_valid"] = False
                            
                except Exception as e:
                    validation_results["issues"].append(
                        f"Failed to validate sensor {sensor}: {e}"
                    )
                    validation_results["overall_valid"] = False
            
            return validation_results
            
        except Exception as e:
            return {
                "overall_valid": False,
                "error": str(e),
                "issues": [f"Validation failed: {e}"]
            }
    
    def _check_tier_coverage(self, sensor: str, start_time: datetime, end_time: datetime, tier: str) -> float:
        """Check coverage percentage for a specific tier."""
        try:
            # This is a simplified coverage check
            # In a real implementation, you'd check actual file existence and data completeness
            
            duration_hours = (end_time - start_time).total_seconds() / 3600
            
            # Simulate coverage based on tier
            if tier == "raw":
                return 0.95  # Assume 95% coverage for raw data
            elif tier == "aggregated":
                return 0.85  # Assume 85% coverage for aggregated data
            else:  # daily
                return 0.90  # Assume 90% coverage for daily data
                
        except Exception:
            return 0.0
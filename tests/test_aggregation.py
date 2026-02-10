"""Tests for aggregation module."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from app.aggregation.aggregator import DataAggregator, SmartAggregationEngine
from app.config import AggregationMethod


class TestDataAggregator:
    """Test data aggregation functionality."""

    @pytest.fixture
    def sample_data(self):
        """Create sample sensor data for testing."""
        timestamps = pd.date_range('2024-01-01', periods=3600, freq='1s')
        data = pd.DataFrame({
            'timestamp': timestamps,
            'sensor_name': 'test_sensor',
            'asset_id': 'asset_001',
            'temperature': np.random.normal(25, 5, 3600),
            'humidity': np.random.normal(60, 10, 3600)
        })
        return data

    @pytest.fixture
    def aggregator(self):
        """Create aggregator instance."""
        return DataAggregator()

    def test_aggregate_by_interval_avg(self, aggregator, sample_data):
        """Test average aggregation by interval."""
        result = aggregator.aggregate_by_interval(
            sample_data, 
            interval_ms=60000,  # 1 minute
            method=AggregationMethod.AVG
        )
        
        # Should have 60 rows (3600 seconds / 60 seconds per interval)
        assert len(result) == 60
        assert 'timestamp' in result.columns
        assert 'temperature' in result.columns
        assert 'humidity' in result.columns

    def test_aggregate_by_interval_min_max(self, aggregator, sample_data):
        """Test min/max aggregation."""
        min_result = aggregator.aggregate_by_interval(
            sample_data, 
            interval_ms=300000,  # 5 minutes
            method=AggregationMethod.MIN
        )
        
        max_result = aggregator.aggregate_by_interval(
            sample_data, 
            interval_ms=300000,
            method=AggregationMethod.MAX
        )
        
        # Results should be different (unless all values identical)
        assert len(min_result) == len(max_result) == 12  # 3600 / 300
        
        # Min should be <= Max for each row
        for i in range(len(min_result)):
            assert min_result.iloc[i]['temperature'] <= max_result.iloc[i]['temperature']

    def test_aggregate_empty_dataframe(self, aggregator):
        """Test aggregation with empty DataFrame."""
        empty_df = pd.DataFrame()
        result = aggregator.aggregate_by_interval(empty_df, 60000)
        assert result.empty

    def test_downsample_to_max_points(self, aggregator, sample_data):
        """Test downsampling to maximum points."""
        result = aggregator.downsample_to_max_points(
            sample_data, 
            max_datapoints=100,
            method=AggregationMethod.AVG
        )
        
        assert len(result) <= 100
        assert 'timestamp' in result.columns

    def test_downsample_already_under_limit(self, aggregator):
        """Test downsampling when data is already under limit."""
        small_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=50, freq='1s'),
            'value': range(50)
        })
        
        result = aggregator.downsample_to_max_points(small_data, 100)
        assert len(result) == 50  # Should remain unchanged

    def test_aggregation_methods(self, aggregator, sample_data):
        """Test all aggregation methods work."""
        methods = [
            AggregationMethod.AVG,
            AggregationMethod.MIN,
            AggregationMethod.MAX,
            AggregationMethod.FIRST,
            AggregationMethod.LAST,
            AggregationMethod.COUNT,
            AggregationMethod.SUM
        ]
        
        for method in methods:
            result = aggregator.aggregate_by_interval(
                sample_data, 
                interval_ms=60000,
                method=method
            )
            assert len(result) > 0
            assert 'timestamp' in result.columns


class TestSmartAggregationEngine:
    """Test smart aggregation engine."""

    @pytest.fixture
    def engine(self):
        """Create smart aggregation engine."""
        return SmartAggregationEngine()

    @pytest.fixture
    def sample_data(self):
        """Create sample data."""
        timestamps = pd.date_range('2024-01-01', periods=7200, freq='1s')  # 2 hours
        data = pd.DataFrame({
            'timestamp': timestamps,
            'sensor_name': 'test_sensor',
            'asset_id': 'asset_001',
            'temperature': np.random.normal(25, 2, 7200),  # Low variability
            'pressure': np.random.normal(1013, 50, 7200)   # Higher variability
        })
        return data

    def test_optimize_query_aggregation(self, engine, sample_data):
        """Test query aggregation optimization."""
        optimization = engine.optimize_query_aggregation(
            sample_data,
            target_interval_ms=1000,
            max_datapoints=1000,
            duration_hours=2.0
        )
        
        assert 'method' in optimization
        assert 'interval_ms' in optimization
        assert 'estimated_points' in optimization
        assert optimization['current_points'] == len(sample_data)

    def test_choose_aggregation_method_short_duration(self, engine, sample_data):
        """Test aggregation method selection for short duration."""
        method = engine._choose_aggregation_method(sample_data, 0.5)  # 30 minutes
        assert method == AggregationMethod.AVG

    def test_choose_aggregation_method_long_duration(self, engine, sample_data):
        """Test aggregation method selection for long duration."""
        method = engine._choose_aggregation_method(sample_data, 48.0)  # 48 hours
        assert method in [AggregationMethod.AVG, AggregationMethod.LAST]

    def test_calculate_optimal_interval(self, engine):
        """Test optimal interval calculation."""
        # Case where current points exceed max
        interval = engine._calculate_optimal_interval(
            current_points=10000,
            duration_hours=1.0,
            max_datapoints=1000,
            target_interval_ms=1000
        )
        assert interval >= 3600  # Should be larger than target

        # Case where current points are under max
        interval = engine._calculate_optimal_interval(
            current_points=500,
            duration_hours=1.0,
            max_datapoints=1000,
            target_interval_ms=1000
        )
        assert interval == 1000  # Should use target

    def test_apply_smart_aggregation(self, engine, sample_data):
        """Test smart aggregation application."""
        result = engine.apply_smart_aggregation(
            sample_data,
            target_interval_ms=1000,
            max_datapoints=1000,
            duration_hours=2.0
        )
        
        assert len(result) <= 1000
        assert 'timestamp' in result.columns

    def test_create_pre_aggregated_data(self, engine, sample_data):
        """Test pre-aggregated data creation."""
        result = engine.create_pre_aggregated_data(sample_data, interval_minutes=1)
        
        # Should have fewer rows than original (aggregated by minute)
        assert len(result) < len(sample_data)
        assert len(result) == 120  # 2 hours * 60 minutes
        
        # Should have min/max columns for numeric data
        temp_cols = [col for col in result.columns if 'temperature' in col]
        assert len(temp_cols) >= 3  # mean, min, max

    def test_empty_dataframe_handling(self, engine):
        """Test handling of empty DataFrames."""
        empty_df = pd.DataFrame()
        
        optimization = engine.optimize_query_aggregation(
            empty_df, 1000, 1000, 1.0
        )
        assert optimization['estimated_points'] == 0
        
        result = engine.apply_smart_aggregation(
            empty_df, 1000, 1000, 1.0
        )
        assert result.empty
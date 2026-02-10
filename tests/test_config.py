"""Tests for configuration module."""

import pytest
from datetime import datetime
from pathlib import Path
import os
from unittest.mock import patch

from app.config import (
    load_config, validate_config, AppConfig, StorageMode,
    AggregationMethod, get_tier_for_query, calculate_optimal_interval
)


class TestConfiguration:
    """Test configuration loading and validation."""

    def test_load_config_defaults(self):
        """Test loading configuration with default values."""
        with patch.dict(os.environ, {}, clear=True):
            config = load_config()
            
            assert config.storage_mode == StorageMode.HYBRID
            assert config.query.max_query_duration_hours == 168
            assert config.cache.enabled is True
            assert config.api.port == 8080

    def test_load_config_with_env_vars(self):
        """Test loading configuration with environment variables."""
        env_vars = {
            'STORAGE_MODE': 'azure',
            'AZURE_STORAGE_ACCOUNT': 'testaccount',
            'AZURE_STORAGE_KEY': 'testkey',
            'CACHE_SIZE_MB': '1024',
            'API_PORT': '9000'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()
            
            assert config.storage_mode == StorageMode.AZURE
            assert config.azure.storage_account == 'testaccount'
            assert config.cache.size_mb == 1024
            assert config.api.port == 9000

    def test_validate_config_azure_valid(self):
        """Test configuration validation for Azure mode."""
        config = load_config()
        config.storage_mode = StorageMode.AZURE
        config.azure.storage_account = 'testaccount'
        config.azure.storage_key = 'testkey'
        
        assert validate_config(config) is True

    def test_validate_config_azure_invalid(self):
        """Test configuration validation fails with missing Azure credentials."""
        config = load_config()
        config.storage_mode = StorageMode.AZURE
        config.azure.storage_account = ''
        config.azure.storage_key = ''
        
        assert validate_config(config) is False

    def test_validate_config_local_valid(self):
        """Test configuration validation for local mode."""
        config = load_config()
        config.storage_mode = StorageMode.LOCAL
        config.local_storage.data_path = Path('/tmp/test')
        
        assert validate_config(config) is True

    def test_validate_config_query_limits(self):
        """Test query limits validation."""
        config = load_config()
        config.query.max_absolute_datapoints = 1000
        config.query.default_max_datapoints = 2000  # Higher than absolute max
        
        assert validate_config(config) is False

    def test_aggregation_methods(self):
        """Test aggregation method enum values."""
        assert AggregationMethod.AVG == "avg"
        assert AggregationMethod.MIN == "min"
        assert AggregationMethod.MAX == "max"
        assert AggregationMethod.LAST == "last"


class TestTierSelection:
    """Test tier selection logic."""

    def test_get_tier_for_query_raw(self):
        """Test tier selection for short duration."""
        from app.config import TierConfig
        
        tier_config = TierConfig(
            raw_tier_max_hours=24,
            aggregated_tier_max_hours=168,
            daily_tier_threshold_hours=168
        )
        
        tier = get_tier_for_query(1.0, tier_config)  # 1 hour
        assert tier == "raw"

    def test_get_tier_for_query_aggregated(self):
        """Test tier selection for medium duration."""
        from app.config import TierConfig
        
        tier_config = TierConfig(
            raw_tier_max_hours=24,
            aggregated_tier_max_hours=168,
            daily_tier_threshold_hours=168
        )
        
        tier = get_tier_for_query(48.0, tier_config)  # 48 hours
        assert tier == "aggregated"

    def test_get_tier_for_query_daily(self):
        """Test tier selection for long duration."""
        from app.config import TierConfig
        
        tier_config = TierConfig(
            raw_tier_max_hours=24,
            aggregated_tier_max_hours=168,
            daily_tier_threshold_hours=168
        )
        
        tier = get_tier_for_query(300.0, tier_config)  # 300 hours
        assert tier == "daily"

    def test_calculate_optimal_interval(self):
        """Test optimal interval calculation."""
        # For 1 hour with 1000 max datapoints should give 1 second intervals
        interval = calculate_optimal_interval(1.0, 1000)
        assert interval == 1000  # 1 second

        # For 24 hours with 1000 max datapoints should give larger intervals
        interval = calculate_optimal_interval(24.0, 1000)
        assert interval >= 60000  # At least 1 minute

    def test_calculate_optimal_interval_edge_cases(self):
        """Test optimal interval calculation edge cases."""
        # Very short duration
        interval = calculate_optimal_interval(0.1, 1000)
        assert interval == 1000  # Minimum 1 second

        # Very long duration
        interval = calculate_optimal_interval(168.0, 100)
        assert interval >= 3600000  # At least 1 hour
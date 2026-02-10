"""Shared pytest fixtures for all tests."""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from app.config import (
    AppConfig, AzureConfig, LocalStorageConfig, QueryConfig, 
    CacheConfig, TierConfig, APIConfig, StorageMode
)


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_sensor_data():
    """Create sample sensor data for testing."""
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    timestamps = pd.date_range(start_time, periods=3600, freq='1s')
    
    data = pd.DataFrame({
        'timestamp': timestamps,
        'sensor_name': ['test_sensor'] * 3600,
        'asset_id': ['asset_001'] * 3600,
        'temperature': 25.0 + pd.Series(range(3600)) * 0.01,  # Gradual increase
        'humidity': 60.0 + pd.Series(range(3600)) * 0.005,    # Gradual increase
        'pressure': [1013.25] * 3600  # Constant
    })
    
    return data


@pytest.fixture
def multi_sensor_data():
    """Create multi-sensor data for testing."""
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    timestamps = pd.date_range(start_time, periods=1800, freq='1s')  # 30 minutes
    
    # Create data for multiple sensors
    sensors = ['temp_sensor', 'humidity_sensor', 'pressure_sensor']
    assets = ['asset_001', 'asset_002']
    
    data_frames = []
    for asset in assets:
        for sensor in sensors:
            df = pd.DataFrame({
                'timestamp': timestamps,
                'sensor_name': [sensor] * len(timestamps),
                'asset_id': [asset] * len(timestamps),
                'value': pd.Series(range(len(timestamps))) * 0.1 + hash(sensor + asset) % 100
            })
            data_frames.append(df)
    
    return pd.concat(data_frames, ignore_index=True)


@pytest.fixture
def app_config(temp_dir):
    """Create test application configuration."""
    return AppConfig(
        storage_mode=StorageMode.LOCAL,
        azure=AzureConfig(
            storage_account="testaccount",
            storage_key="testkey",
            container_name="test-container"
        ),
        local_storage=LocalStorageConfig(
            data_path=temp_dir / "data",
            enable_caching=True
        ),
        query=QueryConfig(
            max_query_duration_hours=24,
            default_max_datapoints=1000,
            max_absolute_datapoints=10000,
            enable_smart_aggregation=True
        ),
        cache=CacheConfig(
            enabled=True,
            size_mb=10,
            ttl_seconds=300,
            max_entries=100
        ),
        tiers=TierConfig(
            raw_tier_max_hours=1,
            aggregated_tier_max_hours=24,
            daily_tier_threshold_hours=168
        ),
        api=APIConfig(
            host="127.0.0.1",
            port=8080,
            workers=1,
            debug=True
        )
    )


@pytest.fixture
def mock_storage_backend():
    """Create mock storage backend."""
    backend = Mock()
    backend.list_files.return_value = [
        'asset_001/2024/01/01/00/test_sensor.parquet',
        'asset_001/2024/01/01/01/test_sensor.parquet',
        'asset_002/2024/01/01/00/temp_sensor.parquet'
    ]
    backend.file_exists.return_value = True
    backend.health_check.return_value = {'healthy': True}
    return backend


@pytest.fixture
def mock_query_engine(app_config, sample_sensor_data):
    """Create mock query engine for testing."""
    engine = Mock()
    
    # Mock query result
    from app.query.engine import QueryResult
    result = QueryResult(
        data=sample_sensor_data,
        metadata={
            'cache_hit': False,
            'tier_used': 'raw',
            'execution_time_ms': 50.0,
            'truncated': False
        }
    )
    
    engine.query_sensor_data.return_value = result
    engine.get_available_sensors.return_value = ['test_sensor', 'temp_sensor']
    engine.get_available_assets.return_value = ['asset_001', 'asset_002']
    engine.get_time_range.return_value = (
        datetime(2024, 1, 1),
        datetime(2024, 1, 2)
    )
    engine.config = app_config
    engine.health_check.return_value = {
        'overall_healthy': True,
        'storage_backends': {'local': {'healthy': True}},
        'cache_status': {'enabled': True}
    }
    engine.get_query_stats.return_value = {
        'total_queries': 100,
        'cache_hits': 20,
        'cache_hit_rate': 0.2,
        'avg_execution_time_ms': 45.5,
        'tier_usage': {'raw': 60, 'aggregated': 30, 'daily': 10},
        'total_execution_time_ms': 4550.0,
        'cache_stats': {
            'hits': 20,
            'misses': 80,
            'hit_rate': 0.2,
            'entries': 15,
            'size_mb': 2.5,
            'enabled': True
        }
    }
    
    return engine


@pytest.fixture(autouse=True)
def disable_logging():
    """Disable logging during tests to reduce noise."""
    import logging
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture
def mock_azure_client():
    """Mock Azure blob service client."""
    with patch('app.storage.azure_storage.BlobServiceClient') as mock_client:
        # Mock container client
        container_client = Mock()
        container_client.list_blobs.return_value = [
            Mock(name='asset_001/2024/01/01/00/sensor1.parquet'),
            Mock(name='asset_001/2024/01/01/01/sensor1.parquet')
        ]
        container_client.get_container_properties.return_value = Mock(
            last_modified=datetime.now()
        )
        
        # Mock blob client
        blob_client = Mock()
        blob_client.exists.return_value = True
        blob_client.download_blob.return_value = Mock()
        blob_client.get_blob_properties.return_value = Mock(
            size=1024,
            last_modified=datetime.now(),
            content_settings=Mock(content_type='application/octet-stream'),
            etag='test-etag'
        )
        
        mock_client_instance = Mock()
        mock_client_instance.get_container_client.return_value = container_client
        mock_client_instance.get_blob_client.return_value = blob_client
        mock_client.return_value = mock_client_instance
        
        yield mock_client_instance
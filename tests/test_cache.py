"""Tests for cache module."""

import pytest
import pandas as pd
import time
from datetime import datetime, timedelta
from unittest.mock import patch

from app.cache.cache_manager import QueryCache, SmartCacheManager
from app.config import CacheConfig


class TestQueryCache:
    """Test query cache functionality."""

    @pytest.fixture
    def cache_config(self):
        """Create cache configuration for testing."""
        return CacheConfig(
            enabled=True,
            size_mb=1,  # Small size for testing
            ttl_seconds=3600,
            max_entries=10
        )

    @pytest.fixture
    def cache(self, cache_config):
        """Create cache instance."""
        return QueryCache(cache_config)

    @pytest.fixture
    def sample_data(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='1s'),
            'sensor_name': 'test_sensor',
            'value': range(100)
        })

    def test_get_cache_key(self, cache):
        """Test cache key generation."""
        sensors = ['sensor1', 'sensor2']
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 1, 1, 0, 0)
        
        key1 = cache.get_cache_key(sensors, start_time, end_time)
        key2 = cache.get_cache_key(sensors, start_time, end_time)
        
        # Same parameters should generate same key
        assert key1 == key2
        
        # Different parameters should generate different keys
        key3 = cache.get_cache_key(sensors, start_time, end_time, interval_ms=60000)
        assert key1 != key3

    def test_put_and_get(self, cache, sample_data):
        """Test storing and retrieving data from cache."""
        cache_key = "test_key"
        
        # Store data
        success = cache.put(cache_key, sample_data)
        assert success is True
        
        # Retrieve data
        cached_data = cache.get(cache_key)
        assert cached_data is not None
        pd.testing.assert_frame_equal(cached_data, sample_data)

    def test_cache_miss(self, cache):
        """Test cache miss scenario."""
        cached_data = cache.get("nonexistent_key")
        assert cached_data is None

    def test_ttl_expiration(self, cache_config, sample_data):
        """Test TTL expiration."""
        # Use very short TTL
        cache_config.ttl_seconds = 1
        cache = QueryCache(cache_config)
        
        cache_key = "test_key"
        cache.put(cache_key, sample_data)
        
        # Should be available immediately
        cached_data = cache.get(cache_key)
        assert cached_data is not None
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be expired
        cached_data = cache.get(cache_key)
        assert cached_data is None

    def test_cache_eviction_by_size(self, cache, sample_data):
        """Test cache eviction when size limit reached."""
        # Fill cache beyond capacity
        for i in range(15):  # More than max_entries (10)
            cache_key = f"key_{i}"
            cache.put(cache_key, sample_data)
        
        # Early entries should be evicted
        assert cache.get("key_0") is None
        assert cache.get("key_14") is not None

    def test_cache_disabled(self, sample_data):
        """Test cache behavior when disabled."""
        config = CacheConfig(enabled=False, size_mb=1, ttl_seconds=3600, max_entries=10)
        cache = QueryCache(config)
        
        success = cache.put("key", sample_data)
        assert success is False
        
        cached_data = cache.get("key")
        assert cached_data is None

    def test_cache_stats(self, cache, sample_data):
        """Test cache statistics."""
        cache_key = "test_key"
        
        # Initial stats
        stats = cache.get_stats()
        assert stats['hits'] == 0
        assert stats['misses'] == 0
        assert stats['entries'] == 0

        # Cache miss
        cache.get(cache_key)
        stats = cache.get_stats()
        assert stats['misses'] == 1

        # Cache put and hit
        cache.put(cache_key, sample_data)
        cache.get(cache_key)
        stats = cache.get_stats()
        assert stats['hits'] == 1
        assert stats['entries'] == 1

    def test_clear_cache(self, cache, sample_data):
        """Test cache clearing."""
        cache.put("key1", sample_data)
        cache.put("key2", sample_data)
        
        assert cache.get("key1") is not None
        assert cache.get("key2") is not None
        
        cache.clear()
        
        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_cleanup_expired(self, cache_config, sample_data):
        """Test expired entry cleanup."""
        cache_config.ttl_seconds = 1
        cache = QueryCache(cache_config)
        
        cache.put("key1", sample_data)
        time.sleep(1.1)  # Let it expire
        cache.put("key2", sample_data)  # This one should not expire
        
        cache.cleanup_expired()
        
        assert cache.get("key1") is None
        assert cache.get("key2") is not None


class TestSmartCacheManager:
    """Test smart cache manager."""

    @pytest.fixture
    def cache_config(self):
        """Create cache configuration."""
        return CacheConfig(
            enabled=True,
            size_mb=10,
            ttl_seconds=3600,
            max_entries=100
        )

    @pytest.fixture
    def manager(self, cache_config):
        """Create smart cache manager."""
        return SmartCacheManager(cache_config)

    @pytest.fixture
    def sample_data(self):
        """Create sample data."""
        return pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=1000, freq='1s'),
            'sensor_name': 'test_sensor',
            'value': range(1000)
        })

    def test_should_cache_query_size_limit(self, manager):
        """Test caching decision based on result size."""
        # Very large result should not be cached
        should_cache = manager.should_cache_query(
            sensors=['sensor1'],
            duration_hours=1.0,
            result_size_mb=6.0  # More than 50% of 10MB cache
        )
        assert should_cache is False

        # Normal size result should be cached
        should_cache = manager.should_cache_query(
            sensors=['sensor1'],
            duration_hours=1.0,
            result_size_mb=1.0
        )
        assert should_cache is True

    def test_should_cache_query_duration(self, manager):
        """Test caching decision based on duration."""
        # Very short duration should not be cached
        should_cache = manager.should_cache_query(
            sensors=['sensor1'],
            duration_hours=0.05,  # 3 minutes
            result_size_mb=1.0
        )
        assert should_cache is False

        # Longer duration should be cached
        should_cache = manager.should_cache_query(
            sensors=['sensor1'],
            duration_hours=2.0,
            result_size_mb=1.0
        )
        assert should_cache is True

    def test_should_cache_query_multi_sensor(self, manager):
        """Test caching decision for multi-sensor queries."""
        # Complex multi-sensor query should be cached
        should_cache = manager.should_cache_query(
            sensors=['s1', 's2', 's3', 's4', 's5', 's6'],  # 6 sensors
            duration_hours=0.5,
            result_size_mb=1.0
        )
        assert should_cache is True

    def test_track_query_access(self, manager):
        """Test query access tracking."""
        cache_key = "test_key"
        
        # Track multiple accesses
        manager.track_query_access(cache_key)
        manager.track_query_access(cache_key)
        manager.track_query_access(cache_key)
        
        # Check frequency tracking
        assert manager._query_frequency[cache_key] == 3

    def test_get_adaptive_ttl(self, manager):
        """Test adaptive TTL calculation."""
        cache_key = "test_key"
        default_ttl = 3600
        
        # Low frequency query
        manager._query_frequency[cache_key] = 2
        ttl = manager.get_adaptive_ttl(cache_key, default_ttl)
        assert ttl == default_ttl

        # High frequency query
        manager._query_frequency[cache_key] = 12
        ttl = manager.get_adaptive_ttl(cache_key, default_ttl)
        assert ttl == default_ttl * 3  # Triple TTL

    def test_get_cached_result(self, manager, sample_data):
        """Test cached result retrieval with tracking."""
        sensors = ['sensor1']
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 1, 1, 0, 0)
        
        # Cache some data
        manager.cache_result(sample_data, sensors, start_time, end_time)
        
        # Retrieve it
        result = manager.get_cached_result(sensors, start_time, end_time)
        assert result is not None
        pd.testing.assert_frame_equal(result, sample_data)

    def test_cache_result_with_smart_logic(self, manager, sample_data):
        """Test result caching with smart logic."""
        sensors = ['sensor1']
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 1, 2, 0, 0)  # 2 hours
        
        success = manager.cache_result(sample_data, sensors, start_time, end_time)
        assert success is True

    def test_get_popular_queries(self, manager):
        """Test popular query retrieval."""
        # Track some queries with different frequencies
        keys = ['key1', 'key2', 'key3']
        frequencies = [10, 5, 15]
        
        for key, freq in zip(keys, frequencies):
            manager._query_frequency[key] = freq
            manager._query_last_access[key] = time.time()
        
        popular = manager.get_popular_queries(limit=2)
        assert len(popular) == 2
        assert popular[0]['frequency'] == 15  # Most popular first

    def test_cleanup_frequency_tracking(self, manager):
        """Test frequency tracking cleanup."""
        old_time = time.time() - 25 * 3600  # 25 hours ago
        recent_time = time.time()
        
        manager._query_frequency['old_key'] = 5
        manager._query_last_access['old_key'] = old_time
        
        manager._query_frequency['recent_key'] = 3
        manager._query_last_access['recent_key'] = recent_time
        
        manager.cleanup_frequency_tracking(max_age_hours=24)
        
        assert 'old_key' not in manager._query_frequency
        assert 'recent_key' in manager._query_frequency

    def test_get_cache_stats(self, manager):
        """Test comprehensive cache statistics."""
        # Add some tracking data
        manager._query_frequency['key1'] = 10
        manager._query_frequency['key2'] = 3
        
        stats = manager.get_cache_stats()
        
        assert 'hits' in stats
        assert 'misses' in stats
        assert 'tracked_queries' in stats
        assert 'total_accesses' in stats
        assert 'popular_queries' in stats
        assert stats['tracked_queries'] == 2
        assert stats['total_accesses'] == 13
        assert stats['popular_queries'] == 1  # Only key1 has > 5 accesses

    def test_clear_all(self, manager, sample_data):
        """Test clearing all cache data and tracking."""
        # Add some data and tracking
        manager.cache_result(sample_data, ['sensor1'], datetime.now(), datetime.now())
        manager._query_frequency['key1'] = 5
        
        manager.clear_all()
        
        assert len(manager._query_frequency) == 0
        assert len(manager._query_last_access) == 0
"""
Intelligent caching system for query results.
"""

import logging
import hashlib
import pickle
import time
from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta
from threading import Lock
import pandas as pd
from collections import OrderedDict

from app.config import CacheConfig

logger = logging.getLogger(__name__)


class QueryCache:
    """LRU cache with TTL for query results."""
    
    def __init__(self, config: CacheConfig):
        """Initialize query cache."""
        self.config = config
        self.enabled = config.enabled
        
        if not self.enabled:
            logger.info("Query cache disabled")
            return
        
        self.max_size_bytes = config.size_mb * 1024 * 1024
        self.ttl_seconds = config.ttl_seconds
        self.max_entries = config.max_entries
        
        # LRU cache storage
        self._cache: OrderedDict = OrderedDict()
        self._cache_info: Dict[str, Dict] = {}
        self._lock = Lock()
        self._current_size = 0
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'entries': 0,
            'size_bytes': 0
        }
        
        logger.info(f"Initialized query cache: {config.size_mb}MB, {config.max_entries} entries, {config.ttl_seconds}s TTL")
    
    def get_cache_key(self, sensors: List[str], start_time: datetime, end_time: datetime,
                     asset_ids: Optional[List[str]] = None, interval_ms: Optional[int] = None,
                     aggregation: Optional[str] = None, max_datapoints: Optional[int] = None) -> str:
        """Generate a cache key for query parameters."""
        # Create a stable key from query parameters
        key_data = {
            'sensors': sorted(sensors),
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'asset_ids': sorted(asset_ids) if asset_ids else None,
            'interval_ms': interval_ms,
            'aggregation': aggregation,
            'max_datapoints': max_datapoints
        }
        
        # Hash the key data
        key_str = str(key_data)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Get cached result."""
        if not self.enabled:
            return None
        
        with self._lock:
            if cache_key not in self._cache:
                self.stats['misses'] += 1
                return None
            
            # Check TTL
            cache_info = self._cache_info[cache_key]
            if time.time() - cache_info['timestamp'] > self.ttl_seconds:
                # Expired, remove from cache
                self._remove_entry(cache_key)
                self.stats['misses'] += 1
                return None
            
            # Move to end (most recently used)
            self._cache.move_to_end(cache_key)
            self.stats['hits'] += 1
            
            # Deserialize DataFrame
            try:
                return pickle.loads(self._cache[cache_key])
            except Exception as e:
                logger.error(f"Error deserializing cached data: {e}")
                self._remove_entry(cache_key)
                self.stats['misses'] += 1
                return None
    
    def put(self, cache_key: str, data: pd.DataFrame) -> bool:
        """Store result in cache."""
        if not self.enabled:
            return False
        
        try:
            # Serialize DataFrame
            serialized_data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
            data_size = len(serialized_data)
            
            with self._lock:
                # Check if we need to make space
                self._make_space(data_size)
                
                # Remove existing entry if present
                if cache_key in self._cache:
                    self._remove_entry(cache_key)
                
                # Add new entry
                self._cache[cache_key] = serialized_data
                self._cache_info[cache_key] = {
                    'timestamp': time.time(),
                    'size': data_size,
                    'rows': len(data),
                    'columns': len(data.columns)
                }
                
                self._current_size += data_size
                self.stats['entries'] = len(self._cache)
                self.stats['size_bytes'] = self._current_size
                
                logger.debug(f"Cached query result: {len(data)} rows, {data_size} bytes")
                return True
                
        except Exception as e:
            logger.error(f"Error caching data: {e}")
            return False
    
    def _make_space(self, needed_size: int):
        """Make space for new entry by evicting old ones."""
        # Check size limit
        while (self._current_size + needed_size > self.max_size_bytes or 
               len(self._cache) >= self.max_entries) and self._cache:
            
            # Remove least recently used entry
            oldest_key = next(iter(self._cache))
            self._remove_entry(oldest_key)
            self.stats['evictions'] += 1
    
    def _remove_entry(self, cache_key: str):
        """Remove entry from cache."""
        if cache_key in self._cache:
            cache_info = self._cache_info[cache_key]
            self._current_size -= cache_info['size']
            del self._cache[cache_key]
            del self._cache_info[cache_key]
    
    def clear(self):
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            self._cache_info.clear()
            self._current_size = 0
            self.stats['entries'] = 0
            self.stats['size_bytes'] = 0
        
        logger.info("Cleared query cache")
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._lock:
            hit_rate = 0
            total_requests = self.stats['hits'] + self.stats['misses']
            if total_requests > 0:
                hit_rate = self.stats['hits'] / total_requests
            
            return {
                **self.stats,
                'hit_rate': hit_rate,
                'size_mb': self._current_size / (1024 * 1024),
                'enabled': self.enabled,
                'max_size_mb': self.config.size_mb,
                'max_entries': self.config.max_entries,
                'ttl_seconds': self.config.ttl_seconds
            }
    
    def cleanup_expired(self):
        """Remove expired entries."""
        if not self.enabled:
            return
        
        current_time = time.time()
        expired_keys = []
        
        with self._lock:
            for key, info in self._cache_info.items():
                if current_time - info['timestamp'] > self.ttl_seconds:
                    expired_keys.append(key)
            
            for key in expired_keys:
                self._remove_entry(key)
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")


class SmartCacheManager:
    """Advanced cache manager with intelligent caching strategies."""
    
    def __init__(self, config: CacheConfig):
        """Initialize smart cache manager."""
        self.cache = QueryCache(config)
        self.config = config
        
        # Popular query tracking
        self._query_frequency: Dict[str, int] = {}
        self._query_last_access: Dict[str, float] = {}
        self._frequency_lock = Lock()
        
        # Adaptive TTL based on query patterns
        self._adaptive_ttl_enabled = True
        
    def should_cache_query(self, sensors: List[str], duration_hours: float, 
                          result_size_mb: float) -> bool:
        """Determine if a query result should be cached."""
        if not self.cache.enabled:
            return False
        
        # Don't cache very large results that would dominate cache
        if result_size_mb > self.config.size_mb * 0.5:  # More than 50% of cache
            return False
        
        # Don't cache very small time ranges (likely real-time queries)
        if duration_hours < 0.1:  # Less than 6 minutes
            return False
        
        # Always cache complex multi-sensor queries
        if len(sensors) > 5:
            return True
        
        # Cache historical queries (older than 1 hour)
        now = datetime.utcnow()
        if duration_hours > 1:  # Historical data
            return True
        
        return True
    
    def get_adaptive_ttl(self, cache_key: str, default_ttl: int) -> int:
        """Get adaptive TTL based on query frequency."""
        if not self._adaptive_ttl_enabled:
            return default_ttl
        
        with self._frequency_lock:
            frequency = self._query_frequency.get(cache_key, 0)
        
        # More frequent queries get longer TTL
        if frequency > 10:
            return default_ttl * 3  # Triple TTL for very popular queries
        elif frequency > 5:
            return default_ttl * 2  # Double TTL for popular queries
        else:
            return default_ttl
    
    def track_query_access(self, cache_key: str):
        """Track query access for frequency analysis."""
        with self._frequency_lock:
            self._query_frequency[cache_key] = self._query_frequency.get(cache_key, 0) + 1
            self._query_last_access[cache_key] = time.time()
    
    def get_cached_result(self, sensors: List[str], start_time: datetime, end_time: datetime,
                         asset_ids: Optional[List[str]] = None, interval_ms: Optional[int] = None,
                         aggregation: Optional[str] = None, max_datapoints: Optional[int] = None) -> Optional[pd.DataFrame]:
        """Get cached query result with smart tracking."""
        cache_key = self.cache.get_cache_key(sensors, start_time, end_time, asset_ids, 
                                           interval_ms, aggregation, max_datapoints)
        
        self.track_query_access(cache_key)
        return self.cache.get(cache_key)
    
    def cache_result(self, result: pd.DataFrame, sensors: List[str], start_time: datetime, 
                    end_time: datetime, asset_ids: Optional[List[str]] = None,
                    interval_ms: Optional[int] = None, aggregation: Optional[str] = None,
                    max_datapoints: Optional[int] = None) -> bool:
        """Cache query result with smart caching logic."""
        duration_hours = (end_time - start_time).total_seconds() / 3600
        
        # Estimate result size
        result_size_mb = result.memory_usage(deep=True).sum() / (1024 * 1024)
        
        if not self.should_cache_query(sensors, duration_hours, result_size_mb):
            return False
        
        cache_key = self.cache.get_cache_key(sensors, start_time, end_time, asset_ids,
                                           interval_ms, aggregation, max_datapoints)
        
        return self.cache.put(cache_key, result)
    
    def get_popular_queries(self, limit: int = 10) -> List[Dict]:
        """Get most popular queries for cache warming."""
        with self._frequency_lock:
            sorted_queries = sorted(
                self._query_frequency.items(),
                key=lambda x: x[1],
                reverse=True
            )[:limit]
            
            return [
                {
                    'cache_key': key,
                    'frequency': freq,
                    'last_access': self._query_last_access.get(key, 0)
                }
                for key, freq in sorted_queries
            ]
    
    def cleanup_frequency_tracking(self, max_age_hours: int = 24):
        """Clean up old frequency tracking data."""
        current_time = time.time()
        cutoff_time = current_time - (max_age_hours * 3600)
        
        with self._frequency_lock:
            old_keys = [
                key for key, last_access in self._query_last_access.items()
                if last_access < cutoff_time
            ]
            
            for key in old_keys:
                self._query_frequency.pop(key, None)
                self._query_last_access.pop(key, None)
        
        if old_keys:
            logger.info(f"Cleaned up {len(old_keys)} old query frequency records")
    
    def get_cache_stats(self) -> Dict:
        """Get comprehensive cache statistics."""
        cache_stats = self.cache.get_stats()
        
        with self._frequency_lock:
            frequency_stats = {
                'tracked_queries': len(self._query_frequency),
                'total_accesses': sum(self._query_frequency.values()),
                'popular_queries': len([f for f in self._query_frequency.values() if f > 5])
            }
        
        return {
            **cache_stats,
            **frequency_stats,
            'adaptive_ttl_enabled': self._adaptive_ttl_enabled
        }
    
    def clear_all(self):
        """Clear all cache data and tracking."""
        self.cache.clear()
        
        with self._frequency_lock:
            self._query_frequency.clear()
            self._query_last_access.clear()
        
        logger.info("Cleared all cache data and tracking")
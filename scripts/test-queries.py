#!/usr/bin/env python3
"""
Test script for query service endpoints.
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any

# Configuration
BASE_URL = "http://localhost:8080"
API_BASE = f"{BASE_URL}/api/v1"

def test_health_check():
    """Test health check endpoint."""
    print("Testing health check...")
    
    response = requests.get(f"{BASE_URL}/health")
    
    if response.status_code == 200:
        health = response.json()
        print(f"✓ Health check passed - Overall: {'healthy' if health['overall_healthy'] else 'unhealthy'}")
        return True
    else:
        print(f"✗ Health check failed - Status: {response.status_code}")
        return False

def test_sensors_endpoint():
    """Test sensors listing endpoint."""
    print("Testing sensors endpoint...")
    
    response = requests.get(f"{API_BASE}/sensors")
    
    if response.status_code == 200:
        sensors = response.json()
        print(f"✓ Found {sensors['total_count']} sensors")
        if sensors['sensors']:
            print(f"  Example: {sensors['sensors'][0]['name']}")
        return sensors['sensors'][:5]  # Return first 5 for testing
    else:
        print(f"✗ Sensors endpoint failed - Status: {response.status_code}")
        return []

def test_assets_endpoint():
    """Test assets listing endpoint."""
    print("Testing assets endpoint...")
    
    response = requests.get(f"{API_BASE}/assets")
    
    if response.status_code == 200:
        assets = response.json()
        print(f"✓ Found {assets['total_count']} assets")
        if assets['assets']:
            print(f"  Example: {assets['assets'][0]['id']}")
        return assets['assets'][:3]  # Return first 3 for testing
    else:
        print(f"✗ Assets endpoint failed - Status: {response.status_code}")
        return []

def test_time_range(sensors):
    """Test time range endpoint."""
    if not sensors:
        print("Skipping time range test - no sensors available")
        return None, None
    
    print("Testing time range endpoint...")
    
    sensor_names = ",".join([s['name'] for s in sensors])
    response = requests.get(f"{API_BASE}/timerange?sensors={sensor_names}")
    
    if response.status_code == 200:
        time_range = response.json()
        print(f"✓ Time range: {time_range['min_time']} to {time_range['max_time']}")
        if time_range['duration_hours']:
            print(f"  Duration: {time_range['duration_hours']:.1f} hours")
        return time_range['min_time'], time_range['max_time']
    else:
        print(f"✗ Time range endpoint failed - Status: {response.status_code}")
        return None, None

def test_basic_query(sensors, min_time, max_time):
    """Test basic query endpoint."""
    if not sensors or not min_time:
        print("Skipping basic query test - insufficient data")
        return
    
    print("Testing basic query...")
    
    # Use first sensor and a 1-hour window
    sensor_name = sensors[0]['name']
    start_time = min_time
    end_time_obj = datetime.fromisoformat(min_time.replace('Z', '+00:00')) + timedelta(hours=1)
    end_time = end_time_obj.isoformat().replace('+00:00', 'Z')
    
    params = {
        'sensors': sensor_name,
        'start_time': start_time,
        'end_time': end_time,
        'max_datapoints': 100
    }
    
    response = requests.get(f"{API_BASE}/query", params=params)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Query returned {result['count']} data points")
        print(f"  Tier used: {result['metadata']['tier_used']}")
        print(f"  Cache hit: {result['metadata']['cache_hit']}")
        print(f"  Execution time: {result['metadata']['execution_time_ms']:.2f}ms")
        
        if result['data']:
            sample = result['data'][0]
            print(f"  Sample data: {list(sample.keys())}")
    else:
        print(f"✗ Basic query failed - Status: {response.status_code}")
        if response.content:
            print(f"  Error: {response.text}")

def test_post_query(sensors, min_time, max_time):
    """Test POST query endpoint."""
    if not sensors or not min_time:
        print("Skipping POST query test - insufficient data")
        return
    
    print("Testing POST query...")
    
    # Use multiple sensors and longer time range
    sensor_names = [s['name'] for s in sensors[:2]]
    start_time = min_time
    end_time_obj = datetime.fromisoformat(min_time.replace('Z', '+00:00')) + timedelta(hours=6)
    end_time = end_time_obj.isoformat().replace('+00:00', 'Z')
    
    payload = {
        "sensors": sensor_names,
        "start_time": start_time,
        "end_time": end_time,
        "interval_ms": 60000,  # 1 minute intervals
        "max_datapoints": 500,
        "aggregation": "avg"
    }
    
    response = requests.post(f"{API_BASE}/query", json=payload)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ POST query returned {result['count']} data points")
        print(f"  Tier used: {result['metadata']['tier_used']}")
        print(f"  Cache hit: {result['metadata']['cache_hit']}")
        print(f"  Execution time: {result['metadata']['execution_time_ms']:.2f}ms")
    else:
        print(f"✗ POST query failed - Status: {response.status_code}")
        if response.content:
            print(f"  Error: {response.text}")

def test_stats_endpoint():
    """Test statistics endpoint."""
    print("Testing stats endpoint...")
    
    response = requests.get(f"{API_BASE}/stats")
    
    if response.status_code == 200:
        stats = response.json()
        print(f"✓ Stats retrieved")
        print(f"  Total queries: {stats['query_stats']['total_queries']}")
        print(f"  Cache hit rate: {stats['query_stats']['cache_hit_rate']:.2%}")
        print(f"  Cache size: {stats['cache_stats']['size_mb']:.1f}MB")
        print(f"  Uptime: {stats['uptime_seconds']:.1f}s")
    else:
        print(f"✗ Stats endpoint failed - Status: {response.status_code}")

def test_cache_operations():
    """Test cache operations."""
    print("Testing cache operations...")
    
    # Clear cache
    response = requests.post(f"{API_BASE}/cache/clear")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Cache cleared: {result['message']}")
    else:
        print(f"✗ Cache clear failed - Status: {response.status_code}")

def test_error_handling():
    """Test error handling."""
    print("Testing error handling...")
    
    # Invalid date range
    params = {
        'sensors': 'invalid_sensor',
        'start_time': '2024-01-01T00:00:00Z',
        'end_time': '2023-01-01T00:00:00Z',  # End before start
        'max_datapoints': 100
    }
    
    response = requests.get(f"{API_BASE}/query", params=params)
    
    if response.status_code == 400:
        print("✓ Error handling works - correctly rejected invalid date range")
    else:
        print(f"✗ Error handling failed - Expected 400, got {response.status_code}")

def performance_test(sensors, min_time, max_time):
    """Simple performance test."""
    if not sensors or not min_time:
        print("Skipping performance test - insufficient data")
        return
    
    print("Running performance test...")
    
    sensor_name = sensors[0]['name']
    start_time = min_time
    end_time_obj = datetime.fromisoformat(min_time.replace('Z', '+00:00')) + timedelta(hours=1)
    end_time = end_time_obj.isoformat().replace('+00:00', 'Z')
    
    params = {
        'sensors': sensor_name,
        'start_time': start_time,
        'end_time': end_time,
        'max_datapoints': 1000
    }
    
    # Run multiple queries to test caching
    times = []
    for i in range(5):
        start = time.time()
        response = requests.get(f"{API_BASE}/query", params=params)
        duration = time.time() - start
        times.append(duration)
        
        if response.status_code == 200:
            result = response.json()
            cache_hit = result['metadata']['cache_hit']
            print(f"  Query {i+1}: {duration*1000:.2f}ms (cache: {'hit' if cache_hit else 'miss'})")
        else:
            print(f"  Query {i+1}: Failed ({response.status_code})")
    
    if times:
        avg_time = sum(times) / len(times)
        print(f"✓ Average query time: {avg_time*1000:.2f}ms")

def main():
    """Run all tests."""
    print("=" * 60)
    print("Sensor Data Query Service Test Suite")
    print("=" * 60)
    
    # Test basic connectivity
    if not test_health_check():
        print("Service not healthy - aborting tests")
        return
    
    print()
    
    # Test discovery endpoints
    sensors = test_sensors_endpoint()
    assets = test_assets_endpoint()
    
    print()
    
    # Test time range
    min_time, max_time = test_time_range(sensors)
    
    print()
    
    # Test query endpoints
    test_basic_query(sensors, min_time, max_time)
    test_post_query(sensors, min_time, max_time)
    
    print()
    
    # Test management endpoints
    test_stats_endpoint()
    test_cache_operations()
    
    print()
    
    # Test error handling
    test_error_handling()
    
    print()
    
    # Performance test
    performance_test(sensors, min_time, max_time)
    
    print()
    print("=" * 60)
    print("Test suite completed!")
    print("=" * 60)

if __name__ == "__main__":
    main()
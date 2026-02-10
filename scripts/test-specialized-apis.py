#!/usr/bin/env python3
"""
Test script for specialized raw and aggregated data APIs.
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
    
    response = requests.get(f"{BASE_URL}/health/simple")
    
    if response.status_code == 200:
        health = response.json()
        print(f"✓ Health check passed - Status: {health['status']}")
        return True
    else:
        print(f"✗ Health check failed - Status: {response.status_code}")
        return False

def test_config_endpoint():
    """Test configuration endpoint."""
    print("Testing configuration endpoint...")
    
    response = requests.get(f"{API_BASE}/config")
    
    if response.status_code == 200:
        config = response.json()
        print(f"✓ Config retrieved:")
        print(f"  Max datapoints: {config['max_datapoints']}")
        print(f"  Storage mode: {config['storage_mode']}")
        print(f"  Supported aggregations: {config['supported_aggregations']}")
        return config
    else:
        print(f"✗ Config endpoint failed - Status: {response.status_code}")
        return None

def test_sensors_endpoint():
    """Test sensors listing endpoint."""
    print("Testing sensors endpoint...")
    
    response = requests.get(f"{API_BASE}/sensors")
    
    if response.status_code == 200:
        sensors = response.json()
        print(f"✓ Found {sensors['total_count']} sensors")
        if sensors['sensors']:
            print(f"  Example sensors: {[s['name'] for s in sensors['sensors'][:3]]}")
        return [s['name'] for s in sensors['sensors'][:5]]  # Return first 5 for testing
    else:
        print(f"✗ Sensors endpoint failed - Status: {response.status_code}")
        return []

def test_time_range(sensors):
    """Test time range endpoint."""
    if not sensors:
        print("Skipping time range test - no sensors available")
        return None, None
    
    print("Testing time range endpoint...")
    
    sensor_types = ",".join(sensors[:2])  # Use first 2 sensors
    response = requests.get(f"{API_BASE}/timerange?sensor_types={sensor_types}")
    
    if response.status_code == 200:
        time_range = response.json()
        print(f"✓ Time range: {time_range['min_date']} to {time_range['max_date']}")
        if time_range['duration_hours']:
            print(f"  Duration: {time_range['duration_hours']:.1f} hours")
        return time_range['min_date'], time_range['max_date']
    else:
        print(f"✗ Time range endpoint failed - Status: {response.status_code}")
        return None, None

def test_interval_recommendation(sensors, min_date, max_date):
    """Test interval recommendation endpoint."""
    if not sensors or not min_date:
        print("Skipping interval recommendation test - insufficient data")
        return None
    
    print("Testing interval recommendation...")
    
    # Use a 6-hour window for testing
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(hours=6)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    sensor_types = ",".join(sensors[:2])
    
    response = requests.get(
        f"{API_BASE}/interval/recommend?start_date={start_date}&end_date={end_date}&sensor_types={sensor_types}"
    )
    
    if response.status_code == 200:
        recommendation = response.json()
        print(f"✓ Recommended interval: {recommendation['recommended_interval_ms']}ms")
        print(f"  Estimated datapoints: {recommendation['estimated_datapoints']}")
        return recommendation
    else:
        print(f"✗ Interval recommendation failed - Status: {response.status_code}")
        return None

def test_raw_data_api_get(sensors, min_date):
    """Test raw data GET endpoint."""
    if not sensors or not min_date:
        print("Skipping raw data GET test - insufficient data")
        return
    
    print("Testing raw data API (GET)...")
    
    # Use first sensor and 1-hour window
    sensor_types = sensors[0]
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(hours=1)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    response = requests.get(
        f"{API_BASE}/raw-data?start_date={start_date}&end_date={end_date}&sensor_types={sensor_types}"
    )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Raw data query returned {result['metadata']['total_data_points']} points")
        print(f"  Interval used: {result['metadata']['interval_ms_used']}ms")
        print(f"  Truncated: {result['metadata']['truncated']}")
        print(f"  Cache hit: {result['metadata']['cache_hit']}")
        print(f"  Execution time: {result['metadata']['execution_time_ms']:.2f}ms")
        
        if result['data']:
            sample = result['data'][0]
            print(f"  Sample fields: {list(sample.keys())}")
    else:
        print(f"✗ Raw data GET failed - Status: {response.status_code}")
        if response.content:
            print(f"  Error: {response.text}")

def test_raw_data_api_post(sensors, min_date):
    """Test raw data POST endpoint."""
    if not sensors or not min_date:
        print("Skipping raw data POST test - insufficient data")
        return
    
    print("Testing raw data API (POST)...")
    
    # Use multiple sensors and 30-minute window
    sensor_types = sensors[:2]
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(minutes=30)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    payload = {
        "start_date": start_date,
        "end_date": end_date,
        "sensor_types": sensor_types
    }
    
    response = requests.post(f"{API_BASE}/raw-data", json=payload)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Raw data POST returned {result['metadata']['total_data_points']} points")
        print(f"  Sensors: {len(sensor_types)}")
        print(f"  Truncated: {result['metadata']['truncated']}")
        print(f"  Execution time: {result['metadata']['execution_time_ms']:.2f}ms")
    else:
        print(f"✗ Raw data POST failed - Status: {response.status_code}")
        if response.content:
            print(f"  Error: {response.text}")

def test_aggregated_data_api_get(sensors, min_date):
    """Test aggregated data GET endpoint."""
    if not sensors or not min_date:
        print("Skipping aggregated data GET test - insufficient data")
        return
    
    print("Testing aggregated data API (GET)...")
    
    # Use multiple sensors and 6-hour window
    sensor_types = ",".join(sensors[:2])
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(hours=6)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    response = requests.get(
        f"{API_BASE}/aggregated-data?start_date={start_date}&end_date={end_date}"
        f"&sensor_types={sensor_types}&aggregation_type=mean&interval_ms=60000"
    )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Aggregated data GET returned {result['metadata']['total_data_points']} points")
        print(f"  Interval used: {result['metadata']['interval_ms_used']}ms")
        print(f"  Truncated: {result['metadata']['truncated']}")
        print(f"  Tier used: {result['metadata']['tier_used']}")
        print(f"  Execution time: {result['metadata']['execution_time_ms']:.2f}ms")
    else:
        print(f"✗ Aggregated data GET failed - Status: {response.status_code}")
        if response.content:
            print(f"  Error: {response.text}")

def test_aggregated_data_api_post(sensors, min_date):
    """Test aggregated data POST endpoint."""
    if not sensors or not min_date:
        print("Skipping aggregated data POST test - insufficient data")
        return
    
    print("Testing aggregated data API (POST)...")
    
    # Use multiple sensors and 24-hour window with auto-interval calculation
    sensor_types = sensors[:3]
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(hours=24)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    payload = {
        "start_date": start_date,
        "end_date": end_date,
        "sensor_types": sensor_types,
        "aggregation_type": "mean"
        # interval_ms not provided - should be auto-calculated
    }
    
    response = requests.post(f"{API_BASE}/aggregated-data", json=payload)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Aggregated data POST returned {result['metadata']['total_data_points']} points")
        print(f"  Auto-calculated interval: {result['metadata']['interval_ms_used']}ms")
        print(f"  Sensors: {len(sensor_types)}")
        print(f"  Truncated: {result['metadata']['truncated']}")
        print(f"  Execution time: {result['metadata']['execution_time_ms']:.2f}ms")
    else:
        print(f"✗ Aggregated data POST failed - Status: {response.status_code}")
        if response.content:
            print(f"  Error: {response.text}")

def test_aggregation_types(sensors, min_date):
    """Test different aggregation types."""
    if not sensors or not min_date:
        print("Skipping aggregation types test - insufficient data")
        return
    
    print("Testing different aggregation types...")
    
    sensor_types = sensors[0]
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(hours=2)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    aggregations = ['min', 'max', 'mean']
    
    for agg_type in aggregations:
        response = requests.get(
            f"{API_BASE}/aggregated-data?start_date={start_date}&end_date={end_date}"
            f"&sensor_types={sensor_types}&aggregation_type={agg_type}&interval_ms=300000"  # 5 minutes
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ {agg_type.upper()}: {result['metadata']['total_data_points']} points")
        else:
            print(f"✗ {agg_type.upper()}: Failed ({response.status_code})")

def test_error_handling():
    """Test error handling."""
    print("Testing error handling...")
    
    # Test invalid date range
    response = requests.get(
        f"{API_BASE}/raw-data?start_date=2024-01-02T00:00:00Z&end_date=2024-01-01T00:00:00Z&sensor_types=quad_ch1"
    )
    
    if response.status_code == 400:
        print("✓ Error handling works - correctly rejected invalid date range")
    else:
        print(f"✗ Error handling failed - Expected 400, got {response.status_code}")
    
    # Test missing parameters
    response = requests.get(f"{API_BASE}/raw-data")
    
    if response.status_code == 422:  # Validation error
        print("✓ Error handling works - correctly rejected missing parameters")
    else:
        print(f"✗ Error handling failed - Expected 422, got {response.status_code}")

def test_performance_comparison(sensors, min_date):
    """Compare performance between raw and aggregated APIs."""
    if not sensors or not min_date:
        print("Skipping performance comparison - insufficient data")
        return
    
    print("Testing performance comparison...")
    
    sensor_types = sensors[0]
    start_date = min_date
    end_date_obj = datetime.fromisoformat(min_date.replace('Z', '+00:00')) + timedelta(hours=1)
    end_date = end_date_obj.isoformat().replace('+00:00', 'Z')
    
    # Test raw data performance
    start_time = time.time()
    raw_response = requests.get(
        f"{API_BASE}/raw-data?start_date={start_date}&end_date={end_date}&sensor_types={sensor_types}"
    )
    raw_time = time.time() - start_time
    
    # Test aggregated data performance (1-minute intervals)
    start_time = time.time()
    agg_response = requests.get(
        f"{API_BASE}/aggregated-data?start_date={start_date}&end_date={end_date}"
        f"&sensor_types={sensor_types}&aggregation_type=mean&interval_ms=60000"
    )
    agg_time = time.time() - start_time
    
    if raw_response.status_code == 200 and agg_response.status_code == 200:
        raw_result = raw_response.json()
        agg_result = agg_response.json()
        
        print(f"✓ Performance comparison:")
        print(f"  Raw API: {raw_result['metadata']['total_data_points']} points in {raw_time*1000:.2f}ms")
        print(f"  Aggregated API: {agg_result['metadata']['total_data_points']} points in {agg_time*1000:.2f}ms")
        print(f"  Data reduction: {raw_result['metadata']['total_data_points'] / max(1, agg_result['metadata']['total_data_points']):.1f}x")

def main():
    """Run all tests for specialized APIs."""
    print("=" * 60)
    print("Specialized Sensor Data APIs Test Suite")
    print("=" * 60)
    
    # Test basic connectivity
    if not test_health_check():
        print("Service not healthy - aborting tests")
        return
    
    print()
    
    # Test configuration
    config = test_config_endpoint()
    if config:
        print(f"Max datapoints limit: {config['max_datapoints']}")
    
    print()
    
    # Test discovery endpoints
    sensors = test_sensors_endpoint()
    min_date, max_date = test_time_range(sensors)
    
    print()
    
    # Test helper endpoints
    recommendation = test_interval_recommendation(sensors, min_date, max_date)
    
    print()
    
    # Test Raw Data API
    print("=== RAW DATA API TESTS ===")
    test_raw_data_api_get(sensors, min_date)
    test_raw_data_api_post(sensors, min_date)
    
    print()
    
    # Test Aggregated Data API
    print("=== AGGREGATED DATA API TESTS ===")
    test_aggregated_data_api_get(sensors, min_date)
    test_aggregated_data_api_post(sensors, min_date)
    
    print()
    
    # Test different aggregation types
    test_aggregation_types(sensors, min_date)
    
    print()
    
    # Test error handling
    test_error_handling()
    
    print()
    
    # Performance comparison
    test_performance_comparison(sensors, min_date)
    
    print()
    print("=" * 60)
    print("Specialized APIs Test Suite Completed!")
    print("=" * 60)
    print()
    print("API Examples:")
    print("Raw Data (GET):")
    print("  curl 'http://localhost:8080/api/v1/raw-data?start_date=2024-01-01T00:00:00Z&end_date=2024-01-01T01:00:00Z&sensor_types=quad_ch1'")
    print()
    print("Aggregated Data (GET):")
    print("  curl 'http://localhost:8080/api/v1/aggregated-data?start_date=2024-01-01T00:00:00Z&end_date=2024-01-01T06:00:00Z&sensor_types=quad_ch1,quad_ch2&aggregation_type=mean&interval_ms=60000'")
    print()
    print("Interval Recommendation:")
    print("  curl 'http://localhost:8080/api/v1/interval/recommend?start_date=2024-01-01T00:00:00Z&end_date=2024-01-02T00:00:00Z&sensor_types=quad_ch1,quad_ch2'")

if __name__ == "__main__":
    main()
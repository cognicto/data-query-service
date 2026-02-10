"""
Configuration management for the sensor data query service.
"""

import os
from typing import List, Optional
from dataclasses import dataclass
from pathlib import Path
from enum import Enum


class StorageMode(str, Enum):
    """Storage mode options."""
    AZURE = "azure"
    LOCAL = "local"
    HYBRID = "hybrid"


class AggregationMethod(str, Enum):
    """Aggregation methods."""
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    LAST = "last"
    FIRST = "first"
    COUNT = "count"
    SUM = "sum"


@dataclass
class AzureConfig:
    """Azure Blob Storage configuration."""
    storage_account: str = ""
    storage_key: str = ""
    blob_endpoint: str = ""  # Full blob endpoint URL
    sas_token: str = ""  # SAS token (with or without leading ?)
    container_name: str = ""
    connection_timeout: int = 30
    retry_attempts: int = 3
    max_workers: int = 8


@dataclass
class LocalStorageConfig:
    """Local storage configuration."""
    data_path: Path
    enable_caching: bool = True
    cache_path: Optional[Path] = None


@dataclass
class QueryConfig:
    """Query engine configuration."""
    max_query_duration_hours: int = 168  # 7 days
    default_max_datapoints: int = 10000
    max_absolute_datapoints: int = 100000
    default_interval_ms: int = 1000
    enable_smart_aggregation: bool = True
    parallel_workers: int = 4


@dataclass
class CacheConfig:
    """Caching configuration."""
    enabled: bool = True
    size_mb: int = 512
    ttl_seconds: int = 3600  # 1 hour
    max_entries: int = 10000
    redis_url: Optional[str] = None  # Optional Redis backend


@dataclass
class TierConfig:
    """Multi-tier storage configuration."""
    raw_tier_max_hours: int = 24  # Use raw data for queries < 24 hours
    aggregated_tier_max_hours: int = 168  # Use pre-aggregated for < 7 days
    daily_tier_threshold_hours: int = 168  # Use daily summaries for > 7 days


@dataclass
class APIConfig:
    """API server configuration."""
    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 4
    debug: bool = False
    cors_origins: List[str] = None
    rate_limit: str = "100/minute"


@dataclass
class AppConfig:
    """Complete application configuration."""
    storage_mode: StorageMode
    azure: AzureConfig
    local_storage: LocalStorageConfig
    query: QueryConfig
    cache: CacheConfig
    tiers: TierConfig
    api: APIConfig


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    
    # Storage mode
    storage_mode = StorageMode(os.getenv("STORAGE_MODE", "hybrid"))
    
    # Azure configuration
    azure_config = AzureConfig(
        storage_account=os.getenv("AZURE_STORAGE_ACCOUNT", ""),
        storage_key=os.getenv("AZURE_STORAGE_KEY", ""),
        blob_endpoint=os.getenv("AZURE_BLOB_ENDPOINT", ""),
        sas_token=os.getenv("AZURE_SAS_TOKEN", ""),
        container_name=os.getenv("AZURE_CONTAINER_NAME", "sensor-data-cold-storage"),
        connection_timeout=int(os.getenv("AZURE_CONNECTION_TIMEOUT", "30")),
        retry_attempts=int(os.getenv("AZURE_RETRY_ATTEMPTS", "3")),
        max_workers=int(os.getenv("AZURE_MAX_WORKERS", "8"))
    )
    
    # Local storage configuration
    local_storage_config = LocalStorageConfig(
        data_path=Path(os.getenv("LOCAL_STORAGE_PATH", "/data")),
        enable_caching=os.getenv("LOCAL_ENABLE_CACHING", "true").lower() == "true",
        cache_path=Path(os.getenv("LOCAL_CACHE_PATH", "/tmp/query_cache")) if os.getenv("LOCAL_CACHE_PATH") else None
    )
    
    # Query configuration
    query_config = QueryConfig(
        max_query_duration_hours=int(os.getenv("MAX_QUERY_DURATION_HOURS", "168")),
        default_max_datapoints=int(os.getenv("DEFAULT_MAX_DATAPOINTS", "10000")),
        max_absolute_datapoints=int(os.getenv("MAX_ABSOLUTE_DATAPOINTS", "100000")),
        default_interval_ms=int(os.getenv("DEFAULT_INTERVAL_MS", "1000")),
        enable_smart_aggregation=os.getenv("ENABLE_SMART_AGGREGATION", "true").lower() == "true",
        parallel_workers=int(os.getenv("QUERY_PARALLEL_WORKERS", "4"))
    )
    
    # Cache configuration
    cache_config = CacheConfig(
        enabled=os.getenv("CACHE_ENABLED", "true").lower() == "true",
        size_mb=int(os.getenv("CACHE_SIZE_MB", "512")),
        ttl_seconds=int(os.getenv("CACHE_TTL_SECONDS", "3600")),
        max_entries=int(os.getenv("CACHE_MAX_ENTRIES", "10000")),
        redis_url=os.getenv("REDIS_URL")
    )
    
    # Tier configuration
    tier_config = TierConfig(
        raw_tier_max_hours=int(os.getenv("RAW_TIER_MAX_HOURS", "24")),
        aggregated_tier_max_hours=int(os.getenv("AGGREGATED_TIER_MAX_HOURS", "168")),
        daily_tier_threshold_hours=int(os.getenv("DAILY_TIER_THRESHOLD_HOURS", "168"))
    )
    
    # API configuration
    cors_origins = None
    if os.getenv("CORS_ORIGINS"):
        cors_origins = os.getenv("CORS_ORIGINS").split(",")
    
    api_config = APIConfig(
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8080")),
        workers=int(os.getenv("API_WORKERS", "4")),
        debug=os.getenv("API_DEBUG", "false").lower() == "true",
        cors_origins=cors_origins,
        rate_limit=os.getenv("API_RATE_LIMIT", "100/minute")
    )
    
    return AppConfig(
        storage_mode=storage_mode,
        azure=azure_config,
        local_storage=local_storage_config,
        query=query_config,
        cache=cache_config,
        tiers=tier_config,
        api=api_config
    )


def validate_config(config: AppConfig) -> bool:
    """Validate the configuration."""
    errors = []
    
    # Validate storage mode
    if config.storage_mode in [StorageMode.AZURE, StorageMode.HYBRID]:
        if not config.azure.storage_account:
            errors.append("AZURE_STORAGE_ACCOUNT is required when using Azure storage")
        if not config.azure.storage_key:
            errors.append("AZURE_STORAGE_KEY is required when using Azure storage")
    
    if config.storage_mode in [StorageMode.LOCAL, StorageMode.HYBRID]:
        if not config.local_storage.data_path:
            errors.append("LOCAL_STORAGE_PATH is required when using local storage")
    
    # Validate query limits
    if config.query.max_absolute_datapoints < config.query.default_max_datapoints:
        errors.append("MAX_ABSOLUTE_DATAPOINTS must be >= DEFAULT_MAX_DATAPOINTS")
    
    if config.query.max_query_duration_hours <= 0:
        errors.append("MAX_QUERY_DURATION_HOURS must be positive")
    
    # Validate tier configuration
    if config.tiers.raw_tier_max_hours >= config.tiers.aggregated_tier_max_hours:
        errors.append("RAW_TIER_MAX_HOURS must be < AGGREGATED_TIER_MAX_HOURS")
    
    if errors:
        for error in errors:
            print(f"Configuration error: {error}")
        return False
    
    return True


def get_tier_for_query(duration_hours: float, config: TierConfig) -> str:
    """Determine the optimal tier for a query based on duration."""
    if duration_hours <= config.raw_tier_max_hours:
        return "raw"
    elif duration_hours <= config.aggregated_tier_max_hours:
        return "aggregated"
    else:
        return "daily"


def calculate_optimal_interval(duration_hours: float, max_datapoints: int) -> int:
    """Calculate optimal interval to stay under max_datapoints."""
    # Convert duration to milliseconds
    duration_ms = duration_hours * 3600 * 1000
    
    # Calculate minimum interval needed
    min_interval_ms = duration_ms / max_datapoints
    
    # Round up to nearest reasonable interval
    if min_interval_ms <= 1000:  # 1 second
        return 1000
    elif min_interval_ms <= 5000:  # 5 seconds
        return 5000
    elif min_interval_ms <= 10000:  # 10 seconds
        return 10000
    elif min_interval_ms <= 30000:  # 30 seconds
        return 30000
    elif min_interval_ms <= 60000:  # 1 minute
        return 60000
    elif min_interval_ms <= 300000:  # 5 minutes
        return 300000
    elif min_interval_ms <= 600000:  # 10 minutes
        return 600000
    elif min_interval_ms <= 1800000:  # 30 minutes
        return 1800000
    elif min_interval_ms <= 3600000:  # 1 hour
        return 3600000
    else:  # More than 1 hour
        return int(min_interval_ms)
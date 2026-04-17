"""
Simplified configuration for DuckDB-only sensor data service.
"""

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class StorageMode(Enum):
    LOCAL = "local"
    AZURE = "azure"


@dataclass
class AzureConfig:
    """Azure Data Lake Storage Gen2 configuration."""
    account_name: str = ""
    container_name: str = ""
    sas_token: str = ""
    data_prefix: str = ""  # Optional path prefix
    
    @property
    def abfss_endpoint(self) -> str:
        """Get ADLS Gen2 endpoint URL."""
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net"


@dataclass
class LocalConfig:
    """Local storage configuration."""
    data_path: str = "/data"


@dataclass
class DuckDBConfig:
    """DuckDB configuration."""
    memory_limit: str = "4GB"
    threads: int = 4


@dataclass
class TierConfig:
    """Data tier selection configuration."""
    raw_threshold_minutes: int = 5           # Use raw if interval_ms < 5 minutes
    minute_threshold_minutes: int = 60       # Use minute agg if interval < 60 minutes  
    hourly_threshold_days: int = 30          # Use hourly agg if interval < 30 days
    # Beyond 30 days uses daily aggregation


@dataclass
class AppConfig:
    """Main application configuration."""
    storage_mode: StorageMode
    azure: AzureConfig
    local: LocalConfig
    duckdb: DuckDBConfig
    tiers: TierConfig
    
    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    
    # Query limits
    max_query_duration_hours: int = 168  # 7 days
    default_max_datapoints: int = 10000


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    
    storage_mode = StorageMode(os.getenv("STORAGE_MODE", "local"))
    
    azure_config = AzureConfig(
        account_name=os.getenv("AZURE_STORAGE_ACCOUNT", ""),
        container_name=os.getenv("AZURE_CONTAINER_NAME", ""),
        sas_token=os.getenv("AZURE_SAS_TOKEN", ""),
        data_prefix=os.getenv("AZURE_DATA_PREFIX", "")
    )
    
    local_config = LocalConfig(
        data_path=os.getenv("LOCAL_STORAGE_PATH", "/data")
    )
    
    duckdb_config = DuckDBConfig(
        memory_limit=os.getenv("DUCKDB_MEMORY_LIMIT", "4GB"),
        threads=int(os.getenv("DUCKDB_THREADS", "4"))
    )
    
    tier_config = TierConfig(
        raw_threshold_minutes=int(os.getenv("TIER_RAW_THRESHOLD_MINUTES", "5")),
        minute_threshold_minutes=int(os.getenv("TIER_MINUTE_THRESHOLD_MINUTES", "60")),
        hourly_threshold_days=int(os.getenv("TIER_HOURLY_THRESHOLD_DAYS", "30"))
    )
    
    return AppConfig(
        storage_mode=storage_mode,
        azure=azure_config,
        local=local_config,
        duckdb=duckdb_config,
        tiers=tier_config,
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("API_PORT", "8082")),
        max_query_duration_hours=int(os.getenv("MAX_QUERY_DURATION_HOURS", "168")),
        default_max_datapoints=int(os.getenv("DEFAULT_MAX_DATAPOINTS", "10000"))
    )
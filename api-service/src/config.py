"""Configuration management for API service."""
import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration parameters for API service."""
    
    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str
    log_level: str
    api_key: str
    require_auth: bool
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            influxdb_url=os.getenv("INFLUXDB_URL", "http://influxdb:8086"),
            influxdb_token=os.getenv("INFLUXDB_TOKEN", ""),
            influxdb_org=os.getenv("INFLUXDB_ORG", "hsl"),
            influxdb_bucket=os.getenv("INFLUXDB_BUCKET", "transport_metrics"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            api_key=os.getenv("API_KEY", ""),
            require_auth=os.getenv("REQUIRE_AUTH", "true").lower() == "true",
        )
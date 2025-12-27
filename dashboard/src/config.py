"""Configuration for dashboard service."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration for dashboard service."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    api_url: str
    api_key: Optional[str]
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093"),
            kafka_topic=os.getenv("KAFKA_AGGREGATED_TOPIC", "hfp-aggregated"),
            api_url=os.getenv("API_URL", "http://api-service:8000"),
            api_key=os.getenv("API_KEY", None),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

"""Configuration management for MQTT ingestor."""
import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration parameters for MQTT ingestor."""
    
    mqtt_broker: str
    mqtt_port: int
    mqtt_topic: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    log_level: str
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            mqtt_broker=os.getenv("MQTT_BROKER", "mqtt.hsl.fi"),
            mqtt_port=int(os.getenv("MQTT_PORT", "8883")),
            mqtt_topic=os.getenv("MQTT_TOPIC", "/hfp/v2/journey/#"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "hfp-raw"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
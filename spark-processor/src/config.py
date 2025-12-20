"""Configuration management for Spark processor."""
import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration parameters for Spark processor."""
    
    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_output_topic: str
    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str
    spark_driver_memory: str
    spark_executor_memory: str
    log_level: str
    checkpoint_location: str = "/tmp/spark-checkpoints"
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093"),
            kafka_input_topic=os.getenv("KAFKA_INPUT_TOPIC", "hfp-raw"),
            kafka_output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "hfp-aggregated"),
            influxdb_url=os.getenv("INFLUXDB_URL", "http://influxdb:8086"),
            influxdb_token=os.getenv("INFLUXDB_TOKEN", ""),
            influxdb_org=os.getenv("INFLUXDB_ORG", "hsl"),
            influxdb_bucket=os.getenv("INFLUXDB_BUCKET", "transport_metrics"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            spark_executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

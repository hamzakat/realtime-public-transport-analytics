"""Pytest fixtures for MQTT ingestor tests."""
import pytest
from unittest.mock import Mock, MagicMock
from src.config import Config


@pytest.fixture
def mock_config():
    """Provide a mock configuration."""
    return Config(
        mqtt_broker="test.mqtt.broker",
        mqtt_port=1883,
        mqtt_topic="/test/topic/#",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test-topic",
        log_level="DEBUG"
    )


@pytest.fixture
def mock_kafka_producer():
    """Provide a mock Kafka producer compatible with confluent_kafka.Producer."""
    producer = MagicMock()
    # We use produce(...) in the implementation; no need to configure return value
    producer.produce = MagicMock()
    producer.flush = MagicMock()
    return producer


@pytest.fixture
def mock_mqtt_client():
    """Provide a mock MQTT client."""
    client = MagicMock()
    return client
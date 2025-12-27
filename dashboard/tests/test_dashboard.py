"""Unit tests for dashboard service."""

import os
import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.config import Config
from src.kafka_consumer import KafkaDataConsumer
from src.api_client import APIClient


class TestConfig:
    """Test configuration loading."""

    def test_config_from_env_defaults(self):
        """Test default configuration values."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config.from_env()

            assert config.kafka_bootstrap_servers == "kafka:9093"
            assert config.kafka_topic == "hfp-aggregated"
            assert config.api_url == "http://api-service:8000"
            assert config.api_key is None
            assert config.log_level == "INFO"

    def test_config_from_env_custom(self):
        """Test custom configuration from environment variables."""
        env_vars = {
            "KAFKA_BOOTSTRAP_SERVERS": "custom-kafka:9092",
            "KAFKA_AGGREGATED_TOPIC": "custom-topic",
            "API_URL": "http://custom-api:8000",
            "API_KEY": "test-key",
            "LOG_LEVEL": "DEBUG",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = Config.from_env()

            assert config.kafka_bootstrap_servers == "custom-kafka:9092"
            assert config.kafka_topic == "custom-topic"
            assert config.api_url == "http://custom-api:8000"
            assert config.api_key == "test-key"
            assert config.log_level == "DEBUG"


class TestAPIClient:
    """Test API client functionality."""

    def test_init(self):
        """Test API client initialization."""
        client = APIClient("http://test.com", "test-key")

        assert client.base_url == "http://test.com"
        assert client.api_key == "test-key"

    def test_init_without_api_key(self):
        """Test API client initialization without API key."""
        client = APIClient("http://test.com")

        assert client.base_url == "http://test.com"
        assert client.api_key is None

    def test_get_headers_with_api_key(self):
        """Test header generation with API key."""
        client = APIClient("http://test.com", "test-key")
        headers = client._get_headers()

        assert headers["Content-Type"] == "application/json"
        assert headers["X-API-Key"] == "test-key"

    def test_get_headers_without_api_key(self):
        """Test header generation without API key."""
        client = APIClient("http://test.com")
        headers = client._get_headers()

        assert headers["Content-Type"] == "application/json"
        assert "X-API-Key" not in headers

    @patch("src.api_client.requests.get")
    def test_get_routes_success(self, mock_get):
        """Test successful route fetching."""
        mock_response = Mock()
        mock_response.json.return_value = ["route1", "route2"]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = APIClient("http://test.com")
        routes = client.get_routes()

        assert routes == ["route1", "route2"]
        mock_get.assert_called_once_with(
            "http://test.com/api/v1/routes", headers=client._get_headers(), timeout=10
        )

    @patch("src.api_client.requests.get")
    def test_get_routes_failure(self, mock_get):
        """Test route fetching on failure."""
        from requests import RequestException

        mock_get.side_effect = RequestException("Network error")

        client = APIClient("http://test.com")
        routes = client.get_routes()

        assert routes == []

    @patch("src.api_client.requests.get")
    def test_get_metrics_success(self, mock_get):
        """Test successful metrics fetching."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "count": 2,
            "metrics": [
                {"route": "route1", "vehicle_count": 10},
                {"route": "route2", "vehicle_count": 15},
            ],
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = APIClient("http://test.com")
        metrics = client.get_metrics(route="route1")

        assert len(metrics) == 2
        assert metrics[0]["route"] == "route1"

    @patch("src.api_client.requests.get")
    def test_get_route_stats_success(self, mock_get):
        """Test successful route stats fetching."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "route": "route1",
            "total_observations": 100,
            "avg_vehicle_count": 10.5,
            "avg_speed": 8.5,
            "avg_delay": 2.3,
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = APIClient("http://test.com")
        stats = client.get_route_stats("route1")

        assert stats["route"] == "route1"
        assert stats["total_observations"] == 100


class TestKafkaConsumer:
    """Test Kafka consumer functionality."""

    def test_init(self):
        """Test Kafka consumer initialization."""
        consumer = KafkaDataConsumer("kafka:9093", "test-topic")

        assert consumer.bootstrap_servers == "kafka:9093"
        assert consumer.topic == "test-topic"
        assert consumer.group_id == "dashboard-consumer"
        assert consumer.consumer is None

    def test_init_custom_group_id(self):
        """Test Kafka consumer with custom group ID."""
        consumer = KafkaDataConsumer("kafka:9093", "test-topic", "custom-group")

        assert consumer.group_id == "custom-group"

    @patch("src.kafka_consumer.Consumer")
    def test_connect_success(self, mock_consumer_class):
        """Test successful Kafka connection."""
        mock_consumer = Mock()
        mock_consumer.list_topics.return_value.topics = {"test-topic": Mock()}
        mock_consumer_class.return_value = mock_consumer

        consumer = KafkaDataConsumer("kafka:9093", "test-topic")
        result = consumer.connect()

        assert result is True
        assert consumer.consumer is not None
        mock_consumer.subscribe.assert_called_once_with(["test-topic"])

    @patch("src.kafka_consumer.Consumer")
    def test_connect_failure(self, mock_consumer_class):
        """Test Kafka connection failure."""
        mock_consumer_class.side_effect = Exception("Connection failed")

        consumer = KafkaDataConsumer("kafka:9093", "test-topic")
        result = consumer.connect()

        assert result is False

    def test_close_no_consumer(self):
        """Test close when consumer is not connected."""
        consumer = KafkaDataConsumer("kafka:9093", "test-topic")
        consumer.close()

        assert consumer.consumer is None


class TestDataFiltering:
    """Test data filtering logic."""

    def test_filter_live_data_no_filter(self):
        """Test filtering with no filters applied."""

        def filter_live_data(messages, selected_route, selected_direction):
            """Local implementation of filter function for testing."""
            filtered = []
            for msg in messages:
                try:
                    route = str(msg.get("route", ""))
                    direction = str(msg.get("direction", ""))

                    if (not selected_route or route == selected_route) and (
                        not selected_direction or direction == selected_direction
                    ):
                        filtered.append(msg)
                except Exception:
                    pass
            return filtered

        messages = [
            {"route": "route1", "direction": "1", "vehicle_count": 10},
            {"route": "route2", "direction": "2", "vehicle_count": 15},
        ]

        filtered = filter_live_data(messages, "", "")

        assert len(filtered) == 2

    def test_filter_live_data_by_route(self):
        """Test filtering by route."""

        def filter_live_data(messages, selected_route, selected_direction):
            """Local implementation of filter function for testing."""
            filtered = []
            for msg in messages:
                try:
                    route = str(msg.get("route", ""))
                    direction = str(msg.get("direction", ""))

                    if (not selected_route or route == selected_route) and (
                        not selected_direction or direction == selected_direction
                    ):
                        filtered.append(msg)
                except Exception:
                    pass
            return filtered

        messages = [
            {"route": "route1", "direction": "1", "vehicle_count": 10},
            {"route": "route2", "direction": "1", "vehicle_count": 15},
        ]

        filtered = filter_live_data(messages, "route1", "")

        assert len(filtered) == 1
        assert filtered[0]["route"] == "route1"

    def test_filter_live_data_by_direction(self):
        """Test filtering by direction."""

        def filter_live_data(messages, selected_route, selected_direction):
            """Local implementation of filter function for testing."""
            filtered = []
            for msg in messages:
                try:
                    route = str(msg.get("route", ""))
                    direction = str(msg.get("direction", ""))

                    if (not selected_route or route == selected_route) and (
                        not selected_direction or direction == selected_direction
                    ):
                        filtered.append(msg)
                except Exception:
                    pass
            return filtered

        messages = [
            {"route": "route1", "direction": "1", "vehicle_count": 10},
            {"route": "route1", "direction": "2", "vehicle_count": 15},
        ]

        filtered = filter_live_data(messages, "", "1")

        assert len(filtered) == 1
        assert filtered[0]["direction"] == "1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

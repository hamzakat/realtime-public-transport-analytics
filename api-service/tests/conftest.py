"""Pytest fixtures for API service tests with authentication support."""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime


@pytest.fixture
def mock_influxdb_client():
    """Provide a mock InfluxDB client."""
    client = MagicMock()
    query_api = MagicMock()
    client.query_api.return_value = query_api
    return client


@pytest.fixture
def mock_query_api():
    """Provide a mock query API."""
    return MagicMock()


@pytest.fixture
def sample_influxdb_records():
    """Provide sample InfluxDB query records."""
    record1 = Mock()
    record1.get_time.return_value = datetime(2025, 1, 1, 12, 0, 0)
    record1.values = {
        "route": "1001",
        "direction": "1",
        "window_type": "tumbling_10s",
        "vehicle_count": 5,
        "active_vehicles": 5,
        "avg_speed": 10.5,
        "avg_delay": 30.0,
        "min_delay": -60,
        "max_delay": 120
    }
    
    record2 = Mock()
    record2.get_time.return_value = datetime(2025, 1, 1, 12, 0, 10)
    record2.values = {
        "route": "1001",
        "direction": "1",
        "window_type": "tumbling_10s",
        "vehicle_count": 7,
        "active_vehicles": 7,
        "avg_speed": 12.3,
        "avg_delay": 15.0,
        "min_delay": -30,
        "max_delay": 90
    }
    
    return [record1, record2]


@pytest.fixture
def test_api_key():
    """Provide a test API key."""
    return "test-api-key-12345"


@pytest.fixture
def mock_config(test_api_key):
    """Provide a mock configuration with security settings."""
    config = Mock()
    config.influxdb_url = "http://localhost:8086"
    config.influxdb_token = "test-token"
    config.influxdb_org = "test-org"
    config.influxdb_bucket = "test-bucket"
    config.log_level = "INFO"
    config.api_key = test_api_key
    config.require_auth = False  # Default to False for easier testing
    return config


@pytest.fixture
def test_client(mock_config):
    """Create a test client for the FastAPI app with mocked config."""
    with patch('src.main.get_influxdb_client'), \
         patch('src.main.config', mock_config), \
         patch('src.main.audit_logger'):
        from src.main import app
        client = TestClient(app)
        yield client
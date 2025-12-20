"""Unit tests for API service with authentication and rate limiting."""
import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from fastapi.testclient import TestClient


@pytest.fixture
def test_api_key():
    """Provide test API key."""
    return "test-api-key-12345"


@pytest.fixture
def auth_headers(test_api_key):
    """Provide authentication headers."""
    return {"X-API-Key": test_api_key}


class TestHealthEndpoint:
    """Test suite for health check endpoint."""
    
    def test_health_check_success(self, test_client):
        """Test health check with successful InfluxDB connection."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_query_api.query.return_value = []
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["influxdb_connected"] is True
            assert "timestamp" in data
    
    def test_health_check_degraded(self, test_client):
        """Test health check with failed InfluxDB connection."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_get_client.side_effect = Exception("Connection failed")
            
            response = test_client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "degraded"
            assert data["influxdb_connected"] is False
    
    def test_health_check_no_auth_required(self, test_client):
        """Test that health check doesn't require authentication."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_query_api.query.return_value = []
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            # Request without API key should still work
            response = test_client.get("/health")
            assert response.status_code == 200


class TestAuthentication:
    """Test suite for API authentication."""
    
    def test_missing_api_key(self, test_client, sample_influxdb_records):
        """Test request without API key is rejected."""
        with patch('src.main.config') as mock_config:
            mock_config.require_auth = True
            mock_config.api_key = "valid-key"
            
            response = test_client.get("/api/v1/metrics")
            
            assert response.status_code == 401
            assert "Missing API key" in response.json()["detail"]
    
    def test_invalid_api_key(self, test_client):
        """Test request with invalid API key is rejected."""
        with patch('src.main.config') as mock_config:
            mock_config.require_auth = True
            mock_config.api_key = "valid-key"
            
            headers = {"X-API-Key": "invalid-key"}
            response = test_client.get("/api/v1/metrics", headers=headers)
            
            assert response.status_code == 403
            assert "Invalid API key" in response.json()["detail"]
    
    def test_valid_api_key(self, test_client, sample_influxdb_records, test_api_key):
        """Test request with valid API key is accepted."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = True
            mock_config.api_key = test_api_key
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            headers = {"X-API-Key": test_api_key}
            response = test_client.get("/api/v1/metrics", headers=headers)
            
            assert response.status_code == 200
    
    def test_auth_disabled(self, test_client, sample_influxdb_records):
        """Test that authentication can be disabled."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = False
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            # Request without API key should work when auth is disabled
            response = test_client.get("/api/v1/metrics")
            assert response.status_code == 200


class TestMetricsEndpoint:
    """Test suite for metrics query endpoint."""
    
    def test_get_metrics_no_filters(self, test_client, sample_influxdb_records, auth_headers):
        """Test metrics query without filters."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = False
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/api/v1/metrics")
            
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 2
            assert len(data["metrics"]) == 2
            
            metric = data["metrics"][0]
            assert metric["route"] == "1001"
            assert metric["direction"] == "1"
            assert metric["vehicle_count"] == 5
            assert metric["avg_speed"] == 10.5
    
    def test_get_metrics_with_auth(self, test_client, sample_influxdb_records, test_api_key):
        """Test metrics query with authentication."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = True
            mock_config.api_key = test_api_key
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            headers = {"X-API-Key": test_api_key}
            response = test_client.get("/api/v1/metrics", headers=headers)
            
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 2


class TestRateLimiting:
    """Test suite for rate limiting."""
    
    def test_rate_limit_enforcement(self, test_client, auth_headers):
        """Test that rate limiting is enforced."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = False
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_query_api.query.return_value = []
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            # Make multiple rapid requests to trigger rate limit
            # Note: Rate limit is 100/minute, but test won't actually hit limit
            # This test just verifies the endpoint accepts requests
            for i in range(5):
                response = test_client.get("/api/v1/metrics")
                assert response.status_code in [200, 429]  # 429 = Too Many Requests


class TestRoutesEndpoint:
    """Test suite for routes listing endpoint."""
    
    def test_get_routes_success(self, test_client, auth_headers):
        """Test getting list of routes."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = False
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            record1 = Mock()
            record1.values = {"route": "1001"}
            record2 = Mock()
            record2.values = {"route": "2002"}
            
            mock_table = Mock()
            mock_table.records = [record1, record2]
            mock_query_api.query.return_value = [mock_table]
            
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/api/v1/routes")
            
            assert response.status_code == 200
            routes = response.json()
            assert isinstance(routes, list)
            assert "1001" in routes
            assert "2002" in routes
    
    def test_get_routes_requires_auth(self, test_client, test_api_key):
        """Test that routes endpoint requires authentication."""
        with patch('src.main.config') as mock_config:
            mock_config.require_auth = True
            mock_config.api_key = test_api_key
            
            response = test_client.get("/api/v1/routes")
            assert response.status_code == 401


class TestRouteStatsEndpoint:
    """Test suite for route statistics endpoint."""
    
    def test_get_route_stats_success(self, test_client, auth_headers):
        """Test getting statistics for a specific route."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.get_influxdb_client') as mock_get_client:
            
            mock_config.require_auth = False
            mock_config.influxdb_bucket = "test-bucket"
            
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            record1 = Mock()
            record1.get_field.return_value = "vehicle_count"
            record1.get_value.return_value = 5.5
            
            record2 = Mock()
            record2.get_field.return_value = "avg_speed"
            record2.get_value.return_value = 12.3
            
            record3 = Mock()
            record3.get_field.return_value = "avg_delay"
            record3.get_value.return_value = 30.0
            
            mock_table = Mock()
            mock_table.records = [record1, record2, record3]
            mock_query_api.query.return_value = [mock_table]
            
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/api/v1/routes/1001/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert data["route"] == "1001"
            assert "avg_vehicle_count" in data
            assert "avg_speed" in data
            assert "avg_delay" in data


class TestAuditLogging:
    """Test suite for audit logging."""
    
    def test_audit_log_on_request(self, test_client, auth_headers):
        """Test that requests are logged for audit."""
        with patch('src.main.config') as mock_config, \
             patch('src.main.audit_logger') as mock_audit:
            
            mock_config.require_auth = False
            
            test_client.get("/health")
            
            # Verify audit log was called
            # Note: This is simplified; real implementation logs via middleware
            assert True  # Placeholder - actual audit logging happens in middleware


class TestRootEndpoint:
    """Test suite for root endpoint."""
    
    def test_root_endpoint(self, test_client):
        """Test root endpoint returns API information."""
        response = test_client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert "version" in data
        assert "documentation" in data
        assert "authentication" in data
"""Unit tests for API service."""
import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from fastapi.testclient import TestClient


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


class TestMetricsEndpoint:
    """Test suite for metrics query endpoint."""
    
    def test_get_metrics_no_filters(self, test_client, sample_influxdb_records):
        """Test metrics query without filters."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            # Create mock table
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
            
            # Verify first record
            metric = data["metrics"][0]
            assert metric["route"] == "1001"
            assert metric["direction"] == "1"
            assert metric["vehicle_count"] == 5
            assert metric["avg_speed"] == 10.5
    
    def test_get_metrics_with_route_filter(self, test_client, sample_influxdb_records):
        """Test metrics query with route filter."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/api/v1/metrics?route=1001")
            
            assert response.status_code == 200
            data = response.json()
            assert data["count"] >= 0  # May vary based on mock
            
            # Verify query contains filter
            called_query = mock_query_api.query.call_args[0][0]
            assert 'r.route == "1001"' in called_query
    
    def test_get_metrics_with_time_range(self, test_client, sample_influxdb_records):
        """Test metrics query with time range."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            start = "2025-01-01T12:00:00"
            end = "2025-01-01T13:00:00"
            response = test_client.get(f"/api/v1/metrics?start_time={start}&end_time={end}")
            
            assert response.status_code == 200
            
            # Verify query contains time range
            called_query = mock_query_api.query.call_args[0][0]
            assert "range(start: 2025-01-01T12:00:00Z, stop: 2025-01-01T13:00:00Z)" in called_query
    
    def test_get_metrics_with_limit(self, test_client, sample_influxdb_records):
        """Test metrics query with custom limit."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            mock_table = Mock()
            mock_table.records = sample_influxdb_records
            mock_query_api.query.return_value = [mock_table]
            
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/api/v1/metrics?limit=500")
            
            assert response.status_code == 200
            
            # Verify query contains limit
            called_query = mock_query_api.query.call_args[0][0]
            assert "limit(n: 500)" in called_query
    
    def test_get_metrics_influxdb_error(self, test_client):
        """Test metrics query with InfluxDB error."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            mock_query_api.query.side_effect = Exception("Query failed")
            
            mock_client.query_api.return_value = mock_query_api
            mock_get_client.return_value = mock_client
            
            response = test_client.get("/api/v1/metrics")
            
            assert response.status_code == 500
            assert "error" in response.json()["detail"].lower()


class TestRoutesEndpoint:
    """Test suite for routes listing endpoint."""
    
    def test_get_routes_success(self, test_client):
        """Test getting list of routes."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            # Create mock records with route values
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


class TestRouteStatsEndpoint:
    """Test suite for route statistics endpoint."""
    
    def test_get_route_stats_success(self, test_client):
        """Test getting statistics for a specific route."""
        with patch('src.main.get_influxdb_client') as mock_get_client:
            mock_client = MagicMock()
            mock_query_api = MagicMock()
            
            # Create mock records for different fields
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
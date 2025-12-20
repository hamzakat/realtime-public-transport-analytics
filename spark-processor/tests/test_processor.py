"""Unit tests for Spark processor."""
import pytest
from datetime import datetime
from pyspark.sql import Row
from src.processor import HFPStreamProcessor
from src.schemas import RAW_MESSAGE_SCHEMA


class TestHFPStreamProcessor:
    """Test suite for HFPStreamProcessor class."""
    
    def test_initialization(self, mock_config, spark_session, monkeypatch):
        """Test processor initialization without expensive Spark startup."""

        def _fake_create_spark_session(self):  # pragma: no cover - trivial shim
            return spark_session

        monkeypatch.setattr("src.processor.HFPStreamProcessor._create_spark_session", _fake_create_spark_session)

        processor = HFPStreamProcessor(mock_config)
        
        assert processor.config == mock_config
        assert processor.spark is not None
        assert processor.influxdb_client is None
    
    def test_parse_messages(self, spark_session, mock_config, sample_kafka_messages):
        """Test message parsing logic."""
        processor = HFPStreamProcessor(mock_config)
        processor.spark = spark_session
        
        # Create test DataFrame
        test_data = [(msg,) for msg in sample_kafka_messages]
        df = spark_session.createDataFrame(test_data, ["value"])
        
        # Parse messages
        parsed_df = processor._parse_messages(df)
        
        # Collect results
        results = parsed_df.collect()
        
        # Assertions
        assert len(results) == 3
        
        # Check first message
        assert results[0]["route"] == "1001"
        assert results[0]["direction"] == "1"
        assert results[0]["vehicle_id"] == 123
        assert results[0]["speed"] == 10.5
        assert results[0]["delay"] == -30
        
        # Check second message
        assert results[1]["route"] == "1001"
        assert results[1]["direction"] == "1"
        assert results[1]["vehicle_id"] == 456
        
        # Check third message
        assert results[2]["route"] == "2002"
        assert results[2]["direction"] == "2"
        assert results[2]["vehicle_id"] == 789
    
    def test_parse_messages_filters_invalid(self, spark_session, mock_config):
        """Test that invalid messages are filtered out."""
        import json
        
        processor = HFPStreamProcessor(mock_config)
        processor.spark = spark_session
        
        # Create messages with missing required fields
        invalid_messages = [
            json.dumps({
                "mqtt_topic": "/test",
                "received_at": 1704110400.0,
                "payload": {"VP": {"veh": 123}}  # Missing route and direction
            }),
            json.dumps({
                "mqtt_topic": "/test",
                "received_at": 1704110400.0,
                "payload": {"VP": {"desi": "1001"}}  # Missing direction
            })
        ]
        
        test_data = [(msg,) for msg in invalid_messages]
        df = spark_session.createDataFrame(test_data, ["value"])
        
        parsed_df = processor._parse_messages(df)
        results = parsed_df.collect()
        
        # Should filter out invalid messages
        assert len(results) == 0
    
    def test_compute_tumbling_window_static(self, spark_session, mock_config):
        """Test tumbling window computation with static data."""
        processor = HFPStreamProcessor(mock_config)
        processor.spark = spark_session
        
        # Create test data with timestamps
        test_data = [
            Row(event_time=datetime(2025, 1, 1, 12, 0, 0), route="1001", direction="1", 
                vehicle_id=123, speed=10.0, delay=-30, latitude=60.2, longitude=24.9, operator=12),
            Row(event_time=datetime(2025, 1, 1, 12, 0, 5), route="1001", direction="1",
                vehicle_id=456, speed=12.0, delay=45, latitude=60.2, longitude=24.9, operator=12),
            Row(event_time=datetime(2025, 1, 1, 12, 0, 15), route="1001", direction="1",
                vehicle_id=789, speed=11.0, delay=0, latitude=60.2, longitude=24.9, operator=12),
        ]
        
        df = spark_session.createDataFrame(test_data)
        
        # For testing, we'll use a simple groupBy instead of watermarking
        # (which requires streaming context)
        from pyspark.sql.functions import window, count, avg, min as spark_min, max as spark_max
        
        agg_df = df.groupBy(
            window(df.event_time, "10 seconds"),
            df.route,
            df.direction
        ).agg(
            count("vehicle_id").alias("vehicle_count"),
            avg("speed").alias("avg_speed"),
            avg("delay").alias("avg_delay"),
            spark_min("delay").alias("min_delay"),
            spark_max("delay").alias("max_delay")
        )
        
        results = agg_df.collect()
        
        # Should have 2 windows: 12:00:00-12:00:10 and 12:00:10-12:00:20
        assert len(results) == 2
        
        # Check first window (contains first two messages)
        first_window = [r for r in results if r["window"]["start"] == datetime(2025, 1, 1, 12, 0, 0)][0]
        assert first_window["vehicle_count"] == 2
        assert first_window["avg_speed"] == pytest.approx(11.0)
        assert first_window["min_delay"] == -30
        assert first_window["max_delay"] == 45


class TestConfig:
    """Test suite for Config class."""
    
    def test_config_from_env_defaults(self):
        """Test configuration loading with default values."""
        from unittest.mock import patch
        
        with patch.dict('os.environ', {}, clear=True):
            from src.config import Config
            config = Config.from_env()
            
            assert config.kafka_bootstrap_servers == "kafka:9093"
            assert config.kafka_input_topic == "hfp-raw"
            assert config.influxdb_org == "hsl"
    
    def test_config_from_env_custom(self):
        """Test configuration loading with custom values."""
        from unittest.mock import patch
        
        env_vars = {
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
            'KAFKA_INPUT_TOPIC': 'custom-input',
            'KAFKA_OUTPUT_TOPIC': 'custom-output',
            'INFLUXDB_URL': 'http://custom:8086',
            'INFLUXDB_TOKEN': 'custom-token',
            'INFLUXDB_ORG': 'custom-org',
            'INFLUXDB_BUCKET': 'custom-bucket',
            'SPARK_DRIVER_MEMORY': '4g',
            'LOG_LEVEL': 'DEBUG'
        }
        
        with patch.dict('os.environ', env_vars):
            from src.config import Config
            config = Config.from_env()
            
            assert config.kafka_bootstrap_servers == 'kafka:9092'
            assert config.kafka_input_topic == 'custom-input'
            assert config.spark_driver_memory == '4g'
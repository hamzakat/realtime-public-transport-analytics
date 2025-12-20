"""Pytest fixtures for Spark processor tests."""
import pytest
from pyspark.sql import SparkSession
from src.config import Config


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("TestHFPProcessor") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def mock_config():
    """Provide a mock configuration."""
    return Config(
        kafka_bootstrap_servers="localhost:9092",
        kafka_input_topic="test-input",
        kafka_output_topic="test-output",
        influxdb_url="http://localhost:8086",
        influxdb_token="test-token",
        influxdb_org="test-org",
        influxdb_bucket="test-bucket",
        spark_driver_memory="1g",
        spark_executor_memory="1g",
        log_level="ERROR",
        checkpoint_location="/tmp/test-checkpoints"
    )


@pytest.fixture
def sample_kafka_messages():
    """Provide sample Kafka messages for testing."""
    import json
    
    messages = [
        {
            "mqtt_topic": "/hfp/v2/journey/ongoing/vp/bus/0012/01234/1001/1/Malmi/12:00/1234567/1/60;24/18/65/16/veh/123/2025-01-01T12:00:00.000Z",
            "received_at": 1704110400.0,
            "payload": {
                "VP": {
                    "desi": "1001",
                    "dir": "1",
                    "veh": 123,
                    "tst": "2025-01-01T12:00:00.000Z",
                    "spd": 10.5,
                    "dl": -30,
                    "lat": 60.2,
                    "long": 24.9,
                    "oper": 12
                }
            }
        },
        {
            "mqtt_topic": "/hfp/v2/journey/ongoing/vp/bus/0012/01234/1001/1/Malmi/12:00/1234567/1/60;24/18/65/16/veh/456/2025-01-01T12:00:05.000Z",
            "received_at": 1704110405.0,
            "payload": {
                "VP": {
                    "desi": "1001",
                    "dir": "1",
                    "veh": 456,
                    "tst": "2025-01-01T12:00:05.000Z",
                    "spd": 12.3,
                    "dl": 45,
                    "lat": 60.21,
                    "long": 24.91,
                    "oper": 12
                }
            }
        },
        {
            "mqtt_topic": "/hfp/v2/journey/ongoing/vp/bus/0012/01234/2002/2/Central/13:00/1234567/1/60;24/18/65/16/veh/789/2025-01-01T12:00:10.000Z",
            "received_at": 1704110410.0,
            "payload": {
                "VP": {
                    "desi": "2002",
                    "dir": "2",
                    "veh": 789,
                    "tst": "2025-01-01T12:00:10.000Z",
                    "spd": 8.7,
                    "dl": 0,
                    "lat": 60.17,
                    "long": 24.95,
                    "oper": 12
                }
            }
        }
    ]
    
    return [json.dumps(msg) for msg in messages]

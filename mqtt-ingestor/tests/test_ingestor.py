"""Unit tests for MQTT ingestor."""
import json
import pytest
from unittest.mock import Mock, MagicMock, patch
from src.ingestor import MQTTToKafkaIngestor
from src.config import Config


class TestMQTTToKafkaIngestor:
    """Test suite for MQTTToKafkaIngestor class."""
    
    def test_initialization(self, mock_config):
        """Test ingestor initialization."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        
        assert ingestor.config == mock_config
        assert ingestor.message_count == 0
        assert ingestor.error_count == 0
        assert ingestor.producer is None
        assert ingestor.mqtt_client is None
    
    def test_on_connect_success(self, mock_config):
        """Test successful MQTT connection callback."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        mock_client = MagicMock()
        
        ingestor._on_connect(mock_client, None, None, 0)
        
        mock_client.subscribe.assert_called_once_with(mock_config.mqtt_topic)
    
    def test_on_connect_failure(self, mock_config):
        """Test failed MQTT connection callback."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        mock_client = MagicMock()
        
        # Should not raise exception
        ingestor._on_connect(mock_client, None, None, 1)
        
        mock_client.subscribe.assert_not_called()
    
    def test_on_message_valid_json(self, mock_config, mock_kafka_producer):
        """Test message processing with valid JSON."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        ingestor.producer = mock_kafka_producer
        
        # Create mock MQTT message
        mock_msg = Mock()
        mock_msg.topic = "/hfp/v2/journey/ongoing/vp/bus/0012/01234/1001/1/Malmi/12:00/1234567/1/60;24/18/65/16/veh/123/2025-01-01T12:00:00.000Z"
        mock_msg.payload = json.dumps({"VP": {"desi": "1001", "dir": "1"}}).encode('utf-8')
        
        ingestor._on_message(None, None, mock_msg)
        
        assert ingestor.message_count == 1
        assert ingestor.error_count == 0
        mock_kafka_producer.produce.assert_called_once()
        
        # Verify Kafka message structure
        call = mock_kafka_producer.produce.call_args
        args, kwargs = call
        # First positional arg is topic for confluent_kafka.Producer.produce
        assert args[0] == mock_config.kafka_topic
        assert kwargs["key"] == "123"

        # Value is a JSON string; decode and inspect
        message_json = call.kwargs["value"]
        message = json.loads(message_json)
        assert message["mqtt_topic"] == mock_msg.topic
        assert "payload" in message
        assert "received_at" in message
    
    def test_on_message_invalid_json(self, mock_config, mock_kafka_producer):
        """Test message processing with invalid JSON."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        ingestor.producer = mock_kafka_producer
        
        # Create mock MQTT message with invalid JSON
        mock_msg = Mock()
        mock_msg.topic = "/test/topic"
        mock_msg.payload = b"invalid json {"
        
        ingestor._on_message(None, None, mock_msg)
        
        assert ingestor.message_count == 0
        assert ingestor.error_count == 1
        mock_kafka_producer.produce.assert_not_called()
    
    def test_on_message_vehicle_id_extraction(self, mock_config, mock_kafka_producer):
        """Test vehicle ID extraction from MQTT topic."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        ingestor.producer = mock_kafka_producer
        
        test_cases = [
            # (topic, expected_vehicle_id)
            ("/hfp/v2/journey/ongoing/vp/bus/0012/01234/1001/1/Malmi/12:00/1234567/1/60;24/18/65/16/veh/456/2025-01-01T12:00:00.000Z", "456"),
            ("/hfp/v2/journey/ongoing/vp/tram/0050/00201/6/1/Arabia/10:30/1234567/2/60;24/18/65/16/veh/789/2025-01-01T10:30:00.000Z", "789"),
            ("/hfp/v2/journey/ongoing/vp/bus/0012/01234/1001/1/Malmi/12:00/1234567/1/60;24/18/65/16", None),  # No vehicle ID
        ]
        
        for topic, expected_veh_id in test_cases:
            mock_msg = Mock()
            mock_msg.topic = topic
            mock_msg.payload = json.dumps({"VP": {}}).encode('utf-8')
            
            ingestor._on_message(None, None, mock_msg)
            
            _, kwargs = mock_kafka_producer.produce.call_args
            assert kwargs["key"] == expected_veh_id
    
    def test_shutdown(self, mock_config):
        """Test graceful shutdown."""
        ingestor = MQTTToKafkaIngestor(mock_config)
        ingestor.producer = MagicMock()
        ingestor.mqtt_client = MagicMock()
        ingestor.message_count = 100
        ingestor.error_count = 5
        
        ingestor.shutdown()
        
        ingestor.mqtt_client.disconnect.assert_called_once()
        ingestor.mqtt_client.loop_stop.assert_called_once()
        ingestor.producer.flush.assert_called_once()
        # We don't explicitly close the confluent_kafka producer; flush is sufficient


class TestConfig:
    """Test suite for Config class."""
    
    def test_config_from_env_defaults(self):
        """Test configuration loading with default values."""
        with patch.dict('os.environ', {}, clear=True):
            config = Config.from_env()
            
            assert config.mqtt_broker == "mqtt.hsl.fi"
            assert config.mqtt_port == 8883
            assert config.kafka_bootstrap_servers == "kafka:9093"
    
    def test_config_from_env_custom(self):
        """Test configuration loading with custom values."""
        env_vars = {
            'MQTT_BROKER': 'custom.broker',
            'MQTT_PORT': '1883',
            'MQTT_TOPIC': '/custom/topic/#',
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
            'KAFKA_TOPIC': 'custom-topic',
            'LOG_LEVEL': 'DEBUG'
        }
        
        with patch.dict('os.environ', env_vars):
            config = Config.from_env()
            
            assert config.mqtt_broker == 'custom.broker'
            assert config.mqtt_port == 1883
            assert config.mqtt_topic == '/custom/topic/#'
            assert config.kafka_bootstrap_servers == 'kafka:9092'
            assert config.kafka_topic == 'custom-topic'
            assert config.log_level == 'DEBUG'
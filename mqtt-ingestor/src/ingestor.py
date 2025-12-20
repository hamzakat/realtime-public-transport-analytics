"""MQTT to Kafka ingestion service with structured audit logging."""
import json
import logging
import ssl
import sys
import time
from typing import Optional
from datetime import datetime, timezone

import paho.mqtt.client as mqtt_client
from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

from .config import Config


class StructuredLogger:
    """Structured JSON logger for audit trails."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = logging.getLogger(f"{service_name}.audit")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_event(self, event_type: str, **kwargs):
        """Log structured event."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name,
            "event_type": event_type,
            **kwargs
        }
        self.logger.info(json.dumps(log_entry))


class MQTTToKafkaIngestor:
    """Ingests MQTT messages and produces to Kafka with audit logging."""
    
    def __init__(self, config: Config):
        """Initialize ingestor with configuration."""
        self.config = config
        self.logger = self._setup_logging()
        self.audit_logger = StructuredLogger("mqtt-ingestor")
        self.producer: Optional[Producer] = None
        self.mqtt_client: Optional[mqtt_client.Client] = None
        self.message_count = 0
        self.error_count = 0
        self.last_stats_time = time.time()
        self.stats_interval = 60  # Log stats every 60 seconds
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging."""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            stream=sys.stdout
        )
        return logging.getLogger(__name__)
    
    def _setup_kafka_producer(self) -> Producer:
        """Initialize Kafka producer with retry logic."""
        max_retries = 10
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                producer_config = {
                    'bootstrap.servers': self.config.kafka_bootstrap_servers,
                    'acks': 'all',
                    'retries': 3,
                    'max.in.flight.requests.per.connection': 1,
                }
                producer = Producer(producer_config)
                
                self.audit_logger.log_event(
                    "kafka_connected",
                    bootstrap_servers=self.config.kafka_bootstrap_servers,
                    topic=self.config.kafka_topic
                )
                
                self.logger.info("Successfully connected to Kafka")
                return producer
            except KafkaException as e:
                self.logger.warning(
                    f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    self.audit_logger.log_event(
                        "kafka_connection_failed",
                        error=str(e),
                        attempts=max_retries
                    )
                    raise
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for MQTT connection."""
        if rc == 0:
            self.audit_logger.log_event(
                "mqtt_connected",
                broker=self.config.mqtt_broker,
                port=self.config.mqtt_port
            )
            
            client.subscribe(self.config.mqtt_topic)
            
            self.audit_logger.log_event(
                "mqtt_subscribed",
                topic=self.config.mqtt_topic
            )
            
            self.logger.info("Connected to MQTT broker")
            self.logger.info(f"Subscribed to topic: {self.config.mqtt_topic}")
        else:
            self.audit_logger.log_event(
                "mqtt_connection_failed",
                return_code=rc
            )
            self.logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
    
    def _log_stats(self):
        """Log periodic statistics."""
        current_time = time.time()
        if current_time - self.last_stats_time >= self.stats_interval:
            self.audit_logger.log_event(
                "ingestion_stats",
                messages_processed=self.message_count,
                errors=self.error_count,
                success_rate=round((self.message_count / (self.message_count + self.error_count) * 100), 2) 
                    if (self.message_count + self.error_count) > 0 else 100.0
            )
            self.last_stats_time = current_time
    
    def _on_message(self, client, userdata, msg):
        """Callback for MQTT message reception."""
        try:
            # Parse MQTT message
            topic = msg.topic
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)

            # Extract vehicle ID from topic for Kafka key (for partitioning)
            topic_parts = topic.split('/')
            vehicle_id = None
            if 'veh' in topic_parts:
                veh_idx = topic_parts.index('veh')
                if veh_idx + 1 < len(topic_parts):
                    vehicle_id = topic_parts[veh_idx + 1]

            # Enrich message with metadata
            message = {
                "mqtt_topic": topic,
                "payload": data,
                "received_at": time.time()
            }

            # Serialize the message
            message_json = json.dumps(message, ensure_ascii=False)
            key_str = vehicle_id if vehicle_id else None

            # Send to Kafka using confluent_kafka
            self.producer.produce(
                self.config.kafka_topic,
                value=message_json,
                key=key_str,
                callback=self._delivery_callback
            )

            # Poll for any delivery reports
            self.producer.poll(0)

            self.message_count += 1
            
            # Log stats periodically
            self._log_stats()
            
            if self.message_count % 100 == 0:
                self.logger.info(f"Processed {self.message_count} messages")

        except json.JSONDecodeError as e:
            self.error_count += 1
            self.audit_logger.log_event(
                "message_parse_error",
                error_type="json_decode",
                error=str(e),
                topic=msg.topic
            )
            self.logger.error(f"JSON decode error: {e}")
        except KafkaException as e:
            self.error_count += 1
            self.audit_logger.log_event(
                "kafka_produce_error",
                error=str(e)
            )
            self.logger.error(f"Kafka send error: {e}")
        except Exception as e:
            self.error_count += 1
            self.audit_logger.log_event(
                "unexpected_error",
                error=str(e)
            )
            self.logger.error(f"Unexpected error: {e}", exc_info=True)

    def _delivery_callback(self, err, msg):
        """Callback for delivery reports from Kafka producer."""
        if err is not None:
            self.error_count += 1
            self.audit_logger.log_event(
                "message_delivery_failed",
                error=str(err)
            )
            self.logger.error(f"Message delivery failed: {err}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection."""
        if rc != 0:
            self.audit_logger.log_event(
                "mqtt_disconnected",
                return_code=rc,
                expected=False
            )
            self.logger.warning(f"Unexpected MQTT disconnection, code: {rc}")
    
    def run(self):
        """Start the ingestion service."""
        try:
            self.audit_logger.log_event("service_startup")
            
            # Initialize Kafka producer
            self.logger.info("Initializing Kafka producer...")
            self.producer = self._setup_kafka_producer()
            
            # Initialize MQTT client
            self.logger.info("Initializing MQTT client...")
            self.mqtt_client = mqtt_client.Client()
            self.mqtt_client.tls_set(
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS
            )
            self.mqtt_client.on_connect = self._on_connect
            self.mqtt_client.on_message = self._on_message
            self.mqtt_client.on_disconnect = self._on_disconnect
            
            # Connect to MQTT broker
            self.logger.info(f"Connecting to MQTT broker: {self.config.mqtt_broker}:{self.config.mqtt_port}")
            self.mqtt_client.connect(
                self.config.mqtt_broker,
                self.config.mqtt_port,
                keepalive=60
            )
            
            # Start MQTT loop
            self.logger.info("Starting MQTT message loop...")
            self.mqtt_client.loop_forever()
            
        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully...")
            self.shutdown()
        except Exception as e:
            self.audit_logger.log_event(
                "fatal_error",
                error=str(e)
            )
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            self.shutdown()
            sys.exit(1)
    
    def shutdown(self):
        """Clean up resources."""
        self.audit_logger.log_event(
            "service_shutdown",
            total_messages=self.message_count,
            total_errors=self.error_count,
            success_rate=round((self.message_count / (self.message_count + self.error_count) * 100), 2) 
                if (self.message_count + self.error_count) > 0 else 100.0
        )
        
        self.logger.info(f"Total messages processed: {self.message_count}")
        self.logger.info(f"Total errors: {self.error_count}")

        if self.mqtt_client:
            self.mqtt_client.disconnect()
            self.mqtt_client.loop_stop()

        if self.producer:
            self.producer.flush()

        self.logger.info("Shutdown complete")


def main():
    """Entry point for the ingestor service."""
    config = Config.from_env()
    ingestor = MQTTToKafkaIngestor(config)
    ingestor.run()


if __name__ == "__main__":
    main()
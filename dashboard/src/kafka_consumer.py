"""Kafka consumer for dashboard service."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.serialization import SerializationError


class KafkaDataConsumer:
    """Consumes aggregated metrics from Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str = None):
        """Initialize Kafka consumer."""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id or "dashboard-consumer"
        self.logger = logging.getLogger(__name__)
        self.consumer: Optional[Consumer] = None

    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer."""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }

        consumer = Consumer(config)
        consumer.subscribe([self.topic])
        return consumer

    def connect(self) -> bool:
        """Connect to Kafka broker."""
        try:
            self.consumer = self._create_consumer()

            # Test connection by listing topics
            cluster_metadata = self.consumer.list_topics(timeout=10)

            if self.topic in cluster_metadata.topics:
                self.logger.info(
                    f"Successfully connected to Kafka and subscribed to topic: {self.topic}"
                )
                return True
            else:
                self.logger.warning(
                    f"Topic {self.topic} not found, will be created automatically"
                )
                return True

        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def consume_messages(self, timeout: float = 1.0) -> List[Dict]:
        """Consume messages from Kafka topic."""
        if not self.consumer:
            self.logger.warning("Consumer not connected, attempting to connect...")
            if not self.connect():
                return []

        messages = []
        try:
            raw_messages = self.consumer.consume(num_messages=100, timeout=timeout)

            for msg in raw_messages:
                if msg is None:
                    continue

                if msg.error():
                    self.logger.warning(f"Kafka message error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    messages.append(data)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    self.logger.warning(f"Failed to decode message: {e}")
                    continue

        except SerializationError as e:
            self.logger.error(f"Serialization error: {e}")
        except KafkaException as e:
            self.logger.error(f"Kafka error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error consuming messages: {e}")

        return messages

    def close(self):
        """Close consumer connection."""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer closed")

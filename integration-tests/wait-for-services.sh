#!/bin/bash
set -e

echo "Waiting for services to be ready..."

# Wait for Kafka
echo "Waiting for Kafka..."
timeout=180
counter=0

# Give services time to initialize
echo "Allowing services initial startup time..."
sleep 30

until python3 -c "
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import sys

try:
    print('Attempting to connect to Kafka at ${KAFKA_BOOTSTRAP_SERVERS}...')
    producer = KafkaProducer(
        bootstrap_servers='${KAFKA_BOOTSTRAP_SERVERS}',
        api_version=(2, 8, 0),
        request_timeout_ms=30000,
        max_block_ms=30000,
        retries=3
    )
    print('Successfully created producer')
    # Try to list topics to ensure we're really connected
    producer.close()
    print('Kafka connection successful!')
    sys.exit(0)
except NoBrokersAvailable as e:
    print(f'No brokers available yet: {e}')
    sys.exit(1)
except KafkaError as e:
    print(f'Kafka connection failed (KafkaError): {e}')
    sys.exit(1)
except Exception as e:
    print(f'Kafka connection failed (Exception): {e}')
    sys.exit(1)
"; do
    counter=$((counter + 1))
    if [ $counter -ge $timeout ]; then
        echo "ERROR: Kafka not ready after ${timeout} seconds"
        echo "Debugging information:"
        echo "1. Checking if Kafka port is reachable..."
        nc -zv kafka 9093 2>&1 || echo "   Port 9093 is NOT reachable"
        echo "2. Checking DNS resolution..."
        getent hosts kafka || echo "   Cannot resolve 'kafka' hostname"
        echo "3. Checking network connectivity..."
        ping -c 3 kafka || echo "   Cannot ping kafka"
        exit 1
    fi
    echo "Kafka not ready yet, waiting... ($counter/$timeout)"
    sleep 2
done
echo "Kafka is ready!"

# Wait for InfluxDB
echo "Waiting for InfluxDB..."
counter=0
until curl -f "${INFLUXDB_URL}/health" 2>/dev/null; do
    counter=$((counter + 1))
    if [ $counter -ge $timeout ]; then
        echo "ERROR: InfluxDB not ready after ${timeout} seconds"
        exit 1
    fi
    echo "InfluxDB not ready yet, waiting... ($counter/$timeout)"
    sleep 1
done
echo "InfluxDB is ready!"

echo "All services are ready!"

# Execute the command passed as arguments
exec "$@"
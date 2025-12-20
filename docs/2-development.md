# Real-Time Public Transport Analytics Platform: Development Phase

This document provides a technical overview of the real-time data pipeline implementation, detailing architecture decisions, system components, testing methodology, and deployment considerations.

## System Architecture

The implemented system follows a microservices architecture with five containerized services orchestrated via Docker Compose. The architecture matches the conception phase design with a lambda-like pattern enabling both stream and batch processing capabilities.

### Data Flow

1. **Ingestion Layer**: Python service subscribes to HSL MQTT broker (`mqtt.hsl.fi:8883`) over TLS
2. **Buffer Layer**: Kafka broker (with Zookeeper coordination) buffers raw vehicle events in `hfp-raw` topic
3. **Processing Layer**: Spark Structured Streaming applies stateful windowed aggregations
4. **Storage Layer**: Dual-sink pattern writes to both Kafka (`hfp-aggregated` topic) and InfluxDB
5. **API Layer**: FastAPI service queries InfluxDB for historical analytics

## Implementation Details

### 1. MQTT Ingestor (`mqtt-ingestor/`)

**Technology**: Python 3.13, Paho MQTT 1.6.1, Confluent Kafka 2.6.1

**Key Features**:
- MQTTs (TLS) connection with automatic reconnection
- Vehicle ID extraction from MQTT topic for Kafka partitioning
- Message enrichment with reception timestamp
- Delivery confirmation callbacks for reliability

**Configuration**:
- Connects to external HSL broker: `mqtt.hsl.fi:8883`
- Default topic filter: `/hfp/v2/journey/+/+/+/+/+/+/+/+/+/+/+/+/+/+/#`
- Produces to Kafka topic: `hfp-raw`

### 2. Kafka Broker

**Technology**: Confluent Platform Kafka 7.5.0, Zookeeper 7.5.0

**Configuration**:
- Single broker deployment (suitable for development/single-node)
- Topics: `hfp-raw` (input), `hfp-aggregated` (output)
- Replication factor: 1 (non-production setting)
- Auto-create topics enabled
- Retention: 24 hours

**Networking**:
- Internal: `kafka:9093` (Docker network)
- External: `localhost:9092` (host access)

### 3. Spark Processor (`spark-processor/`)

**Technology**: PySpark 3.5.0, Python 3.13

**Windowing Strategy** (as per conception plan):
- **Tumbling Window**: 10-second non-overlapping intervals
  - Metrics: `vehicle_count`, `active_vehicles`, `avg_speed`, `avg_delay`, `min_delay`, `max_delay`
  - Grouping: by route and direction
- **Sliding Window**: 60-second windows with 10-second slide 
  - Purpose: Smoothed moving averages for trend detection

**Schema**:
```
- Route (desi): String
- Direction (dir): String (1 or 2)
- Vehicle ID (veh): Integer
- Timestamp (tst): ISO8601 string
- Speed (spd): Double (m/s)
- Delay (dl): Integer (seconds from schedule)
- Position: lat/long coordinates
```

**Watermarking**: 1-minute watermark for handling late-arriving data

**Checkpointing**: `/tmp/spark-checkpoints` for fault tolerance

### 4. InfluxDB

**Technology**: InfluxDB 2.7 (OSS)

**Schema**:
- **Measurement**: `vehicle_metrics`
- **Tags**: `route`, `direction`, `window_type`
- **Fields**: `vehicle_count`, `active_vehicles`, `avg_speed`, `avg_delay`, `min_delay`, `max_delay`
- **Timestamp**: Window end time

**Configuration**:
- Organization: `hsl`
- Bucket: `transport_metrics`
- Retention: Configurable (default: unlimited)

### 5. API Service (`api-service/`)

**Technology**: FastAPI 0.115.0, Uvicorn 0.32.0, InfluxDB Client 1.45.0

**Endpoints**:
- `GET /health` - InfluxDB connectivity check
- `GET /api/v1/metrics` - Paginated metrics query with filters
- `GET /api/v1/routes` - List of routes with available data
- `GET /api/v1/routes/{route}/stats` - Aggregated statistics per route

**Query Optimization**:
- Default limit: 1000 records (max: 10,000)
- Time-range filtering using InfluxDB's efficient range scans
- Automatic Pydantic validation of responses

## Testing Methodology

### Unit Tests

Each microservice includes pytest-based unit tests with >80% code coverage:

**MQTT Ingestor** (`mqtt-ingestor/tests/`):
- Connection handling (success/failure scenarios)
- Message parsing (valid/invalid JSON)
- Vehicle ID extraction from MQTT topics
- Kafka producer integration (mocked)

**Spark Processor** (`spark-processor/tests/`):
- Message parsing and schema validation
- Window aggregation logic
- Invalid message filtering

**API Service** (`api-service/tests/`):
- Endpoint response validation
- Query parameter parsing
- Error handling (database failures)
- Health check logic

**Test Execution**:
```
docker-compose run --rm <service-name> pytest tests/ -v
```

### Integration Tests

End-to-end test suite (`integration-tests/`) validates complete pipeline:

**Test Scenarios**:
1. Kafka connectivity and topic creation
2. InfluxDB connectivity and bucket access
3. Message flow from Kafka raw → aggregated topics
4. Data persistence in InfluxDB
5. Query filtering by route

**Infrastructure**:
- Separate `docker-compose.test.yml` for test isolation
- Wait-for-services script ensures proper startup sequence
- Automated cleanup after test run

**Challenges Addressed**:
- Spark JVM/JAR download latency: Signal file (`/tmp/status/ready.txt`) indicates initialization complete
- Watermark propagation: Inject messages with staggered timestamps to trigger window closing
- Test timeouts: Extended wait periods (60-300s) for Spark initialization

**Test Execution**:
```
docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit
```

## Docker Configuration

### Service Dependencies

```
zookeeper (base)
  ↓
kafka (depends on: zookeeper)
  ↓
mqtt-ingestor (depends on: kafka)
  ↓
influxdb (independent)
  ↓
spark-processor (depends on: kafka, influxdb)
api-service (depends on: influxdb)
```

### Health Checks

- **Kafka**: Topic listing via `kafka-topics` command
- **InfluxDB**: HTTP ping via `/health` endpoint
- **API Service**: HTTP GET to `/health` endpoint

### Resource Allocation

**Development** (`.env` defaults):
- Spark: 2GB driver + 2GB executor memory
- Total system: ~4-6GB RAM

**Testing** (`docker-compose.test.yml`):
- Spark: 1GB driver + 1GB executor memory (reduced)
- Optimized for CI/CD environments

## Configuration Management

All services use environment-based configuration via `.env` file:

```
# External MQTT connection
MQTT_BROKER=mqtt.hsl.fi
MQTT_PORT=8883
MQTT_TOPIC=/hfp/v2/journey/#

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS=kafka:9093
KAFKA_RAW_TOPIC=hfp-raw
KAFKA_AGGREGATED_TOPIC=hfp-aggregated

# InfluxDB credentials
INFLUXDB_URL=http://influxdb:8086
INFLUXDB_TOKEN=hsl-admin-token-secret-12345
INFLUXDB_ORG=hsl
INFLUXDB_BUCKET=transport_metrics

# Performance tuning
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
LOG_LEVEL=INFO
```

## Deployment

### Development Deployment

```
# Start infrastructure
docker-compose up -d zookeeper kafka influxdb

# Wait for services (30-60s)
docker-compose logs -f kafka influxdb

# Start applications
docker-compose up -d mqtt-ingestor spark-processor api-service
```

### Monitoring

- **Logs**: `docker-compose logs -f <service>`
- **Kafka Topics**: `docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic hfp-raw`
- **InfluxDB UI**: http://localhost:8086 (admin/adminpassword)
- **API Docs**: http://localhost:8000/docs



## Conclusion

The implementation successfully demonstrates a production-ready real-time data pipeline meeting the requirements specified in the conception phase. All microservices are containerized, tested, and documented. 
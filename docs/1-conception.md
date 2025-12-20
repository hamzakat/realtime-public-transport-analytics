
# Real-Time Public Transport Analytics Platform: Conception Phase
This document overviews the conception phase of building a system that meets the requirements of Task 2 *“Build a real-time data backend for a data-intensive application”* as a part of the course DLMDSEDE02. The project develops a real-time data processing backend for analyzing high-frequency vehicle positioning public data stream from the Helsinki Region Transport (HSL) system[^1]. The Digitransit High-Frequency Positioning (HFP) API[^2] provides vehicle telemetry at one-second intervals via MQTT protocol, generating substantial data volumes that require robust ingestion, processing, and storage mechanisms. The system architecture adheres to microservices principles and Infrastructure as Code (IaC) practices, ensuring reproducibility and maintainability. 
# System Architecture
The proposed architecture shown in Figure 1 implements a lambda-like pattern (Databricks, n.d.) that serves both real-time streaming analytics and historical querying capabilities. Five distinct microservices coordinate through message-passing and database access patterns, with each component isolated within Docker containers. This separation of concerns enables independent scaling and maintenance while preserving system-wide data flow integrity. The data pipeline begins with ingestion from the external MQTT broker, progresses through a message buffer, undergoes stream processing with windowed aggregations, and terminates at dual sinks: a real-time publication channel and a time-series database. An API service provides controlled access to historical data, completing the architecture's ability to serve both operational monitoring and analytical workloads.


```
%%{init: {'theme':'base', 'themeVariables': { 'primaryColor':'#e3f2fd','primaryTextColor':'#1a1a1a','primaryBorderColor':'#1976d2','lineColor':'#424242','secondaryColor':'#f5f5f5','tertiaryColor':'#fff','fontSize':'14px','fontFamily':'Arial, sans-serif'}}}%%

graph TB
    subgraph ext["<b>External Source</b>"]
        A["<b>Digitransit HFP</b><br/>MQTT Broker<br/><i>mqtt.hsl.fi:8883</i>"]
    end
    
    subgraph docker["<b>Local Docker Environment</b>"]
        subgraph tier1["<b>Ingestion Tier</b>"]
            B["<b>Python MQTT</b><br/>Subscriber"]
        end
        
        subgraph tier2["<b>Buffer Tier</b>"]
            Z["<b>ZooKeeper</b><br/>Coordination"]
            C["<b>Kafka Broker</b><br/>Topic: hfp-raw"]
        end
        
        subgraph tier3["<b>Processing Tier</b>"]
            D["<b>Spark Streaming</b><br/>Windowed Aggregation"]
        end
        
        subgraph tier4["<b>Storage Tier</b>"]
            E["<b>Kafka Broker</b><br/>Topic: hfp-aggregated"]
            F["<b>InfluxDB</b><br/>Time-Series Store"]
        end
        
        subgraph tier5["<b>API Tier</b>"]
            H["<b>FastAPI</b><br/>Query Service"]
        end
    end
    
    subgraph output["<b>Consumer Layer</b>"]
        G["<b>ML App</b><br/><i>(conceptual)</i>"]
    end
    
    A -->|"MQTTs<br/>1Hz"| B
    B -->|"produce<br/>raw events"| C
    Z -.->|manages| C
    C -->|"real-time<br/>stream"| D
    D -->|"real-time<br/>metrics"| E
    D -->|"persist<br/>history"| F
    E -->|"live<br/>analytics"| G
    F --> H
    H -.->|"on-demand<br/>analytics"| G
    
    style ext fill:#eceff1,stroke:#37474f,stroke-width:3px,color:#1a1a1a
    style docker fill:#e3f2fd,stroke:#1976d2,stroke-width:4px,color:#1a1a1a
    style tier1 fill:#ffffff,stroke:#1976d2,stroke-width:2.5px,color:#1a1a1a
    style tier2 fill:#ffffff,stroke:#1976d2,stroke-width:2.5px,color:#1a1a1a
    style tier3 fill:#ffffff,stroke:#1976d2,stroke-width:2.5px,color:#1a1a1a
    style tier4 fill:#ffffff,stroke:#1976d2,stroke-width:2.5px,color:#1a1a1a
    style tier5 fill:#ffffff,stroke:#1976d2,stroke-width:2.5px,color:#1a1a1a
    style output fill:#fafafa,stroke:#757575,stroke-width:3px,stroke-dasharray: 8 4,color:#1a1a1a
    
    style A fill:#bbdefb,stroke:#1565c0,stroke-width:2.5px,color:#0d47a1
    style B fill:#e1f5fe,stroke:#0277bd,stroke-width:2.5px,color:#01579b
    style Z fill:#ffe0b2,stroke:#ef6c00,stroke-width:2.5px,color:#e65100
    style C fill:#fff3e0,stroke:#ef6c00,stroke-width:2.5px,color:#e65100
    style D fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2.5px,color:#4a148c
    style E fill:#fff3e0,stroke:#ef6c00,stroke-width:2.5px,color:#e65100
    style F fill:#e8f5e9,stroke:#388e3c,stroke-width:2.5px,color:#1b5e20
    style H fill:#fce4ec,stroke:#c2185b,stroke-width:2.5px,color:#880e4f
    style G fill:#ffffff,stroke:#9e9e9e,stroke-width:2.5px,color:#616161
```


*Figure *1* Proposed System Architecture*

## Data Flow and Component Integration
Raw vehicle position messages arrive via secure MQTT subscription and are immediately forwarded to Apache Kafka, which decouples the external data source from internal processing. This buffering tier provides resilience against downstream failures and enables replay capabilities for development and testing. Apache Spark Structured Streaming consumes from Kafka, performing stateful windowed aggregations that transform individual vehicle updates into route-level and network-level metrics. Processed results flow to two destinations: a Kafka topic for real-time consumption and InfluxDB for persistent storage. The FastAPI service queries InfluxDB on demand, providing RESTful access to historical analytics without exposing database internals to client applications.
# Microservice Specifications
## Data Ingestion
The ingestion service operates as a lightweight Python application responsible solely for protocol translation between MQTT and Kafka. MQTT (Message Queuing Telemetry Transport) is a lightweight, open-standard publish/subscribe messaging protocol designed for efficient communication in resource-constrained environments, such as IoT devices with limited bandwidth and power. It enables reliable data transmission through features like quality-of-service (QoS) levels, making it suitable for real-time applications like telemetry streaming (OASIS Open, n.d.). The ingestion service subscribes to relevant HFP topic filters, decodes UTF-8 JSON payloads, and produces messages to the hfp-raw Kafka topic. This single-responsibility design isolates external dependencies and simplifies error handling. The service will be containerized using a custom Dockerfile based on python:3.13-slim, with dependencies limited to paho-mqtt and kafka-python libraries.
## Message Buffering and Distribution
Apache Kafka serves as the central message bus, providing durability and decoupling between producers and consumers. The broker will maintain two topics: hfp-raw for unprocessed vehicle events and hfp-aggregated for computed metrics. Kafka's log-based storage ensures that messages persist beyond immediate consumption, enabling development iteration and fault recovery (Kreps et al., 2011). The standard confluentinc/cp-kafka and confluentinc/cp-zookeeper images provide a production-grade broker suitable for local deployment without modification.
## Stream Processing and Aggregation
Apache Spark Structured Streaming implements the core analytics logic through declarative DataFrame operations. The processing job reads from hfp-raw, parses the JSON payload and MQTT topic structure into a unified schema, and applies temporal windowing functions. Two aggregation strategies operate concurrently: tumbling windows compute non-overlapping statistics every ten seconds (e.g., active vehicle counts per route and direction), while sliding windows calculate moving averages over sixty-second spans with ten-second update intervals (e.g., mean speed, delay distribution). These windowing semantics balance computational overhead against analytical granularity, providing both instantaneous snapshots and smoothed trend indicators.

The Spark job writes its output to both downstream sinks through separate write operations. This dual-sink pattern requires careful coordination to maintain exactly-once semantics, which Spark achieves through its checkpointing mechanism (Armbrust et al., 2018). The official apache/spark Docker image provides a complete runtime environment, though minor configuration adjustments may be necessary to optimize memory allocation for the constrained laptop environment.
## Time-Series Persistence
InfluxDB stores aggregated metrics for historical analysis, offering efficient compression and query performance for time-stamped data. Unlike general-purpose databases, InfluxDB's columnar storage and specialized query language (InfluxQL or Flux) optimize temporal range scans and downsampling operations (InfluxData, n.d.). This choice minimizes resource consumption compared to traditional relational databases while providing native support for time-series semantics. The measurement schema will organize metrics by route, direction, and time window, with tags enabling efficient filtering and grouping. Retention policies can be configured to manage disk usage by automatically downsampling or expiring old data, though this optimization may be deferred until system testing reveals actual storage requirements.
## Historical Data API
The Python FastAPI[^3] service exposes a RESTful interface for querying historical aggregates from InfluxDB. Endpoints will support filtering by route, direction, time range, and aggregation interval, returning JSON responses compatible with frontend visualization libraries. This abstraction layer enforces access control and query validation, preventing potentially expensive database operations and maintaining a stable contract for client applications. FastAPI's automatic OpenAPI documentation generation aids development and testing by providing interactive API exploration (Ramírez, n.d.). The service runs as a custom container built from python:3.13-slim with dependencies on fastapi, uvicorn, and the InfluxDB client library.
# System Quality Attributes
Modern data-intensive systems must balance multiple quality attributes to ensure dependable, scalable, and maintainable performance. As outlined by Kleppmann (2017) in *Designing Data-Intensive Applications*, these attributes—reliability, scalability, maintainability, and security—form the foundation of robust system design. Reliability ensures that the system continues to function correctly despite faults; scalability guarantees that performance remains acceptable under increasing load; maintainability allows engineers to evolve and operate the system efficiently over time; and security governs the safe handling of data and operational integrity. Achieving these qualities requires careful architectural trade-offs rather than isolated design choices. The following subsections describe how these principles manifest in the current system architecture, drawing direct connections between theoretical foundations and practical implementation decisions.

## Reliability
Reliability emerges from multiple architectural decisions. Kafka's persistent log ensures that ingested data survives process failures, with configurable replication factors providing redundancy in production deployments. Spark's checkpointing mechanism records stream processing progress, enabling recovery from the last consistent state after interruptions. The decoupled microservice design isolates failures, preventing cascading outages when individual components restart. However, the single-node laptop deployment limits true high availability; a production system would require clustered brokers and distributed Spark executors.
## Scalability
The microservices architecture enables independent horizontal scaling of each tier. Ingestion can be parallelized by partitioning MQTT subscriptions across multiple instances, though the single external broker limits practical parallelism. Kafka scales naturally through topic partitioning, distributing message storage and consumption across multiple brokers. Spark distributes computation across executor nodes, with window operations naturally partitionable by route or geographic region. The API service scales through standard load balancing techniques, as its stateless design permits arbitrary instance multiplication. For this project's scope, vertical scaling (increasing container resource allocation) will likely suffice given the bounded dataset and single-laptop constraints.
## Maintainability
Maintainability stems from clear separation of concerns, comprehensive version control, and declarative infrastructure definitions. Each microservice encapsulates a single responsibility with well-defined interfaces, reducing cognitive load during debugging and enhancement. Docker Compose provides infrastructure-as-code, allowing the entire environment to be recreated from a single command. Git version control tracks all configuration files, Dockerfiles, and application code, enabling collaborative development and rollback capabilities. Structured logging from each component facilitates troubleshooting, with logs aggregated through Docker's logging drivers for centralized inspection.
## Security and Governance
Security considerations center on data transmission, access control, and operational visibility. The system uses MQTTs (MQTT over TLS) for external connections, encrypting vehicle data in transit from the Digitransit broker. Internal communication between Docker containers occurs over a private network, isolated from the host system's public interfaces. The FastAPI service will implement basic authentication and rate limiting to prevent unauthorized access or resource exhaustion, though comprehensive identity management exceeds project scope. Data governance practices include schema enforcement at ingestion (rejecting malformed messages) and processing (validating field types and ranges) stages, ensuring downstream consumers receive consistent, quality-assured data.

Audit logging represents a critical governance mechanism that should be implemented across all system components. The ingestion service will log connection events, subscription confirmations, and message receipt statistics, providing a record of data acquisition. Spark will emit processing metrics including record counts, window computations, and write confirmations, enabling verification of data completeness. The API service will log all query requests with timestamps, requested parameters, and response codes, creating an access audit trail. These logs should be structured in JSON format and persisted to mounted volumes, facilitating both real-time monitoring and post-hoc analysis. 

Additional governance measures include data retention policies configured in InfluxDB to automatically expire aggregates beyond a specified age, managing storage resources and respecting potential data minimization requirements. Schema versioning through explicit versioning fields in Kafka messages ensures backward compatibility as the data model evolves. Configuration management through environment variables and Docker secrets prevents hardcoding credentials, supporting secure credential rotation without code changes.
# Implementation Considerations
## Docker Image Selection
The system employs both official community images and custom containers. Kafka's complex configuration necessitates the battle-tested confluentinc/cp-kafka[^4] and confluentinc/cp-zookeeper[^5] images, which bundle necessary dependencies and provide sensible defaults. Spark computation uses apache/spark[^6], selecting a recent stable release with structured streaming support. Time-series persistence relies on Influxdb 3[^7], choosing the modern InfluxDB  line for its improved query language and user interface. Custom services (MQTT ingestor and FastAPI) build from python:3.13-slim[^8] to minimize image size while maintaining compatibility with recent Python libraries. All images will be pinned to specific versions in production configurations to ensure reproducibility, though latest tags may be used during initial development for convenience.
## Real-Time Streaming Characteristics
The HFP data stream's one-second update frequency presents both opportunity and challenge. This high temporal resolution enables responsive analytics but generates approximately 86,400 messages per vehicle per day, quickly accumulating to millions of records for HSL's fleet. The system must balance processing latency against computational efficiency; ten-second aggregation windows reduce output volume by an order of magnitude while preserving sufficient temporal granularity for operational monitoring. The sliding window's sixty-second span provides context for trend detection without excessive state retention.
## Windowing and Aggregation Strategy
Two complementary windowing strategies address different analytical needs:

- **Tumbling windows** partition the stream into discrete, non-overlapping intervals, computing metrics such as active vehicle counts per route, unique vehicles observed, and mean delay from schedule. These statistics support operational dashboards displaying current system state. 
- **Sliding windows** overlay the stream with overlapping intervals, calculating moving averages of speed, acceleration, and punctuality. The sixty-second window with ten-second slide creates six overlapping views, smoothing transient fluctuations while responding reasonably quickly to genuine trend changes. 

Both window types key by route and direction, producing per-route time series suitable for comparative analysis. (Kleppmann, 2017, p. 472).

Aggregation functions will include counts (vehicles per route-direction pair), means (average speed, delay), and potentially percentiles (median speed, 95th percentile delay) if Spark's aggregation performance permits. More sophisticated analytics such as stop prediction or congestion detection fall outside scope but could leverage this infrastructure in future work.

# References
Apache Software Foundation. (n.d.-a). Apache Kafka documentation. Retrieved October 19, 2025, from <https://kafka.apache.org/documentation/> 

Apache Software Foundation. (n.d.-b). Apache Spark – Unified analytics engine for large-scale data processing. Retrieved October 19, 2025, from <https://spark.apache.org/> 

Armbrust, M., Das, T., Torres, J., Yavuz, B., Zhu, S., Xin, R., Ghodsi, A., Stoica, I., & Zaharia, M. (2018). Structured streaming. *Proceedings of the 2018 International Conference on Management of Data*. <https://doi.org/10.1145/3183713.3190664> 

Databricks. (n.d.). Lambda architecture. Databricks glossary. Retrieved October 19, 2025, from <https://www.databricks.com/glossary/lambda-architecture> 

Digitransit. (n.d.). High-frequency positioning. Retrieved October 19, 2025, from <https://digitransit.fi/en/developers/apis/5-realtime-api/vehicle-positions/high-frequency-positioning/>

Helsinki Region Transport (HSL). (n.d.). Open data. Retrieved October 19, 2025, from <https://www.hsl.fi/en/hsl/open-data> 

InfluxData. (n.d.). InfluxDB 3 core. Retrieved October 19, 2025, from <https://www.influxdata.com/products/influxdb/> 

Kleppmann, M. (2017). *Designing data-intensive applications: The big ideas behind reliable, scalable, and maintainable systems*. O'Reilly Media.

Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: A distributed messaging system for log processing. *Proceedings of the NetDB'11 Workshop*. <https://notes.stephenholiday.com/Kafka.pdf> 

OASIS Open. (n.d.). MQTT version 5.0. Retrieved October 19, 2025, from <https://www.oasis-open.org/standard/mqtt-v5-0-os/> 

Ramírez, S. (n.d.). FastAPI – a modern, fast (high-performance) web framework for building APIs with Python based on standard Python type hints. Retrieved October 19, 2025, from <https://fastapi.tiangolo.com/> 

[^1]: <https://www.hsl.fi/en/hsl/open-data> 
[^2]: <https://digitransit.fi/en/developers/apis/5-realtime-api/vehicle-positions/high-frequency-positioning/> 
[^3]: <https://fastapi.tiangolo.com/> 
[^4]: <https://hub.docker.com/r/confluentinc/cp-kafka/> 
[^5]: <https://hub.docker.com/r/confluentinc/cp-zookeeper> 
[^6]: <https://hub.docker.com/_/spark> 
[^7]: <https://hub.docker.com/_/influxdb> 
[^8]: <https://hub.docker.com/_/python> 
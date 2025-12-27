"""Spark Structured Streaming processor with audit logging."""

import json
import logging
import sys
from typing import Iterator
from datetime import datetime, timezone
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    approx_count_distinct,
    avg,
    min as spark_min,
    max as spark_max,
    current_timestamp,
    to_timestamp,
    lit,
    struct,
    to_json,
)
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from .config import Config
from .schemas import RAW_MESSAGE_SCHEMA


class StructuredLogger:
    """Structured JSON logger for audit trails."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = logging.getLogger(f"{service_name}.audit")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def log_event(self, event_type: str, **kwargs):
        """Log structured event."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name,
            "event_type": event_type,
            **kwargs,
        }
        self.logger.info(json.dumps(log_entry))


class HFPStreamProcessor:
    """Process HFP vehicle position stream with audit logging."""

    def __init__(self, config: Config):
        """Initialize processor with configuration."""
        self.config = config
        self.logger = self._setup_logging()
        self.audit_logger = StructuredLogger("spark-processor")
        self.spark = self._create_spark_session()
        self.influxdb_client = None
        self.batch_count = 0

    def _setup_logging(self) -> logging.Logger:
        """Configure logging."""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
        )
        return logging.getLogger(__name__)

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        self.logger.info("Creating Spark session...")

        spark = (
            SparkSession.builder.appName("HSL-HFP-Processor")
            .config("spark.driver.memory", self.config.spark_driver_memory)
            .config("spark.executor.memory", self.config.spark_executor_memory)
            .config(
                "spark.sql.streaming.checkpointLocation",
                self.config.checkpoint_location,
            )
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        self.audit_logger.log_event(
            "spark_session_created",
            driver_memory=self.config.spark_driver_memory,
            executor_memory=self.config.spark_executor_memory,
        )

        self.logger.info("Spark session created successfully")
        return spark

    def _init_influx_client(self):
        """Lazily initialize InfluxDB client."""
        if self.influxdb_client is None:
            self.logger.info(
                f"Initializing InfluxDB client for org='{self.config.influxdb_org}', "
                f"bucket='{self.config.influxdb_bucket}'"
            )
            self.influxdb_client = InfluxDBClient(
                url=self.config.influxdb_url,
                token=self.config.influxdb_token,
                org=self.config.influxdb_org,
            )

            self.audit_logger.log_event(
                "influxdb_connected",
                url=self.config.influxdb_url,
                org=self.config.influxdb_org,
                bucket=self.config.influxdb_bucket,
            )
        return self.influxdb_client

    def _read_from_kafka(self) -> DataFrame:
        """Read streaming data from Kafka."""
        bootstrap_servers = self.config.kafka_bootstrap_servers or "kafka:9093"
        self.logger.info(f"Connecting to Kafka: {bootstrap_servers}")

        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", self.config.kafka_input_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        self.audit_logger.log_event(
            "kafka_stream_started",
            bootstrap_servers=bootstrap_servers,
            topic=self.config.kafka_input_topic,
        )

        self.logger.info(f"Subscribed to Kafka topic: {self.config.kafka_input_topic}")
        return df

    def _parse_messages(self, df: DataFrame) -> DataFrame:
        """Parse JSON messages and extract vehicle position data."""
        self.logger.info("Parsing messages...")

        # Parse JSON value
        parsed_df = df.select(
            from_json(col("value").cast("string"), RAW_MESSAGE_SCHEMA).alias("data")
        )

        # Flatten structure and extract relevant fields
        flattened_df = parsed_df.select(
            col("data.mqtt_topic").alias("mqtt_topic"),
            to_timestamp(col("data.payload.VP.tst")).alias("event_time"),
            col("data.payload.VP.desi").alias("route"),
            col("data.payload.VP.dir").alias("direction"),
            col("data.payload.VP.veh").alias("vehicle_id"),
            col("data.payload.VP.spd").alias("speed"),
            col("data.payload.VP.dl").alias("delay"),
            col("data.payload.VP.lat").alias("latitude"),
            col("data.payload.VP.long").alias("longitude"),
            col("data.payload.VP.oper").alias("operator"),
        ).filter(
            # Filter out messages without required fields
            col("route").isNotNull()
            & col("direction").isNotNull()
            & col("event_time").isNotNull()
        )

        return flattened_df

    def _write_batch_to_influx(self, batch_df: DataFrame, batch_id: int):
        """Write a micro-batch of aggregated data to InfluxDB."""
        if batch_df.rdd.isEmpty():
            return

        self.batch_count += 1
        self.logger.info(f"Writing batch {batch_id} to InfluxDB")
        client = self._init_influx_client()
        write_api = client.write_api(write_options=SYNCHRONOUS)

        pdf = batch_df.toPandas()
        points = []

        for _, row in pdf.iterrows():
            try:
                point = (
                    Point("vehicle_metrics")
                    .tag("route", str(row["route"]))
                    .tag("direction", str(row["direction"]))
                    .tag("window_type", str(row["window_type"]))
                    .field("vehicle_count", int(row["vehicle_count"]))
                )

                if row.get("active_vehicles") is not None:
                    point = point.field("active_vehicles", int(row["active_vehicles"]))

                if row.get("avg_speed") is not None:
                    point = point.field("avg_speed", float(row["avg_speed"]))
                if row.get("avg_delay") is not None:
                    point = point.field("avg_delay", float(row["avg_delay"]))
                if row.get("min_delay") is not None:
                    point = point.field("min_delay", float(row["min_delay"]))
                if row.get("max_delay") is not None:
                    point = point.field("max_delay", float(row["max_delay"]))

                if row.get("window_end") is not None:
                    point = point.time(row["window_end"])

                points.append(point)
            except Exception as e:
                self.logger.warning(
                    f"Failed to convert row to InfluxDB point: {e}", exc_info=True
                )

        if points:
            write_api.write(
                bucket=self.config.influxdb_bucket,
                org=self.config.influxdb_org,
                record=points,
            )

            self.audit_logger.log_event(
                "batch_written_to_influxdb",
                batch_id=batch_id,
                points_written=len(points),
                total_batches=self.batch_count,
            )

            self.logger.info(
                f"Wrote {len(points)} points to InfluxDB for batch {batch_id}"
            )

    def run(self):
        """Main processing pipeline."""
        try:
            self.audit_logger.log_event("service_startup")
            self.logger.info("Initializing Spark Streaming processor...")

            # Read from Kafka
            kafka_df = self._read_from_kafka()

            # Parse messages
            parsed_df = self._parse_messages(kafka_df)

            # Tumbling window aggregations (10 seconds)
            tumbling_df = (
                parsed_df.withWatermark("event_time", "1 minute")
                .groupBy(
                    window("event_time", "10 seconds").alias("window"),
                    col("route"),
                    col("direction"),
                )
                .agg(
                    count("*").alias("vehicle_count"),
                    approx_count_distinct("vehicle_id").alias("active_vehicles"),
                    avg("speed").alias("avg_speed"),
                    avg("delay").alias("avg_delay"),
                    spark_min("delay").alias("min_delay"),
                    spark_max("delay").alias("max_delay"),
                )
                .withColumn("window_type", lit("tumbling_10s"))
            )

            # Sliding window aggregations (60 seconds with 10-second slide)
            sliding_df = (
                parsed_df.withWatermark("event_time", "1 minute")
                .groupBy(
                    window("event_time", "60 seconds", "10 seconds").alias("window"),
                    col("route"),
                    col("direction"),
                )
                .agg(
                    count("*").alias("vehicle_count"),
                    approx_count_distinct("vehicle_id").alias("active_vehicles"),
                    avg("speed").alias("avg_speed"),
                    avg("delay").alias("avg_delay"),
                    spark_min("delay").alias("min_delay"),
                    spark_max("delay").alias("max_delay"),
                )
                .withColumn("window_type", lit("sliding_60s"))
            )

            # Combine both window types
            combined_df = tumbling_df.union(sliding_df)

            # Common selection for outputs
            output_df = combined_df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("route"),
                col("direction"),
                col("window_type"),
                col("vehicle_count"),
                col("active_vehicles"),
                col("avg_speed"),
                col("avg_delay"),
                col("min_delay"),
                col("max_delay"),
                current_timestamp().alias("processed_at"),
            )

            self.audit_logger.log_event(
                "windowing_configured",
                tumbling_window="10 seconds",
                sliding_window="60 seconds with 10s slide",
                watermark="1 minute",
            )

            # Output to console for debugging
            query = (
                output_df.writeStream.outputMode("update")
                .format("console")
                .trigger(processingTime="10 seconds")
                .start()
            )

            # Write to Kafka for downstream consumers
            kafka_output_query = (
                output_df.withColumn(
                    "value",
                    to_json(
                        struct(
                            col("window_start"),
                            col("window_end"),
                            col("route"),
                            col("direction"),
                            col("window_type"),
                            col("vehicle_count"),
                            col("active_vehicles"),
                            col("avg_speed"),
                            col("avg_delay"),
                            col("min_delay"),
                            col("max_delay"),
                            col("processed_at"),
                        )
                    ),
                )
                .writeStream.outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
                .option("topic", self.config.kafka_output_topic)
                .option(
                    "checkpointLocation",
                    self.config.checkpoint_location + "/kafka-output",
                )
                .trigger(processingTime="10 seconds")
                .start()
            )

            # Write aggregations to InfluxDB
            influx_query = (
                output_df.writeStream.outputMode("update")
                .foreachBatch(self._write_batch_to_influx)
                .option(
                    "checkpointLocation",
                    self.config.checkpoint_location + "/influxdb-output",
                )
                .trigger(processingTime="10 seconds")
                .start()
            )

            self.audit_logger.log_event(
                "streaming_queries_started",
                output_sinks=["console", "kafka", "influxdb"],
                trigger_interval="10 seconds",
            )

            # Wait for termination
            self.logger.info("Starting streaming queries...")

            # Signal initialization complete
            import os

            os.makedirs("/tmp/status", exist_ok=True)
            with open("/tmp/status/ready.txt", "w") as f:
                f.write("READY")
            self.logger.info("Initialization complete. Ready signal written to disk.")

            query.awaitTermination()
            kafka_output_query.awaitTermination()
            influx_query.awaitTermination()

        except Exception as e:
            self.audit_logger.log_event("fatal_error", error=str(e))
            self.logger.error(f"Error in streaming processing: {e}", exc_info=True)
            raise


def main():
    """Entry point for the processor service."""
    config = Config.from_env()
    processor = HFPStreamProcessor(config)
    processor.run()


if __name__ == "__main__":
    main()

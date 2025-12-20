"""Schema definitions for HFP data."""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, LongType
)


# Schema for raw MQTT message
RAW_MESSAGE_SCHEMA = StructType([
    StructField("mqtt_topic", StringType(), False),
    StructField("received_at", DoubleType(), False),
    StructField("payload", StructType([
        StructField("VP", StructType([
            StructField("desi", StringType(), True),  # Route number
            StructField("dir", StringType(), True),   # Direction (1/2)
            StructField("oper", IntegerType(), True), # Operator
            StructField("veh", IntegerType(), True),  # Vehicle number
            StructField("tst", StringType(), True),   # Timestamp
            StructField("lat", DoubleType(), True),   # Latitude
            StructField("long", DoubleType(), True),  # Longitude
            StructField("spd", DoubleType(), True),   # Speed m/s
            StructField("hdg", IntegerType(), True),  # Heading
            StructField("dl", IntegerType(), True),   # Delay in seconds
            StructField("odo", IntegerType(), True),  # Odometer
            StructField("drst", IntegerType(), True), # Door status
            StructField("oday", StringType(), True),  # Operating day
            StructField("jrn", IntegerType(), True),  # Journey number
            StructField("line", IntegerType(), True), # Line number
            StructField("start", StringType(), True), # Journey start time
        ]), True)
    ]), False)
])
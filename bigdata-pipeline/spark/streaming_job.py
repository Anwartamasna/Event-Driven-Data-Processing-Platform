#!/usr/bin/env python3
"""
Spark Structured Streaming Job - Real-time Event Processing

This job reads user events from Kafka, parses the JSON, and writes
raw events to the data lake in Parquet format, partitioned by date.

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming_job.py

This job runs CONTINUOUSLY and is NOT orchestrated by Airflow.
Start it once and let it run.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "user_events"

# Output paths
RAW_DATA_PATH = "/data/raw/user_events"
CHECKPOINT_PATH = "/data/checkpoints/streaming"


def get_event_schema():
    """Define the schema for user events."""
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("page_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("session_id", StringType(), True)
    ])


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("UserEventsStreaming") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


def main():
    print("=" * 60)
    print("SPARK STREAMING JOB - User Events Pipeline")
    print("=" * 60)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Output Path: {RAW_DATA_PATH}")
    print(f"Checkpoint Path: {CHECKPOINT_PATH}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    print("Connecting to Kafka...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("Kafka connection established!")
    
    # Parse the JSON value from Kafka
    schema = get_event_schema()
    
    parsed_df = kafka_df \
        .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json_value") \
        .select(
            col("key"),
            from_json(col("json_value"), schema).alias("event")
        ) \
        .select(
            col("key"),
            col("event.user_id").alias("user_id"),
            col("event.event_type").alias("event_type"),
            col("event.timestamp").alias("event_timestamp"),
            col("event.page_id").alias("page_id"),
            col("event.product_id").alias("product_id"),
            col("event.session_id").alias("session_id")
        )
    
    # Add date partition columns
    events_with_date = parsed_df \
        .withColumn("event_date", to_date(col("event_timestamp"))) \
        .withColumn("year", year(col("event_date"))) \
        .withColumn("month", month(col("event_date"))) \
        .withColumn("day", dayofmonth(col("event_date")))
    
    # Write to Parquet with date partitioning
    print("Starting streaming query...")
    query = events_with_date.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .partitionBy("year", "month", "day") \
        .option("path", RAW_DATA_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Streaming query started successfully!")
    print("Writing events to:", RAW_DATA_PATH)
    print("Press Ctrl+C to stop...")
    print("-" * 60)
    
    # Wait for termination
    query.awaitTermination()


if __name__ == "__main__":
    main()

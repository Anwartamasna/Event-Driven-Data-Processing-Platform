#!/usr/bin/env python3
"""
Spark Batch Job - Daily Metrics Aggregation

This job reads raw events from the data lake, aggregates daily metrics,
and writes the results to the processed data directory.

This job is triggered by Airflow on a daily schedule.

Usage:
    spark-submit batch_job.py [--date YYYY-MM-DD]
    
If no date is provided, defaults to yesterday.
"""

import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, lit, current_timestamp
)


# Paths
RAW_DATA_PATH = "/data/raw/user_events"
PROCESSED_DATA_PATH = "/data/processed/daily_metrics"


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("DailyMetricsBatch") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


def get_target_date(date_str=None):
    """Get the target date for processing. Defaults to yesterday."""
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        return (datetime.now() - timedelta(days=1)).date()


def main():
    parser = argparse.ArgumentParser(description='Spark Daily Batch Job')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Date to process (YYYY-MM-DD). Defaults to yesterday.'
    )
    
    args = parser.parse_args()
    target_date = get_target_date(args.date)
    
    print("=" * 60)
    print("SPARK BATCH JOB - Daily Metrics Aggregation")
    print("=" * 60)
    print(f"Processing date: {target_date}")
    print(f"Raw data path: {RAW_DATA_PATH}")
    print(f"Output path: {PROCESSED_DATA_PATH}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Build the path for the specific date partition
    year = target_date.year
    month = target_date.month
    day = target_date.day
    
    partition_path = f"{RAW_DATA_PATH}/year={year}/month={month}/day={day}"
    print(f"Reading from partition: {partition_path}")
    
    try:
        # Read raw events for the target date
        raw_events = spark.read.parquet(partition_path)
        
        total_events = raw_events.count()
        print(f"Total events found: {total_events}")
        
        if total_events == 0:
            print("WARNING: No events found for this date!")
            # Create empty metrics to indicate processing was attempted
            empty_metrics = spark.createDataFrame([], schema="event_type STRING, event_count LONG, unique_users LONG, process_date DATE, processed_at TIMESTAMP")
            empty_metrics.write \
                .mode("overwrite") \
                .partitionBy("process_date") \
                .parquet(PROCESSED_DATA_PATH)
            print("Wrote empty metrics file.")
            return 0
        
        # Aggregate 1: Events per event type
        events_by_type = raw_events.groupBy("event_type") \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users")
            ) \
            .withColumn("process_date", lit(str(target_date)).cast("date")) \
            .withColumn("processed_at", current_timestamp())
        
        print("\n--- Events by Type ---")
        events_by_type.show()
        
        # Aggregate 2: Overall daily summary
        daily_summary = raw_events.agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("total_unique_users"),
            countDistinct("session_id").alias("total_sessions")
        ) \
            .withColumn("event_type", lit("_DAILY_TOTAL_")) \
            .withColumn("process_date", lit(str(target_date)).cast("date")) \
            .withColumn("processed_at", current_timestamp())
        
        # Rename columns to match schema
        daily_summary = daily_summary.select(
            col("event_type"),
            col("total_events").alias("event_count"),
            col("total_unique_users").alias("unique_users"),
            col("process_date"),
            col("processed_at")
        )
        
        print("\n--- Daily Summary ---")
        daily_summary.show()
        
        # Combine metrics
        combined_metrics = events_by_type.union(daily_summary)
        
        # Write to processed directory, partitioned by date
        output_path = f"{PROCESSED_DATA_PATH}"
        print(f"\nWriting metrics to: {output_path}")
        
        combined_metrics.write \
            .mode("append") \
            .partitionBy("process_date") \
            .parquet(output_path)
        
        print("=" * 60)
        print("BATCH JOB COMPLETED SUCCESSFULLY!")
        print(f"Processed {total_events} events for {target_date}")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        
        # Check if it's a "path not found" error
        if "Path does not exist" in str(e):
            print(f"No data found for date {target_date}")
            print("This might be because:")
            print("  1. No events were generated for this date")
            print("  2. The streaming job hasn't processed any data yet")
            print("  3. The date partition doesn't exist")
        
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    exit(main())

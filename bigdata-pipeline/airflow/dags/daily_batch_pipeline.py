"""
Airflow DAG - Daily Batch Pipeline

This DAG orchestrates the daily batch processing pipeline:
1. Wait for yesterday's raw data to be available
2. Run Spark batch job to aggregate daily metrics
3. Validate the output results

Schedule: Daily at 2:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import os


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}


def validate_results(**context):
    """
    Validate that the batch job produced valid output.
    
    Checks:
    1. Output directory exists
    2. Output has data files
    3. Basic data quality checks
    """
    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d')
    target_date = execution_date - timedelta(days=1)
    
    # Path where batch job writes output
    output_base = "/data/processed/daily_metrics"
    output_path = f"{output_base}/process_date={target_date.date()}"
    
    print(f"Validating output for date: {target_date.date()}")
    print(f"Checking path: {output_path}")
    
    # Check 1: Directory exists
    if not os.path.exists(output_path):
        # Also check if there are any parquet files in the base path
        if os.path.exists(output_base):
            files = os.listdir(output_base)
            print(f"Files in {output_base}: {files}")
        raise ValueError(f"Output directory does not exist: {output_path}")
    
    # Check 2: Has parquet files
    parquet_files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
    
    if not parquet_files:
        raise ValueError(f"No parquet files found in output directory: {output_path}")
    
    print(f"Found {len(parquet_files)} parquet file(s)")
    
    # Check 3: Files have non-zero size
    total_size = 0
    for pf in parquet_files:
        file_path = os.path.join(output_path, pf)
        size = os.path.getsize(file_path)
        total_size += size
        print(f"  {pf}: {size} bytes")
    
    if total_size == 0:
        raise ValueError("All parquet files are empty!")
    
    print(f"Total output size: {total_size} bytes")
    print("Validation PASSED!")
    
    return True


def get_yesterday_path(**context):
    """Get the path for yesterday's raw data."""
    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d')
    yesterday = execution_date - timedelta(days=1)
    
    path = (
        f"/data/raw/user_events/"
        f"year={yesterday.year}/"
        f"month={yesterday.month}/"
        f"day={yesterday.day}"
    )
    return path


# Define the DAG
with DAG(
    dag_id='daily_batch_pipeline',
    default_args=default_args,
    description='Daily batch pipeline for user event metrics aggregation',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['batch', 'spark', 'metrics'],
) as dag:
    
    # Task 1: Wait for yesterday's data to be available
    # Note: FileSensor requires the file to exist. In production,
    # you might use a custom sensor or ExternalTaskSensor
    wait_for_data = BashOperator(
        task_id='wait_for_data',
        bash_command='''
            # Get yesterday's date
            YESTERDAY=$(date -d "{{ ds }} -1 day" +%Y-%-m-%-d)
            YEAR=$(date -d "{{ ds }} -1 day" +%Y)
            MONTH=$(date -d "{{ ds }} -1 day" +%-m)
            DAY=$(date -d "{{ ds }} -1 day" +%-d)
            
            DATA_PATH="/data/raw/user_events/year=${YEAR}/month=${MONTH}/day=${DAY}"
            
            echo "Checking for data at: ${DATA_PATH}"
            
            # Wait up to 30 minutes for data to appear
            TIMEOUT=1800
            ELAPSED=0
            INTERVAL=30
            
            while [ ! -d "${DATA_PATH}" ] || [ -z "$(ls -A ${DATA_PATH} 2>/dev/null)" ]; do
                if [ $ELAPSED -ge $TIMEOUT ]; then
                    echo "Timeout waiting for data at ${DATA_PATH}"
                    exit 1
                fi
                echo "Waiting for data... (${ELAPSED}s elapsed)"
                sleep $INTERVAL
                ELAPSED=$((ELAPSED + INTERVAL))
            done
            
            echo "Data found at ${DATA_PATH}"
            ls -la ${DATA_PATH}
        ''',
        retries=3,
        retry_delay=timedelta(minutes=10),
    )
    
    # Task 2: Run Spark batch job
    # Uses spark-submit to execute the batch job on the Spark cluster
    run_spark_batch = BashOperator(
        task_id='run_spark_batch',
        bash_command='''
            # Get yesterday's date for the batch job
            PROCESS_DATE=$(date -d "{{ ds }} -1 day" +%Y-%m-%d)
            
            echo "Running Spark batch job for date: ${PROCESS_DATE}"
            
            # Submit the Spark job
            # Note: In Docker, we use docker exec to run spark-submit
            # When running from Airflow container, we can call spark-submit directly
            # if pyspark is installed, or use the Spark master's spark-submit
            
            spark-submit \
                --master local[*] \
                /spark/batch_job.py \
                --date ${PROCESS_DATE}
            
            EXIT_CODE=$?
            
            if [ $EXIT_CODE -ne 0 ]; then
                echo "Spark job failed with exit code: ${EXIT_CODE}"
                exit $EXIT_CODE
            fi
            
            echo "Spark batch job completed successfully!"
        ''',
        execution_timeout=timedelta(hours=1),
    )
    
    # Task 3: Validate results
    validate_results_task = PythonOperator(
        task_id='validate_results',
        python_callable=validate_results,
        provide_context=True,
    )
    
    # Define task dependencies
    wait_for_data >> run_spark_batch >> validate_results_task

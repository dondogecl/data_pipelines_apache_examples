"""
Simple Bronze Layer Pipeline for Airflow/Composer

This is a minimal, easy-to-understand DAG that:
1. Loads configuration from a YAML file
2. Submits a Dataproc Serverless job to process CSV -> Parquet

Perfect for learning and testing. You can expand it step by step as needed.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import yaml

# ============================================================================
# CONFIGURATION
# ============================================================================
# Path to your config file (adjust for your Airflow/Composer setup)
CONFIG_FILE = "/opt/airflow/dags/config/simple_pipeline_config.yaml"

# Load the YAML configuration
with open(CONFIG_FILE, 'r') as f:
    config = yaml.safe_load(f)

# Extract values from config for easy access
PROJECT_ID = config['project_id']
REGION = config['region']
LANDING_BUCKET = config['buckets']['landing']
BRONZE_BUCKET = config['buckets']['bronze']
DEPS_BUCKET = config['buckets']['deps']
SOURCE_FILE = config['table']['source_file']
TABLE_NAME = config['table']['table_name']
SERVICE_ACCOUNT = config['dataproc']['service_account']
SCHEDULE_CRON = config['schedule']['cron']

# ============================================================================
# DAG DEFINITION
# ============================================================================
# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,  # Don't wait for previous runs
    'start_date': days_ago(1),  # Start from yesterday
    'retries': 1,  # Retry once if task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 min before retry
}

# Create the DAG
dag = DAG(
    dag_id='simple_bronze_pipeline',  # DAG name in Airflow UI
    default_args=default_args,
    description='Simple bronze ingestion using Dataproc Serverless',
    schedule_interval=SCHEDULE_CRON,  # From config: daily at 2 AM
    catchup=False,  # Don't run for past dates
    tags=['bronze', 'simple', 'dataproc'],
)

# ============================================================================
# DATAPROC JOB CONFIGURATION
# ============================================================================
# This defines what Spark job to run and with what parameters
DATAPROC_JOB = {
    "reference": {
        "project_id": PROJECT_ID,
    },
    "placement": {
        "cluster_name": "",  # Empty string = Dataproc Serverless
    },
    "pyspark_job": {
        # Location of your Python script in GCS
        "main_python_file_uri": f"gs://{DEPS_BUCKET}/bronze_ingestion.py",

        # Arguments passed to your Python script
        "args": [
            f"--project_id={PROJECT_ID}",
            f"--landing_bucket={LANDING_BUCKET}",
            f"--bronze_bucket={BRONZE_BUCKET}",
            f"--source_file={SOURCE_FILE}",
            f"--table_name={TABLE_NAME}",
            f"--batch_date={{{{ ds }}}}",  # Airflow macro: execution date (YYYY-MM-DD)
            "--environment=airflow",
        ],

        # Spark configuration properties
        "properties": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
    }
}

# ============================================================================
# TASK DEFINITION
# ============================================================================
# Create the task that submits the Dataproc job
submit_bronze_job = DataprocSubmitJobOperator(
    task_id='bronze_ingestion',  # Task name in Airflow UI
    job=DATAPROC_JOB,  # Job config defined above
    location=REGION,  # Where to run the job
    project_id=PROJECT_ID,  # Your GCP project
    dag=dag,  # Attach to our DAG
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================
# For now, we only have one task, so no dependencies needed
# Later you can add: task1 >> task2 >> task3 (runs in sequence)
# Or: [task1, task2] >> task3 (task1 and task2 run in parallel, then task3)

# ============================================================================
# HOW TO EXPAND THIS DAG:
# ============================================================================
# 1. Add a validation task before the job:
#    - Check if source file exists in landing bucket
#    - Validate file schema/format
#
# 2. Add a notification task after the job:
#    - Send email on success/failure
#    - Post to Slack
#
# 3. Add data quality checks:
#    - Count records processed
#    - Check for null values
#    - Validate data ranges
#
# 4. Add multiple tables:
#    - Loop over config['tables'] if you have multiple tables
#    - Create one task per table (or use dynamic task mapping)
#
# Example expansion (commented out):
#
# from airflow.operators.python import PythonOperator
#
# def check_file_exists(**context):
#     """Check if source file exists in GCS"""
#     # Your validation code here
#     pass
#
# validate_task = PythonOperator(
#     task_id='validate_source_file',
#     python_callable=check_file_exists,
#     dag=dag,
# )
#
# # Set dependency: validate first, then run ingestion
# validate_task >> submit_bronze_job

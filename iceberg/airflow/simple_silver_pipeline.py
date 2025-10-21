"""
Simple Silver Layer Pipeline for Airflow/Composer

This DAG:
1. Loads configuration from a YAML file
2. Submits a Dataproc Serverless job to transform Bronze -> Silver

Runs after bronze ingestion completes to apply data quality and transformations.
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
BRONZE_BUCKET = config['buckets']['bronze']
SILVER_BUCKET = config['buckets']['silver']
DEPS_BUCKET = config['buckets']['deps']
TABLE_NAME = config['table']['table_name']
SERVICE_ACCOUNT = config['dataproc']['service_account']
SCHEDULE_CRON = config['schedule']['cron']

# ============================================================================
# DAG DEFINITION
# ============================================================================
# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    dag_id='simple_silver_pipeline',
    default_args=default_args,
    description='Simple silver transformation using Dataproc Serverless',
    schedule_interval=SCHEDULE_CRON,  # Same schedule as bronze
    catchup=False,
    tags=['silver', 'simple', 'dataproc'],
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
        "main_python_file_uri": f"gs://{DEPS_BUCKET}/silver_transformation.py",

        # Arguments passed to your Python script
        "args": [
            f"--project_id={PROJECT_ID}",
            f"--bronze_bucket={BRONZE_BUCKET}",
            f"--silver_bucket={SILVER_BUCKET}",
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
submit_silver_job = DataprocSubmitJobOperator(
    task_id='silver_transformation',
    job=DATAPROC_JOB,
    location=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================
# This DAG runs independently on the same schedule as bronze
# If you want to trigger this after bronze completes, you can:
# 1. Use TriggerDagRunOperator in the bronze DAG to trigger this one
# 2. Use an ExternalTaskSensor here to wait for bronze completion
# 3. Combine both DAGs into a single DAG with dependencies

# Example of waiting for bronze DAG completion (commented out):
#
# from airflow.sensors.external_task import ExternalTaskSensor
#
# wait_for_bronze = ExternalTaskSensor(
#     task_id='wait_for_bronze',
#     external_dag_id='simple_bronze_pipeline',
#     external_task_id='bronze_ingestion',
#     dag=dag,
# )
#
# wait_for_bronze >> submit_silver_job

# ============================================================================
# HOW TO EXPAND THIS DAG:
# ============================================================================
# 1. Add data quality checks after transformation:
#    - Validate record counts (bronze vs silver)
#    - Check for required columns
#    - Validate data ranges and business rules
#
# 2. Add conditional logic:
#    - Skip processing if bronze has no data
#    - Handle incremental vs full refresh
#
# 3. Add notifications:
#    - Send alerts on data quality issues
#    - Report transformation statistics
#
# 4. Add multiple tables:
#    - Process multiple silver transformations in parallel
#    - Use dynamic task mapping for table list

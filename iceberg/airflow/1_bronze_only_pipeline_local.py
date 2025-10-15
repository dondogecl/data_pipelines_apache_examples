"""
Bronze Layer Ingestion Pipeline - Local Development Version

This DAG orchestrates the ingestion of CSV files from the landing bucket
into the bronze layer as Parquet files using Dataproc Serverless.

Optimized for local Airflow development environment.
"""

import os
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# Local development configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "pipeline_config.yaml")
DEFAULT_ENV = "dev"

print(f"Base directory: {BASE_DIR}")
print(f"Config path: {CONFIG_PATH}")


def load_pipeline_config() -> Dict[str, Any]:
    """Load pipeline configuration from YAML file"""
    try:
        with open(CONFIG_PATH, 'r') as file:
            config = yaml.safe_load(file)
        print("✅ Configuration loaded successfully")
        return config
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {CONFIG_PATH}")
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")
    except yaml.YAMLError as e:
        print(f"❌ Error parsing YAML configuration: {e}")
        raise ValueError(f"Error parsing YAML configuration: {e}")


def get_environment_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get configuration for the current environment"""
    try:
        env = Variable.get("PIPELINE_ENV", default_value=DEFAULT_ENV)
    except:
        # Fallback for local development without Airflow Variables
        env = os.getenv("PIPELINE_ENV", DEFAULT_ENV)

    print(f"Using environment: {env}")

    if env not in config["environments"]:
        available_envs = list(config["environments"].keys())
        raise ValueError(f"Environment '{env}' not found. Available: {available_envs}")

    return config["environments"][env]


def validate_config(**context) -> Dict[str, Any]:
    """Validate and prepare configuration for the pipeline"""
    print("🔍 Validating configuration...")

    config = load_pipeline_config()
    env_config = get_environment_config(config)

    # Get table configuration
    table_config = config["tables"]["population_stats"]
    job_config = config["jobs"]["bronze_ingestion"]

    # Validate required fields
    required_fields = ["project_id", "region", "buckets", "dataproc"]
    for field in required_fields:
        if field not in env_config:
            raise ValueError(f"Required field '{field}' missing from environment configuration")

    # Prepare job parameters
    job_params = {
        "project_id": env_config["project_id"],
        "region": env_config["region"],
        "landing_bucket": env_config["buckets"]["landing"],
        "bronze_bucket": env_config["buckets"]["bronze"],
        "deps_bucket": env_config["buckets"]["deps"],
        "source_file": table_config["source_file"],
        "table_name": table_config["table_name"],
        "service_account": env_config["dataproc"]["service_account"],
        "subnet": env_config["dataproc"]["subnet"],
        "version": env_config["dataproc"]["version"],
        "spark_config": job_config["spark_config"],
        "timeout_minutes": job_config["timeout_minutes"]
    }

    print("✅ Configuration validation successful")
    print(f"Project: {job_params['project_id']}")
    print(f"Region: {job_params['region']}")
    print(f"Table: {job_params['table_name']}")

    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='job_params', value=job_params)

    return job_params


def create_dataproc_job_config(**context) -> Dict[str, Any]:
    """Create Dataproc job configuration"""
    print("🛠️ Creating Dataproc job configuration...")

    # Get job parameters from XCom
    job_params = context['task_instance'].xcom_pull(key='job_params')

    # Generate job ID
    execution_date = context['execution_date']
    job_id = f"bronze-ingestion-{execution_date.strftime('%Y%m%d-%H%M%S')}"

    print(f"Job ID: {job_id}")

    # Create job configuration for Dataproc Serverless
    job_config = {
        "reference": {"job_id": job_id},
        "placement": {
            "cluster_name": ""  # Empty for serverless
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{job_params['deps_bucket']}/bronze_ingestion.py",
            "args": [
                f"--project_id={job_params['project_id']}",
                f"--landing_bucket={job_params['landing_bucket']}",
                f"--bronze_bucket={job_params['bronze_bucket']}",
                f"--source_file={job_params['source_file']}",
                f"--table_name={job_params['table_name']}",
                f"--batch_date={execution_date.strftime('%Y-%m-%d')}",
                "--environment=airflow-local"
            ],
            "properties": job_params['spark_config']
        }
    }

    print("✅ Dataproc job configuration created")

    # Push to XCom for the Dataproc operator
    context['task_instance'].xcom_push(key='dataproc_job_config', value=job_config)
    context['task_instance'].xcom_push(key='job_id', value=job_id)

    return job_config


# Load initial configuration for DAG setup with error handling
try:
    initial_config = load_pipeline_config()
    env_config = get_environment_config(initial_config)
    airflow_config = env_config.get("airflow", {})
    project_id = env_config.get("project_id", "default-project")
    region = env_config.get("region", "us-central1")
    print("✅ DAG configuration loaded successfully")
except Exception as e:
    # Fallback configuration if config file is not available during DAG parsing
    print(f"⚠️ Warning: Could not load configuration during DAG parsing: {e}")
    print("Using fallback configuration for DAG definition")
    airflow_config = {
        "schedule_interval": None,  # Manual trigger for local dev
        "max_active_runs": 1,
        "catchup": False
    }
    project_id = "default-project"
    region = "us-central1"

# DAG Definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '1_bronze_only_pipeline_local',
    default_args=default_args,
    description='Bronze layer ingestion pipeline - Local Development',
    schedule_interval=airflow_config.get("schedule_interval", None),  # Manual trigger for dev
    max_active_runs=airflow_config.get("max_active_runs", 1),
    catchup=airflow_config.get("catchup", False),
    tags=['bronze', 'ingestion', 'dataproc', 'local-dev'],
)

# Task 1: Validate Configuration
validate_config_task = PythonOperator(
    task_id='validate_config',
    python_callable=validate_config,
    dag=dag,
)

# Task 2: Create Dataproc Job Configuration
create_job_config_task = PythonOperator(
    task_id='create_job_config',
    python_callable=create_dataproc_job_config,
    dag=dag,
)

# Task 3: Submit Dataproc Serverless Job
submit_bronze_job = DataprocSubmitJobOperator(
    task_id='submit_bronze_ingestion',
    job="{{ task_instance.xcom_pull(task_ids='create_job_config', key='dataproc_job_config') }}",
    location=region,
    project_id=project_id,
    dag=dag,
)

# Task Dependencies
validate_config_task >> create_job_config_task >> submit_bronze_job
"""
Dataproc Serverless Job Submission Script

This script reads configuration from YAML and submits Dataproc Serverless jobs
using the Dataproc REST API. It supports both bronze ingestion and silver
transformation jobs, with optional Iceberg catalog configuration for silver jobs.

Usage:
    python submit_dataproc_job.py \\
        --config config/pipeline_config.yaml \\
        --env dev \\
        --job-type bronze \\
        --table population_stats

    # Or import as module:
    from submit_dataproc_job import submit_dataproc_job
    result = submit_dataproc_job(
        config_path="config/pipeline_config.yaml",
        environment="dev",
        job_type="bronze",
        table_name="population_stats"
    )
"""

import argparse
import logging
import uuid
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import requests
from google.auth import default
from google.auth.transport.requests import Request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataprocJobSubmitter:
    """Handles Dataproc Serverless job submission via REST API"""

    def __init__(self, config_path: str, environment: str):
        """
        Initialize the submitter with configuration.

        Args:
            config_path: Path to pipeline_config.yaml
            environment: Environment name (dev, prod, etc.)
        """
        self.config_path = Path(config_path)
        self.environment = environment
        self.config = self._load_config()
        self.env_config = self.config['environments'][environment]

    def _load_config(self) -> Dict[str, Any]:
        """Load and parse YAML configuration"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        logger.info(f"Loaded configuration from {self.config_path}")
        return config

    def _get_auth_token(self) -> str:
        """
        Get authentication token for Google Cloud API.

        Returns:
            Bearer token string
        """
        credentials, project = default()
        credentials.refresh(Request())
        return credentials.token

    def _build_bronze_job_request(
        self,
        table_name: str,
        batch_id: str,
        additional_args: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Build Dataproc API request body for bronze ingestion job.

        Args:
            table_name: Name of the table to process
            batch_id: Unique batch ID for this job
            additional_args: Optional additional arguments for the PySpark script

        Returns:
            Request body dictionary for Dataproc API
        """
        table_config = self.config['tables'].get(table_name)
        if not table_config:
            raise ValueError(f"Table '{table_name}' not found in configuration")

        bronze_config = table_config.get('bronze', {})
        job_config = self.config['jobs'].get('bronze_ingestion', {})

        # Extract configuration values
        project_id = self.env_config['project_id']
        region = self.env_config['region']
        landing_bucket = self.env_config['buckets']['landing']
        bronze_bucket = self.env_config['buckets']['bronze']
        deps_bucket = self.env_config['buckets']['deps']
        source_file = table_config['source_file']
        service_account = self.env_config['dataproc']['service_account']
        subnet = self.env_config['dataproc']['subnet']
        version = self.env_config['dataproc']['version']

        # Build script arguments
        script_args = [
            f"--project_id={project_id}",
            f"--landing_bucket={landing_bucket}",
            f"--bronze_bucket={bronze_bucket}",
            f"--source_file={source_file}",
            f"--table_name={table_name}",
            "--environment=dataproc-api",
        ]

        # Add additional arguments if provided
        if additional_args:
            script_args.extend(additional_args)

        # Get Spark properties from job config
        spark_properties = job_config.get('spark_config', {})

        # Build request body
        request_body = {
            "runtimeConfig": {
                "version": version,
                "properties": spark_properties
            },
            "environmentConfig": {
                "executionConfig": {
                    "serviceAccount": service_account,
                    "subnetworkUri": f"projects/{project_id}/regions/{region}/subnetworks/{subnet}"
                }
            },
            "pysparkBatch": {
                "mainPythonFileUri": f"gs://{deps_bucket}/bronze_ingestion.py",
                "args": script_args,
                "jarFileUris": []
            },
            "labels": {
                "goog-dataproc-batch-id": batch_id,
                "job-type": "bronze-ingestion",
                "table": table_name,
                "environment": self.environment
            }
        }

        return request_body

    def _build_silver_job_request(
        self,
        table_name: str,
        batch_id: str,
        additional_args: Optional[list] = None,
        enable_iceberg: bool = True
    ) -> Dict[str, Any]:
        """
        Build Dataproc API request body for silver transformation job.

        Args:
            table_name: Name of the table to process
            batch_id: Unique batch ID for this job
            additional_args: Optional additional arguments for the PySpark script
            enable_iceberg: Whether to include Iceberg catalog configuration

        Returns:
            Request body dictionary for Dataproc API
        """
        table_config = self.config['tables'].get(table_name)
        if not table_config:
            raise ValueError(f"Table '{table_name}' not found in configuration")

        silver_config = table_config.get('silver', {})
        if not silver_config.get('enabled', False):
            raise ValueError(f"Silver transformation not enabled for table '{table_name}'")

        job_config = self.config['jobs'].get('silver_transformation', {})

        # Extract configuration values
        project_id = self.env_config['project_id']
        region = self.env_config['region']
        bronze_bucket = self.env_config['buckets']['bronze']
        silver_bucket = self.env_config['buckets']['silver']
        deps_bucket = self.env_config['buckets']['deps']
        silver_dataset = self.env_config['datasets']['silver']
        service_account = self.env_config['dataproc']['service_account']
        subnet = self.env_config['dataproc']['subnet']
        version = self.env_config['dataproc']['version']

        # Build script arguments
        script_args = [
            f"--project_id={project_id}",
            f"--bronze_bucket={bronze_bucket}",
            f"--silver_bucket={silver_bucket}",
            f"--silver_dataset={silver_dataset}",
            f"--table_name={table_name}",
            "--environment=dataproc-api",
        ]

        # Add additional arguments if provided
        if additional_args:
            script_args.extend(additional_args)

        # Get base Spark properties from job config
        spark_properties = job_config.get('spark_config', {}).copy()

        # Add Iceberg catalog configuration if enabled
        if enable_iceberg:
            # Assume BigQuery location is same as region (can be made configurable)
            bq_location = region
            warehouse_path = f"gs://{silver_bucket}/warehouse"

            iceberg_properties = {
                "spark.sql.catalog.icecat": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.icecat.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
                "spark.sql.catalog.icecat.gcp_project": project_id,
                "spark.sql.catalog.icecat.gcp_location": bq_location,
                "spark.sql.catalog.icecat.warehouse": warehouse_path,
            }

            spark_properties.update(iceberg_properties)
            logger.info(f"Iceberg catalog configured: warehouse={warehouse_path}")

        # Build JAR file URIs
        jar_uris = []
        if enable_iceberg:
            # Add BigQuery connector for Iceberg
            jar_uris.append("gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar")

        # Build request body
        request_body = {
            "runtimeConfig": {
                "version": version,
                "properties": spark_properties
            },
            "environmentConfig": {
                "executionConfig": {
                    "serviceAccount": service_account,
                    "subnetworkUri": f"projects/{project_id}/regions/{region}/subnetworks/{subnet}"
                }
            },
            "pysparkBatch": {
                "mainPythonFileUri": f"gs://{deps_bucket}/silver_transformation.py",
                "args": script_args,
                "jarFileUris": jar_uris
            },
            "labels": {
                "goog-dataproc-batch-id": batch_id,
                "job-type": "silver-transformation",
                "table": table_name,
                "environment": self.environment
            }
        }

        return request_body

    def submit_job(
        self,
        job_type: str,
        table_name: str,
        additional_args: Optional[list] = None,
        enable_iceberg: bool = True
    ) -> bool:
        """
        Submit a Dataproc Serverless job.

        Args:
            job_type: Type of job ("bronze" or "silver")
            table_name: Name of the table to process
            additional_args: Optional additional arguments for the PySpark script
            enable_iceberg: Whether to enable Iceberg for silver jobs

        Returns:
            True if submission successful, False otherwise
        """
        try:
            # Generate unique batch ID
            batch_id = f"dataproc-{job_type}-{table_name}-{uuid.uuid4().hex[:8]}"

            # Build request body based on job type
            if job_type == "bronze":
                request_body = self._build_bronze_job_request(
                    table_name=table_name,
                    batch_id=batch_id,
                    additional_args=additional_args
                )
            elif job_type == "silver":
                request_body = self._build_silver_job_request(
                    table_name=table_name,
                    batch_id=batch_id,
                    additional_args=additional_args,
                    enable_iceberg=enable_iceberg
                )
            else:
                raise ValueError(f"Invalid job type: {job_type}. Must be 'bronze' or 'silver'")

            # Get authentication token
            token = self._get_auth_token()

            # Build API URL
            project_id = self.env_config['project_id']
            region = self.env_config['region']
            url = (
                f"https://dataproc.googleapis.com/v1/"
                f"projects/{project_id}/locations/{region}/batches"
                f"?batchId={batch_id}"
            )

            # Prepare headers
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8"
            }

            # Submit the job
            logger.info(f"Submitting {job_type} job for table '{table_name}'...")
            logger.info(f"Batch ID: {batch_id}")
            logger.info(f"API URL: {url}")

            response = requests.post(url, headers=headers, json=request_body)

            if response.status_code != 200:
                logger.error(f"Dataproc submission failed: {response.text}")
                logger.error(f"Request body: {request_body}")
                return False

            logger.info(f"Dataproc job {batch_id} triggered successfully!")
            logger.info(f"Response: {response.json()}")
            return True

        except Exception as e:
            logger.error(f"Error triggering Dataproc: {e}")
            if 'request_body' in locals():
                logger.error(f"Request body detail: {request_body}")
            return False


def submit_dataproc_job(
    config_path: str,
    environment: str,
    job_type: str,
    table_name: str,
    additional_args: Optional[list] = None,
    enable_iceberg: bool = True
) -> bool:
    """
    Convenience function to submit a Dataproc job.

    Args:
        config_path: Path to pipeline_config.yaml
        environment: Environment name (dev, prod, etc.)
        job_type: Type of job ("bronze" or "silver")
        table_name: Name of the table to process
        additional_args: Optional additional arguments for the PySpark script
        enable_iceberg: Whether to enable Iceberg for silver jobs

    Returns:
        True if submission successful, False otherwise
    """
    submitter = DataprocJobSubmitter(config_path, environment)
    return submitter.submit_job(
        job_type=job_type,
        table_name=table_name,
        additional_args=additional_args,
        enable_iceberg=enable_iceberg
    )


def main():
    """Command-line interface for job submission"""
    parser = argparse.ArgumentParser(
        description="Submit Dataproc Serverless jobs using REST API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Submit bronze ingestion job
  python submit_dataproc_job.py \\
      --config config/pipeline_config.yaml \\
      --env dev \\
      --job-type bronze \\
      --table population_stats

  # Submit silver transformation job with Iceberg
  python submit_dataproc_job.py \\
      --config config/pipeline_config.yaml \\
      --env dev \\
      --job-type silver \\
      --table population_stats \\
      --enable-iceberg

  # Add custom arguments to PySpark script
  python submit_dataproc_job.py \\
      --config config/pipeline_config.yaml \\
      --env prod \\
      --job-type bronze \\
      --table population_stats \\
      --script-args "--batch_date=2025-01-01" "--full_refresh=true"
        """
    )

    parser.add_argument(
        '--config',
        required=True,
        help='Path to pipeline configuration YAML file'
    )

    parser.add_argument(
        '--env',
        required=True,
        help='Environment name (dev, prod, etc.)'
    )

    parser.add_argument(
        '--job-type',
        required=True,
        choices=['bronze', 'silver'],
        help='Type of job to submit'
    )

    parser.add_argument(
        '--table',
        required=True,
        help='Table name to process'
    )

    parser.add_argument(
        '--script-args',
        nargs='*',
        help='Additional arguments to pass to PySpark script'
    )

    parser.add_argument(
        '--enable-iceberg',
        action='store_true',
        default=True,
        help='Enable Iceberg catalog configuration for silver jobs (default: True)'
    )

    parser.add_argument(
        '--no-iceberg',
        action='store_true',
        help='Disable Iceberg catalog configuration for silver jobs'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine Iceberg setting
    enable_iceberg = args.enable_iceberg and not args.no_iceberg

    # Submit the job
    success = submit_dataproc_job(
        config_path=args.config,
        environment=args.env,
        job_type=args.job_type,
        table_name=args.table,
        additional_args=args.script_args,
        enable_iceberg=enable_iceberg
    )

    # Exit with appropriate status code
    exit(0 if success else 1)


if __name__ == "__main__":
    main()

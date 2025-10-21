# Dataproc Job Submission Script

## Overview

`submit_dataproc_job.py` is a Python utility that submits Dataproc Serverless jobs using the Dataproc REST API. It reads configuration from your YAML files and handles job submission with proper authentication and error handling.

## Features

- **YAML-driven configuration**: Reads from `pipeline_config.yaml`
- **Environment support**: Switch between dev, prod, etc.
- **Iceberg integration**: Automatically configures Iceberg catalog properties for silver jobs
- **Authentication**: Uses Google Application Default Credentials
- **Error handling**: Comprehensive logging and error reporting
- **CLI and module**: Can be used as a command-line tool or imported as a Python module

## Prerequisites

Install required dependencies:

```bash
pip install pyyaml requests google-auth
```

## Configuration

The script reads from `pipeline_config.yaml` with this structure:

```yaml
environments:
  dev:
    project_id: "your-project-id"
    region: "us-central1"
    buckets:
      landing: "landing-bucket"
      bronze: "bronze-bucket"
      silver: "silver-bucket"
      deps: "deps-bucket"
    datasets:
      silver: "silver"
    dataproc:
      service_account: "dataproc-sa@project.iam.gserviceaccount.com"
      subnet: "your-subnet"
      version: "2.1"

tables:
  population_stats:
    source_file: "global_population_stats_2024.csv"
    table_name: "population_stats"
    bronze:
      enabled: true
    silver:
      enabled: true

jobs:
  bronze_ingestion:
    spark_config:
      "spark.sql.adaptive.enabled": "true"
  silver_transformation:
    spark_config:
      "spark.sql.adaptive.enabled": "true"
```

## Usage

### Command Line

#### Submit Bronze Ingestion Job

```bash
python submit_dataproc_job.py \
    --config ../config/pipeline_config.yaml \
    --env dev \
    --job-type bronze \
    --table population_stats
```

#### Submit Silver Transformation Job with Iceberg

```bash
python submit_dataproc_job.py \
    --config ../config/pipeline_config.yaml \
    --env dev \
    --job-type silver \
    --table population_stats \
    --enable-iceberg
```

#### Add Custom Arguments to PySpark Script

```bash
python submit_dataproc_job.py \
    --config ../config/pipeline_config.yaml \
    --env prod \
    --job-type bronze \
    --table population_stats \
    --script-args "--batch_date=2025-01-01" "--full_refresh=true"
```

#### Disable Iceberg for Silver Job

```bash
python submit_dataproc_job.py \
    --config ../config/pipeline_config.yaml \
    --env dev \
    --job-type silver \
    --table population_stats \
    --no-iceberg
```

### As a Python Module

```python
from submit_dataproc_job import submit_dataproc_job

# Submit bronze job
success = submit_dataproc_job(
    config_path="../config/pipeline_config.yaml",
    environment="dev",
    job_type="bronze",
    table_name="population_stats"
)

# Submit silver job with Iceberg
success = submit_dataproc_job(
    config_path="../config/pipeline_config.yaml",
    environment="dev",
    job_type="silver",
    table_name="population_stats",
    enable_iceberg=True
)

# Submit with custom arguments
success = submit_dataproc_job(
    config_path="../config/pipeline_config.yaml",
    environment="prod",
    job_type="bronze",
    table_name="population_stats",
    additional_args=["--batch_date=2025-01-01", "--full_refresh=true"]
)
```

### Using the Class Directly

```python
from submit_dataproc_job import DataprocJobSubmitter

# Initialize submitter
submitter = DataprocJobSubmitter(
    config_path="../config/pipeline_config.yaml",
    environment="dev"
)

# Submit job
success = submitter.submit_job(
    job_type="bronze",
    table_name="population_stats",
    additional_args=["--batch_date=2025-01-01"]
)
```

## Iceberg Configuration

For silver transformation jobs, the script automatically configures Iceberg catalog properties:

```python
spark_properties = {
    "spark.sql.catalog.icecat": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.icecat.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
    "spark.sql.catalog.icecat.gcp_project": "your-project-id",
    "spark.sql.catalog.icecat.gcp_location": "us-central1",
    "spark.sql.catalog.icecat.warehouse": "gs://silver-bucket/warehouse"
}
```

This allows your PySpark job to write to Iceberg tables using BigQuery Metastore:

```python
# In your silver_transformation.py
spark.sql(f"USE `icecat`")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{dataset}`")

target = f"icecat.{dataset}.{table}"
df_silver.writeTo(target).append()
```

## Authentication

The script uses Google Application Default Credentials. Ensure you're authenticated:

```bash
# Option 1: Use gcloud auth
gcloud auth application-default login

# Option 2: Set service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

## Dataproc API Request Structure

The script constructs requests following this structure:

### Bronze Job Request

```json
{
  "runtimeConfig": {
    "version": "2.1",
    "properties": {
      "spark.sql.adaptive.enabled": "true"
    }
  },
  "environmentConfig": {
    "executionConfig": {
      "serviceAccount": "dataproc-sa@project.iam.gserviceaccount.com",
      "subnetworkUri": "projects/project/regions/us-central1/subnetworks/subnet"
    }
  },
  "pysparkBatch": {
    "mainPythonFileUri": "gs://deps-bucket/bronze_ingestion.py",
    "args": [
      "--project_id=project-id",
      "--landing_bucket=landing-bucket",
      "--bronze_bucket=bronze-bucket",
      "--source_file=data.csv",
      "--table_name=table_name",
      "--environment=dataproc-api"
    ],
    "jarFileUris": []
  },
  "labels": {
    "goog-dataproc-batch-id": "unique-batch-id",
    "job-type": "bronze-ingestion",
    "table": "table_name",
    "environment": "dev"
  }
}
```

### Silver Job Request (with Iceberg)

```json
{
  "runtimeConfig": {
    "version": "2.1",
    "properties": {
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.catalog.icecat": "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.icecat.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
      "spark.sql.catalog.icecat.gcp_project": "project-id",
      "spark.sql.catalog.icecat.gcp_location": "us-central1",
      "spark.sql.catalog.icecat.warehouse": "gs://silver-bucket/warehouse"
    }
  },
  "environmentConfig": {
    "executionConfig": {
      "serviceAccount": "dataproc-sa@project.iam.gserviceaccount.com",
      "subnetworkUri": "projects/project/regions/us-central1/subnetworks/subnet"
    }
  },
  "pysparkBatch": {
    "mainPythonFileUri": "gs://deps-bucket/silver_transformation.py",
    "args": [
      "--project_id=project-id",
      "--bronze_bucket=bronze-bucket",
      "--silver_bucket=silver-bucket",
      "--silver_dataset=silver",
      "--table_name=table_name",
      "--environment=dataproc-api"
    ],
    "jarFileUris": [
      "gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar"
    ]
  },
  "labels": {
    "goog-dataproc-batch-id": "unique-batch-id",
    "job-type": "silver-transformation",
    "table": "table_name",
    "environment": "dev"
  }
}
```

## Logging

The script provides detailed logging:

```
2025-01-21 10:15:23 - __main__ - INFO - Loaded configuration from ../config/pipeline_config.yaml
2025-01-21 10:15:24 - __main__ - INFO - Submitting bronze job for table 'population_stats'...
2025-01-21 10:15:24 - __main__ - INFO - Batch ID: dataproc-bronze-population_stats-a1b2c3d4
2025-01-21 10:15:24 - __main__ - INFO - API URL: https://dataproc.googleapis.com/v1/projects/...
2025-01-21 10:15:26 - __main__ - INFO - Dataproc job dataproc-bronze-population_stats-a1b2c3d4 triggered successfully!
```

Use `--verbose` flag for debug-level logging:

```bash
python submit_dataproc_job.py \
    --config ../config/pipeline_config.yaml \
    --env dev \
    --job-type bronze \
    --table population_stats \
    --verbose
```

## Error Handling

The script handles common errors:

- Missing configuration file
- Invalid environment or table name
- Authentication failures
- API errors (with full request body logging)
- Network issues

Example error output:

```
2025-01-21 10:15:26 - __main__ - ERROR - Dataproc submission failed: ...
2025-01-21 10:15:26 - __main__ - ERROR - Request body: {...}
```

## Integration with Airflow

You can use this script in Airflow DAGs:

```python
from airflow.operators.python import PythonOperator
from submit_dataproc_job import submit_dataproc_job

def submit_bronze_job(**context):
    success = submit_dataproc_job(
        config_path="/opt/airflow/dags/config/pipeline_config.yaml",
        environment="dev",
        job_type="bronze",
        table_name="population_stats"
    )
    if not success:
        raise Exception("Dataproc job submission failed")

submit_task = PythonOperator(
    task_id='submit_dataproc_job',
    python_callable=submit_bronze_job,
    dag=dag
)
```

## Comparison with gcloud Command

### Using gcloud (original approach from iceberg_setup.txt):

```bash
gcloud dataproc batches submit pyspark silver_job.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --deps-bucket=$DEPS_BUCKET \
  --version=2.2 \
  --properties="\
spark.sql.catalog.icecat=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.icecat.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog,\
spark.sql.catalog.icecat.gcp_project=$PROJECT_ID,\
spark.sql.catalog.icecat.gcp_location=$BQ_LOCATION,\
spark.sql.catalog.icecat.warehouse=gs://$SILVER_BUCKET/warehouse"
```

### Using this script (Python API approach):

```bash
python submit_dataproc_job.py \
    --config ../config/pipeline_config.yaml \
    --env dev \
    --job-type silver \
    --table population_stats
```

**Advantages of Python API approach:**
- All configuration in YAML (single source of truth)
- No need for environment variables
- Better error handling and logging
- Can be imported as a module
- Easier integration with Airflow and other Python tools
- Programmatic control over job submission

## Troubleshooting

### Issue: "Authentication failed"
**Solution**: Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`

### Issue: "Table not found in configuration"
**Solution**: Ensure the table is defined in the `tables` section of your YAML config

### Issue: "Silver transformation not enabled"
**Solution**: Set `silver.enabled: true` for the table in your YAML config

### Issue: "Dataproc submission failed: 403 Forbidden"
**Solution**: Ensure your service account has the `dataproc.batches.create` permission

### Issue: "Network error"
**Solution**: Check VPC configuration and firewall rules for Dataproc API access

## References

- [Dataproc REST API Documentation](https://cloud.google.com/dataproc/docs/reference/rest)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [BigQuery Metastore for Iceberg](https://cloud.google.com/bigquery/docs/iceberg-tables)

## License

MIT License - See project root for details

# Project overview

Build a demo of data pipelines in GCP that use Apache Iceberg, Airflow, Dataproc Serverless and BigQuery.

## Architecture

The simplified architecture is:

1. Landing Bucket: receives database extracts and diverse file types on a daily basis
2. Airflow launches a scheduled load job from Landing into Bronze
3. We use a data processing tool (could be Dataproc Serverless) to convert incoming data into Parquet
4. Parquet are appended into the Bronze Bucket under a folder name for the table in the format YYYY/MM/DD/
5. The pipeline then triggers a job to load into the Silver Area, which is Iceberg Tables in GCP (BigQuery managed Iceberg). We will have both a Silver bucket and the proper dataset for testing. The transformation is minimal
6. We apply some data quality rules into the Silver transformation process
7. The ingestion pattern into silver will be append.
8. All the stages are monitored in a monitoring_tracking table (name TBD) with timestamps, records created, status (SUCCESS, FAILED, etc), and batch_id of the extraction
9. Gold transformation should happen in BQ but it is TBD at this moment

## Testing

We will develop and test each component until we can integrate all the steps into a pipeline. As transformations will be done with Dataproc serverless, when we have defined variables that could change between jobs, those variables will be extracted to config files to use as input parameters (ie: csv file, target table, silver schema, etc.).

## Tech stack

We have the following tech stack:
- Composer
- Dataproc Serverless (2.1+)
- Google Cloud Storage
- BigQuery
- BigQuery Iceberg Managed tables
- Looker Studio (current data studio, not Looker)
- Cloud Logging
- Cloud Monitoring
- Python 3.12.3

## Project Structure

```
root/
|-  airflow/
|   |-  1_bronze_only_pipeline.py
|   |-  2_silver_only_pipeline.py
|   |-  3_bronze_silver_pipeline.py
|-  pyspark/
|   |-  bronze_ingestion.py
|   |-  silver_transformation.py
|-  config/
|   |-  pipeline_config.yaml
|-  requirements.txt
|-  sample_data/
|-  tests/
|-  ../env_iceberg/
|-  .env
```

Only in the local environment we have a python venv and the .env files.

## Project

We should use when possible variables, such as PROJECT_ID to avoid hard-coding values. The project used is considered a testing/development environment.

The code for the project is saved to github, so we should be careful with the names of resources.

We should also follow Iceberg practices, so it will probably be wise to use a folder for each table to keep the bronze layer organized.

## Locations

**Bronze:**
- $BRONZE_BUCKET

**Silver:**
- $SILVER_BUCKET
- $SILVER_DATASET

**Gold bucket:**
- $GOLD_BUCKET (will be in BQ so we don't know it a bucket will be necessary)
- $GOLD_DATASET

## Permissions

Service account for Cloud Composer:

```
gcloud iam service-accounts create composer-sa \
  --description="Service Account for Cloud Composer environment" \
  --display-name="composer-sa"
```

Permissions and roles:

The following permissions exist for the local testing account ("airflow-local") and the cloud composer account ("composer-sa"). We will develop using local airflow to avoid high costs, but the final test will be using an actual Cloud Composer environment.

$SA_NAME= depends on the stage of development

```
# Composer worker role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/composer.worker"

# Composer env + storage admin
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/composer.environmentAndStorageObjectAdmin"

# GCS object admin (for DAGs/logs/Xcom)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Cloud Logging writer
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter"

# (Optional but best practice) Allow it to use/impersonate other SAs
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

# Optional/ Could be in other SA being impersonated:

# BigQuery User
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# Dataproc Editor
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/dataproc.editor"

gcloud projects add-iam-policy-binding mindful-silo-469119-r1 \
  --member="serviceAccount:$DATAPROC_SA" \
  --role="roles/dataproc.editor"
```

## Orchestrator

Locally we can use Airflow which runs via Docker Compose. By default, using the port 8080.
We can place our DAGs in the airflow subfolder and a batch script will copy it to the real DAGs folder.

## Bronze Layer Ingestion

The bronze layer ingestion process reads raw data files from the landing bucket and converts them to Parquet format for storage in GCS. The implementation is in `pyspark/bronze_ingestion.py`.

**Process:**
1. Reads raw data files (CSV, JSON, etc.) from landing bucket
2. Applies minimal schema inference and type casting
3. Adds metadata columns: ingestion timestamp, source file name, batch date
4. Writes Parquet files to bronze bucket in partitioned format: `table_name/YYYY/MM/DD/`
5. Uses Spark for distributed processing via Dataproc Serverless

**Key Features:**
- Schema inference with configurable type overrides via YAML config files
- Partitioned storage by date for efficient querying
- Append-only writes with compression (snappy)
- Validation to ensure data was successfully loaded
- Configuration-driven approach for reusability across different data sources

**Configuration:**
Bronze ingestion jobs are parameterized using YAML files in `config/` directory. These define source schema, target table name, and transformation rules.

## Silver Layer Transformation

The silver layer transformation process reads bronze Parquet data, applies business transformations, and writes to either standard GCS Parquet or BigQuery-managed Iceberg tables. The implementation is in `pyspark/silver_transformation.py`.

**Process:**
1. Reads partitioned Parquet from bronze layer for specified batch date
2. Applies data quality rules and transformations (schema standardization, type casting, deduplication, filtering)
3. Adds silver metadata columns: processing timestamp, batch date
4. Validates transformed data using sampling-based approach (efficient, minimal resource usage)
5. Writes to target storage backend (GCS or BigLake Iceberg)

**Key Features:**
- Lazy evaluation throughout transformation pipeline for optimal performance
- Environment-aware execution: verbose debugging in dev/local, minimal actions in production
- Sample-based validation (reads only 10 records) instead of expensive full counts
- Single write action in production mode to minimize cost
- Schema validation for required metadata columns
- Metrics available via Spark UI and Dataproc monitoring

**Storage Backends:**

The silver layer supports two output modes:

1. **Standard GCS Parquet (default):** Writes Parquet files directly to GCS bucket with same partitioning strategy as bronze layer. Suitable for traditional Spark-based downstream processing.

2. **BigQuery-managed Iceberg (BigLake):** Writes Parquet files to GCS and automatically creates/updates BigQuery external Iceberg tables. This approach uses BigQuery's built-in Iceberg catalog (no Dataproc Metastore required, zero metastore cost). The table is queryable directly from BigQuery with full Iceberg features including time travel and schema evolution.

**BigLake Iceberg Implementation:**

When using BigLake Iceberg mode (`--use_iceberg` flag), the transformation automatically creates a BigQuery external table pointing to the Parquet data in GCS. The table uses:
- External BigQuery connection for BigLake (must be pre-configured)
- Iceberg table format with Parquet file format
- Storage URI pointing to silver bucket location
- Auto-generated schema from DataFrame with Spark-to-BigQuery type mapping

The implementation uses direct Parquet writes to GCS storage URI, not the Spark Iceberg catalog. This eliminates the need for expensive Dataproc Metastore services. BigQuery manages all Iceberg metadata internally through its own catalog.

**Performance Optimizations:**

The silver transformation implements several efficiency patterns:
- Avoids multiple DataFrame scans by using single write action
- Removes expensive count operations in production (uses Spark metrics instead)
- Implements sample-based validation for empty dataset detection
- Environment-based conditional execution for debugging features
- No caching required due to single-action pattern

See `pyspark/BIGLAKE_ICEBERG_SETUP.md` for detailed setup instructions including connection creation, permission grants, and usage examples.

**Configuration Parameters:**

Silver jobs accept command-line arguments for flexibility:
- Standard parameters: project ID, bucket names, table name, batch date, environment
- Iceberg parameters: dataset name, connection name, location (required when `--use_iceberg` is enabled)

Examples of both GCS and Iceberg usage patterns are documented in the module docstring of `silver_transformation.py`.

## Data Quality and Validation

Data quality checks are integrated into the transformation pipeline:
- Bronze layer validates successful file reads and non-empty datasets
- Silver layer validates schema presence, required metadata columns, and data existence
- Validation uses sampling approach to minimize performance impact
- Failed validations halt pipeline execution and log errors for monitoring

## Monitoring and Observability

Pipeline execution is monitored through multiple channels:
- Cloud Logging captures all job logs with severity levels (INFO, ERROR)
- Spark UI provides detailed execution metrics (record counts, task duration, resource usage)
- Dataproc metrics in Cloud Monitoring track job-level statistics
- Airflow DAG status tracks orchestration-level success/failure
- Future enhancement: dedicated monitoring table to track batch metadata, record counts, and execution status across pipeline stages
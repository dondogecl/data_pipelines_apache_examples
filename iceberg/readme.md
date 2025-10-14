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
7. All the stages are monitored in a monitoring_tracking table (name TBD) with timestamps, records created, status (SUCCESS, FAILED, etc), and batch_id of the extraction
8. Gold transformation should happen in BQ but it is TBD at this moment

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

Only in the local environment we have a python venv and the .env files.

## Locations

Bronze Bucket:

- gs://bronze-mindful-silo-469119-r1

Silver bucket:
- gs://silver-mindful-silo-469119-r1

Gold bucket:
- TBD (will be in BQ so we don't know it a bucket will be necessary)
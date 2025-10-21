# BigLake Iceberg Setup Guide

This guide explains how to configure and use BigQuery-managed BigLake Iceberg tables with the silver transformation pipeline.

## Overview

BigLake Iceberg is **BigQuery's fully-managed Iceberg implementation**:
- ✅ **No Dataproc Metastore needed** (no extra cost!)
- ✅ BigQuery manages all Iceberg metadata
- ✅ Spark writes Parquet → BigQuery reads as Iceberg
- ✅ Full Iceberg features: time travel, schema evolution, partition evolution

## Prerequisites

### 1. Install Required Python Package

```bash
pip install google-cloud-bigquery
```

### 2. Create BigQuery Connection for BigLake

```bash
# Set environment variables
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export CONNECTION_NAME="biglake-connection"

# Create connection
bq mk --connection \
  --location=$REGION \
  --project_id=$PROJECT_ID \
  --connection_type=CLOUD_RESOURCE \
  $CONNECTION_NAME

# Get the service account created by the connection
bq show --connection --format=json \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  $CONNECTION_NAME
```

### 3. Grant Permissions to Connection Service Account

The connection creates a service account (format: `bqcx-<PROJECT_NUM>-<HASH>@gcp-sa-bigquery-condel.iam.gserviceaccount.com`).

Grant it permissions to read/write GCS:

```bash
export CONNECTION_SERVICE_ACCOUNT="bqcx-xxx@gcp-sa-bigquery-condel.iam.gserviceaccount.com"
export SILVER_BUCKET="your-silver-bucket"

# Grant Storage Object Admin
gsutil iam ch \
  serviceAccount:$CONNECTION_SERVICE_ACCOUNT:roles/storage.objectAdmin \
  gs://$SILVER_BUCKET
```

### 4. Grant BigQuery Permissions to Job Service Account

Your Dataproc/job service account needs BigQuery permissions:

```bash
export JOB_SERVICE_ACCOUNT="your-job-sa@your-project.iam.gserviceaccount.com"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$JOB_SERVICE_ACCOUNT" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$JOB_SERVICE_ACCOUNT" \
  --role="roles/bigquery.connectionUser"
```

## Usage

### Option 1: Standard GCS Parquet (Default)

```bash
python silver_transformation.py \
  --project_id=$PROJECT_ID \
  --bronze_bucket=my-bronze-bucket \
  --silver_bucket=my-silver-bucket \
  --table_name=sensor_data \
  --batch_date=2024-01-15 \
  --environment=prod
```

**Writes to:** `gs://my-silver-bucket/sensor_data/2024/01/15/`

### Option 2: BigLake Iceberg

```bash
python silver_transformation.py \
  --project_id=$PROJECT_ID \
  --bronze_bucket=my-bronze-bucket \
  --silver_bucket=my-silver-bucket \
  --table_name=sensor_data \
  --use_iceberg \
  --bq_dataset=silver \
  --bq_connection=biglake-connection \
  --bq_location=us-central1 \
  --batch_date=2024-01-15 \
  --environment=prod
```

**What happens:**
1. Script auto-creates BigQuery table: `project.silver.sensor_data` (if not exists)
2. Table configured as Iceberg with storage: `gs://my-silver-bucket/silver/sensor_data/`
3. Spark writes Parquet to GCS
4. BigQuery automatically manages Iceberg metadata

### Option 3: Dataproc Serverless with BigLake Iceberg

```bash
gcloud dataproc batches submit pyspark silver_transformation.py \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --deps-bucket=my-deps-bucket \
  --version=2.2 \
  -- \
  --project_id=$PROJECT_ID \
  --bronze_bucket=my-bronze-bucket \
  --silver_bucket=my-silver-bucket \
  --table_name=sensor_data \
  --use_iceberg \
  --bq_dataset=silver \
  --bq_connection=biglake-connection \
  --bq_location=us-central1 \
  --environment=prod
```

## Querying BigLake Iceberg Tables

Once data is written, query from BigQuery:

```sql
-- Basic query
SELECT * FROM `project-id.silver.sensor_data` LIMIT 10;

-- Time travel (query as of specific time)
SELECT * FROM `project-id.silver.sensor_data`
FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00 UTC';

-- Check table metadata
SELECT * FROM `project-id.silver.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE table_name = 'sensor_data';
```

## Architecture

```
┌─────────────────┐
│  Bronze Layer   │
│  (GCS Parquet)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Spark Transformation   │
│  (silver_transformation │
│       .py)              │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  BigQuery DDL Execution │
│  CREATE TABLE IF NOT    │
│  EXISTS (Iceberg)       │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Write Parquet to GCS   │
│  gs://bucket/dataset/   │
│        table/           │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  BigQuery reads via     │
│  BigLake connection     │
│  (Iceberg metadata      │
│   managed by BigQuery)  │
└─────────────────────────┘
```

## Cost Comparison

### Standard GCS Parquet
- **Storage:** GCS Standard ($0.020/GB/month)
- **Compute:** Dataproc Serverless (~$0.056/vCPU-hour)
- **Total:** Storage + Compute

### BigLake Iceberg
- **Storage:** GCS Standard ($0.020/GB/month)
- **Compute:** Dataproc Serverless (~$0.056/vCPU-hour)
- **Metastore:** FREE (BigQuery-managed)
- **Total:** Storage + Compute

### Dataproc Metastore (NOT recommended for this use case)
- **Storage:** GCS Standard ($0.020/GB/month)
- **Compute:** Dataproc Serverless (~$0.056/vCPU-hour)
- **Metastore:** Dataproc Metastore ($0.30-$1.50/hour)
- **Total:** Storage + Compute + Metastore 💰💰💰

## Troubleshooting

### Error: "google-cloud-bigquery not available"

Install the package:
```bash
pip install google-cloud-bigquery
```

### Error: "Access Denied: BigQuery Connection Service Account"

Grant Storage Object Admin to the connection SA:
```bash
gsutil iam ch serviceAccount:CONNECTION_SA:roles/storage.objectAdmin gs://BUCKET
```

### Error: "Permission denied on BigQuery table"

Grant BigQuery permissions to your job service account:
```bash
gcloud projects add-iam-policy-binding PROJECT \
  --member="serviceAccount:JOB_SA" \
  --role="roles/bigquery.dataEditor"
```

### Table not appearing in BigQuery

1. Check connection exists: `bq show --connection CONNECTION_NAME`
2. Verify storage URI has data: `gsutil ls gs://bucket/dataset/table/`
3. Check BigQuery logs for errors

## Best Practices

1. **Use environment-based debugging:**
   - `--environment=local` or `--environment=dev`: Full validation with counts
   - `--environment=prod`: Minimal actions, rely on Spark UI metrics

2. **Partitioning:**
   - Consider partitioning by date in future iterations
   - BigLake Iceberg supports partition evolution

3. **Schema Evolution:**
   - BigQuery handles schema evolution
   - Add columns as needed, BigLake Iceberg supports it

4. **Monitoring:**
   - Check Spark UI for record counts
   - Use BigQuery's `INFORMATION_SCHEMA` for metadata
   - Set up Cloud Monitoring alerts

## Next Steps: Merge/Upsert

For incremental updates (future enhancement):

```python
# Future: MERGE support
# BigQuery supports MERGE for Iceberg tables
MERGE `project.silver.table` AS target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

This requires additional logic in the transformation pipeline but is fully supported by BigLake Iceberg.

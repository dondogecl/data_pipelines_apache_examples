# Configuration Guide

This guide explains how to configure the data pipeline using the provided templates.

## Quick Start

1. **Copy template files and customize them:**
   ```bash
   # Copy YAML template
   cp pyspark/bronze_job_template.yaml pyspark/bronze_job.yaml

   # Copy pipeline config template
   cp config/pipeline_config.yaml config/pipeline_config_local.yaml
   ```

2. **Replace placeholder values in your copied files**

3. **Set up Airflow environment variable:**
   ```bash
   # In Airflow UI or via CLI
   airflow variables set PIPELINE_ENV dev
   ```

## Configuration Files

### 1. Pipeline Configuration (`config/pipeline_config.yaml`)

**Main configuration file** that defines environments, tables, and job settings.

#### Key Sections:

- **`environments`**: Define dev/prod environment settings
- **`tables`**: Configure source files and table specifications
- **`jobs`**: Job-specific settings like Spark configuration
- **`monitoring`**: Alerting and logging configuration

#### Required Customizations:

Replace these placeholders with your actual values:

- `YOUR_DEV_PROJECT_ID` / `YOUR_PROD_PROJECT_ID`
- `YOUR_DATAPROC_SUBNET` / `YOUR_PROD_DATAPROC_SUBNET`
- `YOUR_EMAIL@example.com`
- `YOUR_SLACK_WEBHOOK_URL`

### 2. Dataproc Job Template (`pyspark/bronze_job_template.yaml`)

**Dataproc Serverless job specification** for direct job submission.

#### Required Customizations:

- `YOUR_PROJECT_ID`: Your GCP project ID
- `YOUR_DEPS_BUCKET`: Bucket for storing PySpark scripts
- `YOUR_LANDING_BUCKET`: Source data bucket
- `YOUR_BRONZE_BUCKET`: Target bronze layer bucket
- `YOUR_SOURCE_FILE.csv`: Source file name
- `YOUR_TABLE_NAME`: Target table name
- `YOUR_DATAPROC_SA`: Service account email
- `YOUR_REGION`: GCP region (e.g., us-central1)
- `YOUR_DATAPROC_SUBNET`: Subnet name

## Deployment Options

### Option 1: Airflow Orchestration (Recommended)

Uses the centralized `pipeline_config.yaml` and Airflow DAG:

1. **Configure**: Edit `config/pipeline_config.yaml`
2. **Deploy**: Place DAG in Airflow DAGs folder
3. **Run**: `airflow dags trigger 1_bronze_only_pipeline`

**Benefits:**
- ✅ Centralized configuration
- ✅ Scheduling and monitoring
- ✅ Error handling and retries
- ✅ Multi-environment support

### Option 2: Direct Script Submission

Uses bash scripts for direct job submission:

1. **Configure**: Edit variables in `submit_job.sh`
2. **Run**: `./submit_job.sh`

**Benefits:**
- ✅ Simple for testing
- ✅ Direct control
- ❌ No scheduling
- ❌ Limited error handling

### Option 3: YAML-based Submission

Uses the YAML template for job specification:

1. **Configure**: Edit `bronze_job.yaml`
2. **Run**: `./submit_yaml.sh`

**Benefits:**
- ✅ Declarative configuration
- ✅ Version control friendly
- ❌ Limited environment management

## Environment Setup

### 1. Required Environment Variables

Set these in your environment or Airflow:

```bash
# For bash scripts
export REGION="us-central1"
export DEPS_BUCKET="your-deps-bucket"
export DATAPROC_SA="your-sa@project.iam.gserviceaccount.com"
export DATAPROC_SUBNET="your-subnet"

# For Airflow (set as Airflow Variable)
PIPELINE_ENV=dev  # or prod
```

### 2. Required Permissions

Your service account needs these roles:

- `roles/dataproc.editor`
- `roles/storage.objectAdmin`
- `roles/logging.logWriter`
- `roles/bigquery.user` (for silver layer)

### 3. Required Infrastructure

- ✅ GCS buckets created (landing, bronze, silver, deps)
- ✅ VPC subnet configured
- ✅ Service account created with permissions
- ✅ Airflow environment deployed (for orchestration)

## Customization Examples

### Adding a New Table

Edit `config/pipeline_config.yaml`:

```yaml
tables:
  your_new_table:
    source_file: "your_data.csv"
    table_name: "your_table"
    bronze:
      enabled: true
      file_format: "csv"
      header: true
      infer_schema: true
    silver:
      enabled: true
      transformations:
        - "data_quality_checks"
```

### Environment-Specific Settings

```yaml
environments:
  staging:
    project_id: "your-staging-project"
    region: "us-west1"
    buckets:
      landing: "staging-landing-bucket"
      bronze: "staging-bronze-bucket"
    # ... other staging-specific settings
```

## Troubleshooting

### Common Issues:

1. **Configuration not found**
   - Check file paths in DAG configuration
   - Ensure config files are in the correct Airflow DAGs directory

2. **Permission errors**
   - Verify service account has required roles
   - Check bucket permissions

3. **Job submission failures**
   - Validate YAML syntax
   - Check Dataproc quotas and limits
   - Verify subnet and network configuration

### Debugging:

1. **Check Airflow logs** for configuration validation errors
2. **Review Dataproc job logs** in GCP Console
3. **Validate YAML files** using `yamllint` or online validators

## Security Best Practices

1. **Never commit files with real credentials** to version control
2. **Use Airflow Variables/Connections** for sensitive values
3. **Keep template files** with placeholder values in the repo
4. **Add real config files** to `.gitignore`

## Next Steps

1. **Test bronze ingestion** with your configuration
2. **Implement silver transformation** layer
3. **Add monitoring and alerting**
4. **Set up production deployment**
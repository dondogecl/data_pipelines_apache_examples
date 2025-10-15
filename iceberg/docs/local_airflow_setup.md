# Local Airflow Setup for GCP Dataproc

This guide explains how to run the bronze ingestion pipeline from local Airflow to GCP Dataproc Serverless.

## Prerequisites

### 1. Install Required Packages

```bash
# Install Airflow with Google Cloud provider
pip install apache-airflow apache-airflow-providers-google pyyaml

# Or install from requirements
pip install -r requirements_airflow.txt
```

### 2. GCP Authentication

Choose one of these authentication methods:

#### Option A: Service Account Key (Recommended for Dev)
```bash
# Download service account key from GCP Console
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

#### Option B: Application Default Credentials
```bash
gcloud auth application-default login
```

### 3. Required GCP Permissions

Your service account needs these roles:
- `roles/dataproc.editor`
- `roles/storage.objectAdmin`
- `roles/iam.serviceAccountUser`

## Configuration Setup

### 1. Copy and Customize Configuration

```bash
# Copy template configuration
cp config/pipeline_config.yaml config/pipeline_config_local.yaml

# Edit the local configuration
nano config/pipeline_config_local.yaml
```

### 2. Set Environment Variables

```bash
# Set pipeline environment
export PIPELINE_ENV=dev

# Optional: Set custom config path
export AIRFLOW_CONFIG_PATH="/path/to/your/config/pipeline_config_local.yaml"
```

### 3. Initialize Airflow Database

```bash
# Initialize Airflow (first time only)
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

## Running the Pipeline

### 1. Start Airflow Services

```bash
# Terminal 1: Start webserver
airflow webserver --port 8080

# Terminal 2: Start scheduler
airflow scheduler
```

### 2. Access Airflow UI

Open http://localhost:8080 in your browser

### 3. Configure DAG

1. **Upload bronze_ingestion.py** to your deps bucket:
   ```bash
   gsutil cp pyspark/bronze_ingestion.py gs://your-deps-bucket/
   ```

2. **Set Airflow Variables** (optional, can use environment variables instead):
   - Go to Admin > Variables in Airflow UI
   - Add: `PIPELINE_ENV` = `dev`

### 4. Trigger the DAG

```bash
# Via CLI
airflow dags trigger 1_bronze_only_pipeline_local

# Or use the Airflow UI
```

## Troubleshooting

### Common Issues

#### 1. Configuration Not Found
```
FileNotFoundError: Configuration file not found
```

**Solution**: Check that config file exists and path is correct:
```bash
ls -la config/pipeline_config.yaml
export AIRFLOW_CONFIG_PATH="/full/path/to/config/pipeline_config.yaml"
```

#### 2. Authentication Errors
```
google.auth.exceptions.DefaultCredentialsError
```

**Solution**: Set up authentication:
```bash
# Check current auth
gcloud auth list

# Re-authenticate if needed
gcloud auth application-default login
```

#### 3. Permission Denied
```
Permission denied on resource 'projects/PROJECT_ID/locations/REGION/batches'
```

**Solution**: Check service account roles:
```bash
# List current roles
gcloud projects get-iam-policy PROJECT_ID

# Add required role
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/dataproc.editor"
```

#### 4. Import Errors
```
ModuleNotFoundError: No module named 'airflow.providers.google'
```

**Solution**: Install Google Cloud provider:
```bash
pip install apache-airflow-providers-google
```

### Debugging Steps

1. **Check Airflow Logs**:
   ```bash
   # DAG logs
   tail -f ~/airflow/logs/1_bronze_only_pipeline_local/*/2024-*/*.log

   # Scheduler logs
   tail -f ~/airflow/logs/scheduler/latest/*.log
   ```

2. **Test Configuration Loading**:
   ```python
   import yaml
   with open('config/pipeline_config.yaml', 'r') as f:
       config = yaml.safe_load(f)
   print(config)
   ```

3. **Test GCP Authentication**:
   ```bash
   gcloud dataproc batches list --region=us-central1
   ```

## Local Development Features

The local DAG (`1_bronze_only_pipeline_local.py`) includes:

- ✅ **Enhanced logging** for debugging
- ✅ **Fallback configuration** if YAML is missing
- ✅ **Environment variable support**
- ✅ **Manual trigger** (no automatic scheduling)
- ✅ **Local path resolution**

## Production Deployment

When ready for production:

1. **Use the production DAG**: `1_bronze_only_pipeline.py`
2. **Deploy to Cloud Composer** or managed Airflow
3. **Use Airflow Variables/Connections** for sensitive data
4. **Enable scheduling** by setting proper intervals

## Example Local Workflow

```bash
# 1. Setup
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa-key.json"
export PIPELINE_ENV=dev

# 2. Start Airflow
airflow webserver --port 8080 &
airflow scheduler &

# 3. Upload script to GCS
gsutil cp pyspark/bronze_ingestion.py gs://your-deps-bucket/

# 4. Trigger DAG
airflow dags trigger 1_bronze_only_pipeline_local

# 5. Monitor
airflow dags state 1_bronze_only_pipeline_local
```

This setup allows you to develop and test your data pipeline locally while leveraging GCP's scalable Dataproc Serverless infrastructure! 🚀
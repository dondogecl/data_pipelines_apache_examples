#!/bin/bash

# Generate clean job ID and timestamp
TIMESTAMP=$(date +%Y%m%d%H%M%S)
JOB_ID="dataproc-serverless-bronze-${TIMESTAMP}"

echo "Submitting bronze ingestion job with ID: $JOB_ID"
echo "Using YAML configuration: bronze_job.yaml"

# Create temporary YAML with substituted values
TEMP_YAML="/tmp/bronze_job_${TIMESTAMP}.yaml"

# Substitute placeholders in YAML
sed "s#TIMESTAMP/${TIMESTAMP}/g; \
     s#DEPS_BUCKET/${DEPS_BUCKET}/g; \
     s#DATAPROC_SA/${DATAPROC_SA}/g; \
     s#REGION/${REGION}/g; \
     s#DATAPROC_SUBNET/${DATAPROC_SUBNET}/g" \
     bronze_job.yaml > ${TEMP_YAML}

# Submit the job using YAML
gcloud dataproc batches submit --from-file ${TEMP_YAML} --region=${REGION}

# Clean up temporary file
rm ${TEMP_YAML}

echo "Job submitted with ID: $JOB_ID"
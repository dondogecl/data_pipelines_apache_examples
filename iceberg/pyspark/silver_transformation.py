"""
Silver Layer Transformation for Data Pipeline

This script transforms bronze layer data to silver layer with support for both:
1. Standard GCS Parquet writes
2. BigQuery-managed BigLake Iceberg tables

Usage Examples:

1. Standard GCS Parquet write:
   python silver_transformation.py \
     --project_id=my-project \
     --bronze_bucket=my-bronze-bucket \
     --silver_bucket=my-silver-bucket \
     --table_name=sensor_data \
     --batch_date=2024-01-15 \
     --environment=prod

2. BigLake Iceberg write:
   python silver_transformation.py \
     --project_id=my-project \
     --bronze_bucket=my-bronze-bucket \
     --silver_bucket=my-silver-bucket \
     --table_name=sensor_data \
     --use_iceberg \
     --bq_dataset=silver \
     --bq_connection=my-biglake-connection \
     --bq_location=us-central1 \
     --environment=prod

Note: For BigLake Iceberg, ensure you have:
- BigQuery connection created for BigLake
- Service account with BigQuery table creation permissions
- google-cloud-bigquery Python library installed
"""

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery client for BigLake Iceberg table management
try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    logger.warning("google-cloud-bigquery not available. Iceberg writes will not work.")


@dataclass
class SilverConfig:
    """Configuration for silver layer transformation"""
    project_id: str
    bronze_bucket: str
    silver_bucket: str
    table_name: str
    batch_date: Optional[str] = None
    environment: Optional[str] = None

    # BigLake Iceberg configuration
    use_iceberg: bool = False
    bq_dataset: Optional[str] = None
    bq_connection: Optional[str] = None
    bq_location: Optional[str] = None

    def __post_init__(self):
        if self.batch_date is None:
            self.batch_date = datetime.now().strftime("%Y-%m-%d")

        # Validate Iceberg configuration
        if self.use_iceberg:
            if not self.bq_dataset:
                raise ValueError("bq_dataset is required when use_iceberg=True")
            if not self.bq_connection:
                raise ValueError("bq_connection is required when use_iceberg=True")
            if not self.bq_location:
                raise ValueError("bq_location is required when use_iceberg=True")

    @property
    def source_path(self) -> str:
        """Path to bronze layer data"""
        year, month, day = self.batch_date.split("-")
        return f"gs://{self.bronze_bucket}/{self.table_name}/{year}/{month}/{day}/"

    @property
    def target_path(self) -> str:
        """Path to silver layer data"""
        year, month, day = self.batch_date.split("-")
        return f"gs://{self.silver_bucket}/{self.table_name}/{year}/{month}/{day}/"


def get_spark_session(app_name: str = "SilverTransformation") -> SparkSession:
    """Create and configure Spark session for Dataproc Serverless"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def validate_config(config: SilverConfig) -> None:
    """Validate silver transformation configuration parameters"""
    if not config.project_id:
        raise ValueError("project_id is required")
    if not config.bronze_bucket:
        raise ValueError("bronze_bucket is required")
    if not config.silver_bucket:
        raise ValueError("silver_bucket is required")
    if not config.table_name:
        raise ValueError("table_name is required")


def read_bronze_data(spark: SparkSession, source_path: str) -> DataFrame:
    """Read Parquet files from bronze layer"""
    logger.info(f"Reading bronze data from {source_path}")

    df = spark.read.parquet(source_path)

    # Validation removed - let downstream operations handle empty datasets
    # Spark will handle missing files/empty paths appropriately
    logger.info(f"Successfully configured read from bronze layer")
    return df

def apply_transformations(df: DataFrame, table_name: str) -> DataFrame:
    """
    Apply silver layer transformations to the dataframe.

    This is where you customize your transformation logic based on your schema.
    Replace these example transformations with your actual sensor data transformations.

    Common transformations:
    - Cast columns to proper data types
    - Rename columns to standard naming conventions
    - Filter out invalid/null records
    - Add derived/calculated columns
    - Remove duplicate records
    """
    logger.info(f"Applying transformations for {table_name}")

    # Example transformation pattern - customize for your schema
    # =========================================================

    # 1. Remove records with null key fields (customize field names)
    # df = df.filter(F.col("some_key_field").isNotNull())

    # 2. Cast columns to proper types (customize column names and types)
    # df = df.withColumn("sensor_value", F.col("sensor_value").cast(DoubleType()))
    # df = df.withColumn("sensor_id", F.col("sensor_id").cast(StringType()))

    # 3. Rename columns to standardized names (customize mappings)
    # df = df.withColumnRenamed("old_name", "new_name")

    # 4. Add calculated/derived columns (customize logic)
    # df = df.withColumn("value_in_celsius", (F.col("value_fahrenheit") - 32) * 5/9)

    # 5. Remove duplicates based on key columns (customize key columns)
    # df = df.dropDuplicates(["sensor_id", "timestamp"])

    # Placeholder: For now, just pass through the data
    # Replace this with your actual transformations
    transformed_df = df

    logger.info("Transformations applied successfully")

    return transformed_df


def add_silver_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    """Add silver layer metadata columns"""
    logger.info("Adding silver metadata columns")

    return df \
        .withColumn("silver_processed_timestamp", F.current_timestamp()) \
        .withColumn("silver_batch_date", F.lit(batch_date))


def validate_transformed_data(df: DataFrame, table_name: str) -> None:
    """Validate transformed data before writing to silver using sampling"""

    # Use sampling instead of full count - only reads first 10 records
    sample = df.limit(10).collect()

    if len(sample) == 0:
        raise ValueError(f"No records after transformation for {table_name}")

    # Validate schema - this is free (metadata only)
    required_columns = ["silver_processed_timestamp", "silver_batch_date"]
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required silver metadata columns: {missing}")

    logger.info(f"Validation passed: sample of {len(sample)} records validated with correct schema")


def log_transformation_stats(df: DataFrame, source_path: str, target_path: str, environment: Optional[str] = None) -> None:
    """Log transformation statistics with environment-based verbosity"""
    logger.info("Silver layer schema:")
    df.printSchema()  # Free - metadata only

    logger.info(f"Source (Bronze): {source_path}")
    logger.info(f"Target (Silver): {target_path}")

    # Environment-based deep validation - only in dev/local
    if environment in ["local", "dev"]:
        logger.info("=== DEBUG MODE: Running expensive validations ===")
        record_count = df.count()
        logger.info(f"Record count: {record_count}")
        logger.info("Sample transformed data (first 5 rows):")
        df.show(5, truncate=False)
    else:
        logger.info("Production mode: Skipping count/show operations.")
        logger.info("Check Spark UI or Dataproc metrics for record counts and execution details.")


def write_silver_data(df: DataFrame, target_path: str) -> None:
    """Write transformed data to silver layer in Parquet format"""
    logger.info(f"Writing silver data to {target_path}")

    df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .parquet(target_path)

    logger.info("Successfully wrote silver data")


def map_spark_type_to_bq(spark_field: StructField) -> str:
    """
    Map Spark data types to BigQuery data types for Iceberg table creation.

    Args:
        spark_field: Spark StructField with name and data type

    Returns:
        BigQuery type string (e.g., 'STRING', 'INT64', 'TIMESTAMP')
    """
    type_name = spark_field.dataType.typeName()

    type_mapping = {
        'string': 'STRING',
        'integer': 'INT64',
        'long': 'INT64',
        'bigint': 'INT64',
        'short': 'INT64',
        'byte': 'INT64',
        'float': 'FLOAT64',
        'double': 'FLOAT64',
        'decimal': 'NUMERIC',
        'boolean': 'BOOL',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE',
        'binary': 'BYTES',
    }

    bq_type = type_mapping.get(type_name, 'STRING')  # Default to STRING for unknown types

    # Handle nullability
    mode = "NULLABLE" if spark_field.nullable else "REQUIRED"

    return bq_type, mode


def create_biglake_table_if_not_exists(df: DataFrame, config: SilverConfig) -> None:
    """
    Create BigQuery-managed BigLake Iceberg table if it doesn't exist.

    Uses BigQuery Python client to execute DDL for table creation.
    The table uses external Iceberg format with Parquet files in GCS.

    Args:
        df: DataFrame with the schema to create
        config: SilverConfig with BigQuery and Iceberg settings
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery is required for Iceberg writes. "
                         "Install with: pip install google-cloud-bigquery")

    logger.info(f"Creating BigLake Iceberg table if not exists: {config.project_id}.{config.bq_dataset}.{config.table_name}")

    # Build storage URI
    storage_uri = f"gs://{config.silver_bucket}/{config.bq_dataset}/{config.table_name}"

    # Generate schema from DataFrame
    schema_fields = []
    for field in df.schema.fields:
        bq_type, mode = map_spark_type_to_bq(field)
        schema_fields.append(f"  {field.name} {bq_type}")

    schema_ddl = ",\n".join(schema_fields)

    # Build full table reference
    full_table_name = f"`{config.project_id}`.`{config.bq_dataset}`.`{config.table_name}`"
    full_connection = f"`{config.project_id}`.`{config.bq_location}`.`{config.bq_connection}`"

    # Create table DDL
    ddl = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
)
WITH CONNECTION {full_connection}
OPTIONS (
  table_format = 'ICEBERG',
  file_format = 'PARQUET',
  storage_uri = '{storage_uri}'
)
"""

    logger.info(f"Executing BigQuery DDL:\n{ddl}")

    # Execute DDL using BigQuery client
    client = bigquery.Client(project=config.project_id)

    try:
        query_job = client.query(ddl)
        query_job.result()  # Wait for completion
        logger.info(f"Successfully created/verified BigLake Iceberg table: {full_table_name}")
    except Exception as e:
        logger.error(f"Failed to create BigLake Iceberg table: {str(e)}")
        raise


def write_silver_biglake_iceberg(df: DataFrame, config: SilverConfig) -> None:
    """
    Write transformed data to BigQuery-managed BigLake Iceberg table.

    This function:
    1. Creates the BigLake Iceberg table in BigQuery if it doesn't exist
    2. Writes Parquet data to the GCS storage_uri
    3. BigQuery automatically manages Iceberg metadata

    Args:
        df: Transformed DataFrame to write
        config: SilverConfig with BigQuery and Iceberg settings
    """
    logger.info(f"Writing to BigLake Iceberg table: {config.bq_dataset}.{config.table_name}")

    # Step 1: Ensure table exists in BigQuery
    create_biglake_table_if_not_exists(df, config)

    # Step 2: Write Parquet to storage URI
    storage_uri = f"gs://{config.silver_bucket}/{config.bq_dataset}/{config.table_name}"
    logger.info(f"Writing Parquet data to storage_uri: {storage_uri}")

    df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .parquet(storage_uri)

    logger.info(f"Successfully wrote data to BigLake Iceberg table: {config.bq_dataset}.{config.table_name}")
    logger.info(f"Query in BigQuery: SELECT * FROM `{config.project_id}.{config.bq_dataset}.{config.table_name}` LIMIT 10")


def transform_to_silver(spark: SparkSession, config: SilverConfig) -> bool:
    """
    Main transformation function - orchestrates the silver layer transformation

    Args:
        spark: SparkSession instance
        config: SilverConfig with all required parameters

    Returns:
        bool: Success status
    """
    logger.info(f"Starting silver transformation from {config.source_path} to {config.target_path}")

    try:
        # Validate configuration
        validate_config(config)

        # Read bronze data (lazy)
        df = read_bronze_data(spark, config.source_path)

        # Apply transformations (lazy)
        transformed_df = apply_transformations(df, config.table_name)

        # Add silver metadata (lazy)
        df_with_metadata = add_silver_metadata(transformed_df, config.batch_date)

        # Validate transformed data (sample-based - minimal action)
        validate_transformed_data(df_with_metadata, config.table_name)

        # Log statistics (environment-aware)
        log_transformation_stats(
            df_with_metadata,
            config.source_path,
            config.target_path,
            config.environment
        )

        # ONLY FULL ACTION: Write to silver layer
        if config.use_iceberg:
            write_silver_biglake_iceberg(df_with_metadata, config)
        else:
            write_silver_data(df_with_metadata, config.target_path)

        logger.info("Silver transformation completed successfully.")
        logger.info("Check Spark UI or Dataproc metrics for record counts and execution details.")
        return True

    except Exception as e:
        logger.error(f"Error during silver transformation: {str(e)}")
        return False


def main() -> int:
    """Main function for silver layer transformation"""
    parser = argparse.ArgumentParser(description="Silver Layer Data Transformation")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--bronze_bucket", required=True, help="Bronze bucket name")
    parser.add_argument("--silver_bucket", required=True, help="Silver bucket name")
    parser.add_argument("--table_name", required=True, help="Table name")
    parser.add_argument("--batch_date", help="Batch date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--environment", help="Environment identifier (local, dev, prod)")

    # BigLake Iceberg options
    parser.add_argument("--use_iceberg", action="store_true", default=False,
                       help="Enable BigLake Iceberg table writes (default: False)")
    parser.add_argument("--bq_dataset", help="BigQuery dataset name (required if --use_iceberg)")
    parser.add_argument("--bq_connection", help="BigQuery connection name for BigLake (required if --use_iceberg)")
    parser.add_argument("--bq_location", help="BigQuery location/region (required if --use_iceberg)")

    args = parser.parse_args()

    # Debug environment
    logger.info(f"Environment: {args.environment}")

    try:
        # Create configuration object
        config = SilverConfig(
            project_id=args.project_id,
            bronze_bucket=args.bronze_bucket,
            silver_bucket=args.silver_bucket,
            table_name=args.table_name,
            batch_date=args.batch_date,
            environment=args.environment,
            use_iceberg=args.use_iceberg,
            bq_dataset=args.bq_dataset,
            bq_connection=args.bq_connection,
            bq_location=args.bq_location
        )

        # Get Spark session
        spark = get_spark_session()

        try:
            # Perform transformation
            success = transform_to_silver(spark, config)

            if success:
                logger.info("Silver transformation job completed successfully.")
                return 0
            else:
                logger.error("Silver transformation failed")
                return 1

        finally:
            spark.stop()

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

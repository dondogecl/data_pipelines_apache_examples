import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    """Configuration for bronze layer ingestion"""
    project_id: str
    landing_bucket: str
    bronze_bucket: str
    source_file: str
    table_name: str
    batch_date: Optional[str] = None
    environment: Optional[str] = None

    def __post_init__(self):
        if self.batch_date is None:
            self.batch_date = datetime.now().strftime("%Y-%m-%d")

    @property
    def source_path(self) -> str:
        return f"gs://{self.landing_bucket}/{self.source_file}"

    @property
    def target_path(self) -> str:
        year, month, day = self.batch_date.split("-")
        return f"gs://{self.bronze_bucket}/{self.table_name}/{year}/{month}/{day}/"

def get_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    """Create and configure Spark session for Dataproc Serverless"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def validate_config(config: IngestionConfig) -> None:
    """Validate ingestion configuration parameters"""
    if not config.project_id:
        raise ValueError("project_id is required")
    if not config.landing_bucket:
        raise ValueError("landing_bucket is required")
    if not config.bronze_bucket:
        raise ValueError("bronze_bucket is required")
    if not config.source_file:
        raise ValueError("source_file is required")
    if not config.table_name:
        raise ValueError("table_name is required")


def read_csv_file(spark: SparkSession, source_path: str) -> DataFrame:
    """Read CSV file with header inference"""
    logger.info(f"Reading CSV file from {source_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)

    logger.info(f"Successfully read CSV with {df.count()} records")
    return df


def add_metadata_columns(df: DataFrame, source_path: str, batch_date: str) -> DataFrame:
    """Add standard metadata columns to dataframe"""
    logger.info("Adding metadata columns")

    return df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", F.lit(source_path.split("/")[-1])) \
        .withColumn("batch_date", F.lit(batch_date))


def validate_dataframe(df: DataFrame, table_name: str) -> None:
    """Basic validation of dataframe before processing"""
    record_count = df.count()

    if record_count == 0:
        raise ValueError(f"No records found in {table_name}")

    logger.info(f"Validation passed: {record_count} records for {table_name}")


def log_ingestion_stats(df: DataFrame, source_path: str, target_path: str) -> int:
    """Log ingestion statistics and return record count"""
    logger.info("Schema:")
    df.printSchema()

    record_count = df.count()
    logger.info(f"Record count: {record_count}")
    logger.info(f"Source: {source_path}")
    logger.info(f"Target: {target_path}")
    logger.info("Sample data (first 5 rows):")
    df.show(5, truncate=False)

    return record_count


def write_parquet(df: DataFrame, target_path: str) -> None:
    """Write dataframe to Parquet format in GCS"""
    logger.info(f"Writing Parquet to {target_path}")

    df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .parquet(target_path)

    logger.info("Successfully wrote Parquet file")


def ingest_csv_to_bronze(spark: SparkSession, config: IngestionConfig) -> Tuple[bool, int]:
    """
    Main ingestion function - orchestrates the bronze layer ingestion process

    Args:
        spark: SparkSession instance
        config: IngestionConfig with all required parameters

    Returns:
        Tuple of (success: bool, record_count: int)
    """
    logger.info(f"Starting bronze ingestion from {config.source_path} to {config.target_path}")

    try:
        # Validate configuration
        validate_config(config)

        # Read source CSV file
        df = read_csv_file(spark, config.source_path)

        # Validate data
        validate_dataframe(df, config.table_name)

        # Add metadata columns
        df_with_metadata = add_metadata_columns(df, config.source_path, config.batch_date)

        # Log statistics
        record_count = log_ingestion_stats(df_with_metadata, config.source_path, config.target_path)

        # Write to bronze layer
        write_parquet(df_with_metadata, config.target_path)

        logger.info(f"Bronze ingestion completed successfully. Processed {record_count} records.")
        return True, record_count

    except Exception as e:
        logger.error(f"Error during bronze ingestion: {str(e)}")
        return False, 0


def main() -> int:
    """Main function for bronze ingestion"""
    parser = argparse.ArgumentParser(description="Bronze Layer CSV Ingestion")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--landing_bucket", required=True, help="Landing bucket name")
    parser.add_argument("--bronze_bucket", required=True, help="Bronze bucket name")
    parser.add_argument("--source_file", default="global_population_stats_2024.csv", help="Source CSV file name")
    parser.add_argument("--table_name", required=True, help="Table name for organization")
    parser.add_argument("--batch_date", help="Batch date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--environment", help="Helps identify the environment of execution (local, cloud)")

    args = parser.parse_args()

    # Debug environment
    logger.info(f"Environment: {args.environment}")

    try:
        # Create configuration object
        config = IngestionConfig(
            project_id=args.project_id,
            landing_bucket=args.landing_bucket,
            bronze_bucket=args.bronze_bucket,
            source_file=args.source_file,
            table_name=args.table_name,
            batch_date=args.batch_date,
            environment=args.environment
        )

        # Get Spark session
        spark = get_spark_session()

        try:
            # Perform ingestion
            success, record_count = ingest_csv_to_bronze(spark, config)

            if success:
                logger.info(f"Bronze ingestion completed successfully. Processed {record_count} records.")
                return 0
            else:
                logger.error("Bronze ingestion failed")
                return 1

        finally:
            spark.stop()

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
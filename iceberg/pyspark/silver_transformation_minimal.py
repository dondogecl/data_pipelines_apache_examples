"""
Minimal Silver Transformation - Proof of Concept

This is a simplified version of silver_transformation.py designed for:
- Easy understanding for new team members
- Proof of concept for Iceberg integration
- Learning the basic transformation pattern

It focuses ONLY on:
1. Reading from Bronze (Parquet)
2. Applying basic transformations
3. Simple validation
4. Writing to Iceberg tables in BigQuery

For production use, see silver_transformation.py which includes:
- Multiple output modes (Parquet + Iceberg)
- BigQuery table auto-creation
- Environment-based features
- Comprehensive error handling

Usage:
    # Iceberg catalog properties should be configured via Dataproc submit:
    gcloud dataproc batches submit pyspark silver_transformation_minimal.py \\
      --version=2.2 \\
      --properties="\\
    spark.sql.catalog.icecat=org.apache.iceberg.spark.SparkCatalog,\\
    spark.sql.catalog.icecat.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog,\\
    spark.sql.catalog.icecat.gcp_project=YOUR_PROJECT,\\
    spark.sql.catalog.icecat.gcp_location=us-central1,\\
    spark.sql.catalog.icecat.warehouse=gs://YOUR_BUCKET/warehouse" \\
      -- \\
      --bronze_path=gs://bronze-bucket/population_stats/2025/01/21/ \\
      --catalog=icecat \\
      --database=silver \\
      --table=population_stats

    Or use submit_dataproc_job.py which handles this automatically!
"""

import argparse
import logging
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# SPARK SESSION
# ============================================================================
def get_spark_session() -> SparkSession:
    """
    Create Spark session.

    Note: Iceberg catalog properties should be configured via Dataproc submit
    or in your job configuration, not here.
    """
    spark = SparkSession.builder \
        .appName("SilverTransformationMinimal") \
        .getOrCreate()

    logger.info("Spark session created")
    return spark


# ============================================================================
# STEP 1: READ BRONZE DATA
# ============================================================================
def read_bronze_data(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Read Parquet files from bronze layer.

    Args:
        spark: SparkSession
        bronze_path: Full GCS path to bronze data (e.g., gs://bucket/table/2025/01/21/)

    Returns:
        DataFrame with bronze data
    """
    logger.info(f"Reading bronze data from: {bronze_path}")

    df = spark.read.parquet(bronze_path)

    logger.info("Bronze data loaded successfully")
    logger.info("Schema:")
    df.printSchema()

    return df


# ============================================================================
# STEP 2: APPLY TRANSFORMATIONS
# ============================================================================
def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply silver layer transformations.

    This is where you add your business logic. This example shows:
    1. Removing records with null key fields
    2. Adding derived columns
    3. Standardizing data

    Customize this function for your specific data and requirements!

    Args:
        df: Bronze DataFrame

    Returns:
        Transformed DataFrame
    """
    logger.info("Applying transformations...")

    # ========================================================================
    # EXAMPLE TRANSFORMATIONS - CUSTOMIZE FOR YOUR DATA!
    # ========================================================================

    # Example 1: Remove records where critical fields are null
    # Uncomment and adjust for your schema:
    # df = df.filter(F.col("id").isNotNull())
    # df = df.filter(F.col("country").isNotNull())

    # Example 2: Add derived columns
    # Uncomment and adjust for your schema:
    # df = df.withColumn("population_millions", F.col("population") / 1_000_000)
    # df = df.withColumn("data_year", F.year(F.col("date_column")))

    # Example 3: Standardize data (e.g., uppercase country names)
    # df = df.withColumn("country", F.upper(F.col("country")))

    # Example 4: Remove duplicates
    # df = df.dropDuplicates(["id", "date"])

    # For this minimal example, we'll just add metadata
    # In your implementation, add the transformations above!

    # Add silver layer metadata (processing timestamp)
    df = df.withColumn("silver_processed_at", F.current_timestamp())

    logger.info("Transformations applied")

    return df


# ============================================================================
# STEP 3: VALIDATE DATA
# ============================================================================
def validate_data(df: DataFrame) -> None:
    """
    Perform basic data quality checks.

    This validates the transformed data before writing to Iceberg.
    Raises exceptions if validation fails.

    Args:
        df: Transformed DataFrame
    """
    logger.info("Validating transformed data...")

    # Check 1: Ensure we have data (sample-based check)
    sample = df.limit(1).collect()
    if len(sample) == 0:
        raise ValueError("No data after transformations! Check your filters.")

    # Check 2: Ensure required metadata columns exist
    required_columns = ["silver_processed_at"]
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Check 3: Log sample statistics
    logger.info("Validation checks passed")
    logger.info(f"Schema has {len(df.columns)} columns")
    logger.info("Sample record (first row):")
    df.show(1, truncate=False)


# ============================================================================
# STEP 4: WRITE TO ICEBERG
# ============================================================================
def write_to_iceberg(
    df: DataFrame,
    catalog: str,
    database: str,
    table: str
) -> None:
    """
    Write DataFrame to Iceberg table using Spark catalog.

    This uses the Iceberg catalog configured in Spark properties.
    The table format is: catalog.database.table

    Prerequisites:
    - Iceberg catalog must be configured in Spark properties
    - Database (namespace) must exist
    - Table will be created if it doesn't exist

    Args:
        df: DataFrame to write
        catalog: Catalog name (e.g., 'icecat')
        database: Database/namespace name (e.g., 'silver')
        table: Table name (e.g., 'population_stats')
    """
    logger.info(f"Writing to Iceberg table: {catalog}.{database}.{table}")

    # Create database if it doesn't exist
    spark = df.sparkSession
    spark.sql(f"USE {catalog}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {database}")
    logger.info(f"Database {database} ready")

    # Full table reference
    full_table = f"{catalog}.{database}.{table}"

    # Write using Iceberg V2 API
    # This automatically creates the table if it doesn't exist
    logger.info(f"Writing data to {full_table}...")

    df.writeTo(full_table) \
        .using("iceberg") \
        .append()

    logger.info(f"Successfully wrote data to Iceberg table: {full_table}")
    logger.info(f"Query in BigQuery: SELECT * FROM `{database}.{table}` LIMIT 10")


# ============================================================================
# MAIN TRANSFORMATION FLOW
# ============================================================================
def main() -> int:
    """
    Main function - orchestrates the entire silver transformation.

    Flow:
    1. Parse arguments
    2. Create Spark session
    3. Read bronze data
    4. Apply transformations
    5. Validate data
    6. Write to Iceberg

    Returns:
        Exit code (0=success, 1=failure)
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Minimal Silver Transformation with Iceberg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python silver_transformation_minimal.py \\
      --bronze_path gs://bronze-bucket/population_stats/2025/01/21/ \\
      --catalog icecat \\
      --database silver \\
      --table population_stats

Note: Iceberg catalog properties must be configured via Dataproc submit!
        """
    )

    parser.add_argument(
        "--bronze_path",
        required=True,
        help="Full GCS path to bronze data (e.g., gs://bucket/table/2025/01/21/)"
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Iceberg catalog name (e.g., 'icecat')"
    )

    parser.add_argument(
        "--database",
        required=True,
        help="Target database/namespace (e.g., 'silver')"
    )

    parser.add_argument(
        "--table",
        required=True,
        help="Target table name (e.g., 'population_stats')"
    )

    args = parser.parse_args()

    # Start processing
    logger.info("=" * 70)
    logger.info("STARTING MINIMAL SILVER TRANSFORMATION")
    logger.info("=" * 70)
    logger.info(f"Bronze path: {args.bronze_path}")
    logger.info(f"Target: {args.catalog}.{args.database}.{args.table}")
    logger.info("=" * 70)

    try:
        # Step 1: Create Spark session
        spark = get_spark_session()

        try:
            # Step 2: Read bronze data
            df_bronze = read_bronze_data(spark, args.bronze_path)

            # Step 3: Apply transformations
            df_silver = apply_transformations(df_bronze)

            # Step 4: Validate data
            validate_data(df_silver)

            # Step 5: Write to Iceberg
            write_to_iceberg(
                df=df_silver,
                catalog=args.catalog,
                database=args.database,
                table=args.table
            )

            logger.info("=" * 70)
            logger.info("SILVER TRANSFORMATION COMPLETED SUCCESSFULLY!")
            logger.info("=" * 70)
            return 0

        finally:
            # Always stop Spark session
            spark.stop()
            logger.info("Spark session stopped")

    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"TRANSFORMATION FAILED: {str(e)}")
        logger.error("=" * 70)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

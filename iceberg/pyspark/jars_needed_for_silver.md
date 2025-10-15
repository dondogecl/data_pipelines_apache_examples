# JARs needed for Iceberg (Silver layer)

When implementing silver transformation with Iceberg tables:

```bash
--jars=gs://spark-lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.4.2.jar
```

Or for BigQuery integration:
```bash
--jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.34.0.jar
```

For bronze layer (CSV->Parquet): No JARs needed!
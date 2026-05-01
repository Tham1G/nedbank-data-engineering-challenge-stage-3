"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

from pyspark.sql.functions import current_timestamp
from spark import get_spark
from config_loader import load_pipeline_config



def run_ingestion():
    
    config = load_pipeline_config()

    spark = get_spark()
    paths = config["paths"]

    accounts = spark.read.option("header", True).csv(paths["input"]["accounts"])
    customers = spark.read.option("header", True).csv(paths["input"]["customers"])
    transactions = spark.read.json(paths["input"]["transactions"])

    run_timestamp = current_timestamp()

    accounts = accounts.withColumn("ingestion_timestamp", run_timestamp)
    customers = customers.withColumn("ingestion_timestamp", run_timestamp)
    transactions = transactions.withColumn("ingestion_timestamp", run_timestamp)

    (
        accounts.write
        .format("delta")
        .mode("overwrite")
        .save(paths["bronze"]["accounts"])
    )

    (
        customers.write
        .format("delta")
        .mode("overwrite")
        .save(paths["bronze"]["customers"])
    )

    (
        transactions.write
        .format("delta")
        .mode("overwrite")
        .save(paths["bronze"]["transactions"])
    )
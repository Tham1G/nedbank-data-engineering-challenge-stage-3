"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

import json
import os
from pyspark.sql import functions as F
from spark import get_spark
from config_loader import load_pipeline_config


def stable_bigint_key(column_name):
    return (
        F.conv(
            F.substring(F.sha2(F.col(column_name).cast("string"), 256), 1, 15),
            16,
            10
        ).cast("long")
    )


def ensure_column(df, column_name, default_value=None, data_type="string"):
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(default_value).cast(data_type))
    return df


def update_dq_report_with_gold_counts(gold_counts):
    dq_report_path = "/data/output/dq_report.json"

    if not os.path.exists(dq_report_path):
        return

    with open(dq_report_path, "r") as f:
        report = json.load(f)

    report["gold_layer_record_counts"] = {
        "dim_accounts": int(gold_counts["dim_accounts"]),
        "dim_customers": int(gold_counts["dim_customers"]),
        "fact_transactions": int(gold_counts["fact_transactions"])
    }

    with open(dq_report_path, "w") as f:
        json.dump(report, f, indent=2)


def run_provisioning():
    config = load_pipeline_config()

    spark = get_spark()
    paths = config["paths"]

    accounts = spark.read.format("delta").load(paths["silver"]["accounts"])
    customers = spark.read.format("delta").load(paths["silver"]["customers"])
    transactions = spark.read.format("delta").load(paths["silver"]["transactions"])

    transactions = ensure_column(transactions, "merchant_category", None, "string")
    transactions = ensure_column(transactions, "merchant_subcategory", None, "string")
    transactions = ensure_column(transactions, "currency", "ZAR", "string")
    transactions = ensure_column(transactions, "channel", None, "string")
    transactions = ensure_column(transactions, "province", None, "string")
    transactions = ensure_column(transactions, "dq_flag", None, "string")
    transactions = ensure_column(transactions, "transaction_timestamp", None, "timestamp")
    transactions = ensure_column(transactions, "ingestion_timestamp", None, "timestamp")

    customer_age = F.floor(F.months_between(F.current_date(), F.col("dob")) / F.lit(12))

    dim_customers = (
        customers
        .withColumn("customer_sk", stable_bigint_key("customer_id"))
        .withColumn(
            "age_band",
            F.when(customer_age < 25, F.lit("18-24"))
            .when(customer_age < 35, F.lit("25-34"))
            .when(customer_age < 50, F.lit("35-49"))
            .when(customer_age < 65, F.lit("50-64"))
            .otherwise(F.lit("65+"))
        )
        .select(
            F.col("customer_sk").cast("long").alias("customer_sk"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("gender").cast("string").alias("gender"),
            F.col("province").cast("string").alias("province"),
            F.col("income_band").cast("string").alias("income_band"),
            F.col("segment").cast("string").alias("segment"),
            F.col("risk_score").cast("int").alias("risk_score"),
            F.col("kyc_status").cast("string").alias("kyc_status"),
            F.col("age_band").cast("string").alias("age_band")
        )
    )

    dim_accounts = (
        accounts
        .withColumnRenamed("customer_ref", "customer_id")
        .withColumn("account_sk", stable_bigint_key("account_id"))
        .select(
            F.col("account_sk").cast("long").alias("account_sk"),
            F.col("account_id").cast("string").alias("account_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("account_type").cast("string").alias("account_type"),
            F.col("account_status").cast("string").alias("account_status"),
            F.col("open_date").cast("date").alias("open_date"),
            F.col("product_tier").cast("string").alias("product_tier"),
            F.col("digital_channel").cast("string").alias("digital_channel"),
            F.col("credit_limit").cast("decimal(18,2)").alias("credit_limit"),
            F.col("current_balance").cast("decimal(18,2)").alias("current_balance"),
            F.col("last_activity_date").cast("date").alias("last_activity_date")
        )
    )

    dim_accounts_join = dim_accounts.select(
        "account_id",
        "account_sk",
        "customer_id"
    )

    dim_customers_join = dim_customers.select(
        "customer_id",
        "customer_sk",
        F.col("province").alias("customer_province")
    )

    fact_transactions = (
        transactions
        .join(F.broadcast(dim_accounts_join), "account_id", "inner")
        .join(F.broadcast(dim_customers_join), "customer_id", "inner")
        .withColumn("transaction_sk", stable_bigint_key("transaction_id"))
        .withColumn(
            "province",
            F.coalesce(F.col("province"), F.col("customer_province"))
        )
        .select(
            F.col("transaction_sk").cast("long").alias("transaction_sk"),
            F.col("transaction_id").cast("string").alias("transaction_id"),
            F.col("account_sk").cast("long").alias("account_sk"),
            F.col("customer_sk").cast("long").alias("customer_sk"),
            F.col("transaction_date").cast("date").alias("transaction_date"),
            F.col("transaction_timestamp").cast("timestamp").alias("transaction_timestamp"),
            F.col("transaction_type").cast("string").alias("transaction_type"),
            F.col("merchant_category").cast("string").alias("merchant_category"),
            F.col("merchant_subcategory").cast("string").alias("merchant_subcategory"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.col("currency").cast("string").alias("currency"),
            F.col("channel").cast("string").alias("channel"),
            F.col("province").cast("string").alias("province"),
            F.col("dq_flag").cast("string").alias("dq_flag"),
            F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp")
        )
    )

    (
        dim_customers.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths["gold"]["dim_customers"])
    )

    (
        dim_accounts.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths["gold"]["dim_accounts"])
    )

    (
        fact_transactions.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths["gold"]["fact_transactions"])
    )

    gold_counts = {
        "dim_accounts": dim_accounts.count(),
        "dim_customers": dim_customers.count(),
        "fact_transactions": fact_transactions.count()
    }

    update_dq_report_with_gold_counts(gold_counts)
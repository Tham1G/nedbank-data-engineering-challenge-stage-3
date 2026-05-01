"""
Stage 3 — Streaming extension: Process micro-batch JSONL files from /data/stream/.

This module is only used at Stage 3. You do not need to implement it for
Stage 1 or Stage 2.

Input paths:
  /data/stream/            — Directory of micro-batch JSONL files, arriving
                             during pipeline execution. The scoring system
                             drops new files here while your container runs.

Output paths (your pipeline must create these directories):
  /data/output/stream_gold/current_balances/    — 4 fields; upsert table
  /data/output/stream_gold/recent_transactions/ — 7 fields; last 50 per account

Requirements:
  - Poll /data/stream/ periodically for new files (the scoring system will
    deliver micro-batches over the course of the run).
  - Process each new file and merge results into the stream_gold tables.
  - current_balances: maintain one row per account_id (upsert/merge).
  - recent_transactions: maintain the 50 most recent transactions per account.
  - SLA: updated_at must be within 300 seconds of the source event timestamp
    for full credit; 300–600 seconds receives partial credit.
  - Write all stream_gold output as Delta Parquet tables.
  - The stream_ingest loop must terminate on its own when no new files have
    arrived for a reasonable quiesce period. The container has a 30-minute
    hard timeout — do not run indefinitely.

See output_schema_spec.md §5 and §6 for the full field-by-field specification
of current_balances and recent_transactions.

See docker_interface_contract.md §3 for the /data/stream/ mount details.
"""

import os
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from spark import get_spark


STREAM_ROOT = "/data/stream"
OUTPUT_BASE = "/data/output/stream_gold"
PROCESSED_FILE = "/tmp/processed_files.txt"

CURRENT_BALANCES_PATH = f"{OUTPUT_BASE}/current_balances"
RECENT_TRANSACTIONS_PATH = f"{OUTPUT_BASE}/recent_transactions"


def load_processed_files():
    if not os.path.exists(PROCESSED_FILE):
        return set()

    with open(PROCESSED_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())


def save_processed_files(processed_files):
    with open(PROCESSED_FILE, "w") as f:
        for file_path in sorted(processed_files):
            f.write(file_path + "\n")


def discover_stream_files():
    if not os.path.exists(STREAM_ROOT):
        return []

    files_found = []

    for root, _, files in os.walk(STREAM_ROOT):
        for file_name in files:
            if file_name.lower().endswith(".jsonl"):
                files_found.append(os.path.join(root, file_name))

    return sorted(files_found)


def ensure_column(df, column_name, default_value=None):
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(default_value))
    return df


def get_nested_or_null(df, parent_col, child_col, alias_name):
    if parent_col not in df.columns:
        return df.withColumn(alias_name, F.lit(None).cast("string"))

    try:
        nested_fields = [field.name for field in df.schema[parent_col].dataType.fields]
    except Exception:
        nested_fields = []

    if child_col not in nested_fields:
        return df.withColumn(alias_name, F.lit(None).cast("string"))

    return df.withColumn(alias_name, F.col(f"{parent_col}.{child_col}").cast("string"))


def parse_stream_transactions(spark, file_path):
    raw = spark.read.json(file_path)

    raw = ensure_column(raw, "transaction_id", None)
    raw = ensure_column(raw, "account_id", None)
    raw = ensure_column(raw, "transaction_date", None)
    raw = ensure_column(raw, "transaction_time", "00:00:00")
    raw = ensure_column(raw, "transaction_type", None)
    raw = ensure_column(raw, "amount", None)
    raw = ensure_column(raw, "currency", "ZAR")
    raw = ensure_column(raw, "channel", None)
    raw = ensure_column(raw, "merchant_category", None)
    raw = ensure_column(raw, "merchant_subcategory", None)

    raw = get_nested_or_null(raw, "location", "province", "province")

    parsed_transaction_date = F.coalesce(
        F.to_date(F.col("transaction_date").cast("string"), "yyyy-MM-dd"),
        F.to_date(F.col("transaction_date").cast("string"), "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(F.col("transaction_date").cast("long"))),
        F.to_date(
            F.from_unixtime(
                (F.col("transaction_date").cast("double") / F.lit(1000)).cast("long")
            )
        )
    )

    raw_currency = F.upper(F.trim(F.col("currency").cast("string")))

    parsed = (
        raw
        .withColumn("transaction_date", parsed_transaction_date)
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(parsed_transaction_date, "yyyy-MM-dd"),
                    F.col("transaction_time")
                ),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .withColumn(
            "amount",
            F.regexp_replace(F.col("amount").cast("string"), ",", "")
            .cast(DecimalType(18, 2))
        )
        .withColumn(
            "currency",
            F.when(
                raw_currency.isin("ZAR", "RAND", "RANDS", "R", "710"),
                F.lit("ZAR")
            ).otherwise(raw_currency)
        )
        .select(
            F.col("account_id").cast("string").alias("account_id"),
            F.col("transaction_id").cast("string").alias("transaction_id"),
            F.col("transaction_timestamp").cast("timestamp").alias("transaction_timestamp"),
            F.col("amount").cast(DecimalType(18, 2)).alias("amount"),
            F.col("transaction_type").cast("string").alias("transaction_type"),
            F.col("channel").cast("string").alias("channel")
        )
        .filter(F.col("account_id").isNotNull())
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("transaction_timestamp").isNotNull())
        .filter(F.col("amount").isNotNull())
        .filter(F.col("transaction_type").isin("DEBIT", "CREDIT", "FEE", "REVERSAL"))
    )

    return parsed


def signed_amount_column():
    return (
        F.when(F.col("transaction_type") == "CREDIT", F.col("amount"))
        .when(F.col("transaction_type") == "REVERSAL", F.col("amount"))
        .when(F.col("transaction_type") == "DEBIT", -F.col("amount"))
        .when(F.col("transaction_type") == "FEE", -F.col("amount"))
        .otherwise(F.lit(0).cast(DecimalType(18, 2)))
    )


def initialise_current_balances_if_needed(spark):
    if DeltaTable.isDeltaTable(spark, CURRENT_BALANCES_PATH):
        return

    accounts = spark.read.format("delta").load("/data/output/gold/dim_accounts")

    balances = (
        accounts
        .select(
            F.col("account_id").cast("string").alias("account_id"),
            F.col("current_balance").cast(DecimalType(18, 2)).alias("current_balance")
        )
        .withColumn("last_transaction_timestamp", F.lit(None).cast("timestamp"))
        .withColumn("updated_at", F.lit(None).cast("timestamp"))
    )

    (
        balances.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(CURRENT_BALANCES_PATH)
    )


def merge_current_balances(spark, batch_df):
    if batch_df.rdd.isEmpty():
        return

    initialise_current_balances_if_needed(spark)

    account_updates = (
        batch_df
        .withColumn("signed_amount", signed_amount_column())
        .groupBy("account_id")
        .agg(
            F.sum("signed_amount").cast(DecimalType(18, 2)).alias("balance_delta"),
            F.max("transaction_timestamp").alias("last_transaction_timestamp")
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    target = DeltaTable.forPath(spark, CURRENT_BALANCES_PATH)

    (
        target.alias("target")
        .merge(
            account_updates.alias("source"),
            "target.account_id = source.account_id"
        )
        .whenMatchedUpdate(set={
            "current_balance": "cast(target.current_balance + source.balance_delta as decimal(18,2))",
            "last_transaction_timestamp": "source.last_transaction_timestamp",
            "updated_at": "source.updated_at"
        })
        .whenNotMatchedInsert(values={
            "account_id": "source.account_id",
            "current_balance": "cast(source.balance_delta as decimal(18,2))",
            "last_transaction_timestamp": "source.last_transaction_timestamp",
            "updated_at": "source.updated_at"
        })
        .execute()
    )


def merge_recent_transactions(spark, batch_df):
    if batch_df.rdd.isEmpty():
        return

    recent_batch = (
        batch_df
        .select(
            "account_id",
            "transaction_id",
            "transaction_timestamp",
            "amount",
            "transaction_type",
            "channel"
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, RECENT_TRANSACTIONS_PATH):
        window = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())

        initial_recent = (
            recent_batch
            .dropDuplicates(["account_id", "transaction_id"])
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") <= 50)
            .drop("rn")
        )

        (
            initial_recent.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(RECENT_TRANSACTIONS_PATH)
        )
        return

    target = DeltaTable.forPath(spark, RECENT_TRANSACTIONS_PATH)

    (
        target.alias("target")
        .merge(
            recent_batch.alias("source"),
            """
            target.account_id = source.account_id
            AND target.transaction_id = source.transaction_id
            """
        )
        .whenMatchedUpdate(set={
            "transaction_timestamp": "source.transaction_timestamp",
            "amount": "source.amount",
            "transaction_type": "source.transaction_type",
            "channel": "source.channel",
            "updated_at": "source.updated_at"
        })
        .whenNotMatchedInsert(values={
            "account_id": "source.account_id",
            "transaction_id": "source.transaction_id",
            "transaction_timestamp": "source.transaction_timestamp",
            "amount": "source.amount",
            "transaction_type": "source.transaction_type",
            "channel": "source.channel",
            "updated_at": "source.updated_at"
        })
        .execute()
    )

    combined = spark.read.format("delta").load(RECENT_TRANSACTIONS_PATH)

    window = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())

    pruned = (
        combined
        .dropDuplicates(["account_id", "transaction_id"])
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") <= 50)
        .drop("rn")
    )

    (
        pruned.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(RECENT_TRANSACTIONS_PATH)
    )


def process_stream_file(spark, file_path):
    batch_df = parse_stream_transactions(spark, file_path)

    merge_current_balances(spark, batch_df)
    merge_recent_transactions(spark, batch_df)


def run_stream_ingestion():
    spark = get_spark()

    processed_files = load_processed_files()

    idle_cycles = 0
    max_idle_cycles = 2
    poll_interval_seconds = 10

    while idle_cycles < max_idle_cycles:
        available_files = discover_stream_files()
        new_files = [
            file_path for file_path in available_files
            if file_path not in processed_files
        ]

        if not new_files:
            idle_cycles += 1
            time.sleep(poll_interval_seconds)
            continue

        idle_cycles = 0

        for file_path in new_files:
            process_stream_file(spark, file_path)
            processed_files.add(file_path)
            save_processed_files(processed_files)

    print("Streaming ingestion completed")
"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""

import json
import time
from datetime import datetime
from functools import reduce
from operator import or_

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from spark import get_spark
from config_loader import load_pipeline_config, load_dq_rules


def ensure_column(df, column_name, default_value=None):
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(default_value))
    return df


def get_nested_or_null(df, parent_col, child_col, alias_name):
    if parent_col not in df.columns:
        return df.withColumn(alias_name, F.lit(None).cast("string"))

    try:
        field_names = [field.name for field in df.schema[parent_col].dataType.fields]
    except Exception:
        field_names = []

    if child_col not in field_names:
        return df.withColumn(alias_name, F.lit(None).cast("string"))

    return df.withColumn(alias_name, F.col(f"{parent_col}.{child_col}").cast("string"))


def parse_date_from_rules(column_name, accepted_formats):
    raw_col = F.col(column_name).cast("string")

    parsed_options = []

    for fmt in accepted_formats:
        if fmt == "yyyy-MM-dd":
            parsed_options.append(F.to_date(raw_col, "yyyy-MM-dd"))
        elif fmt == "dd/MM/yyyy":
            parsed_options.append(F.to_date(raw_col, "dd/MM/yyyy"))
        elif fmt == "epoch_seconds":
            parsed_options.append(F.to_date(F.from_unixtime(raw_col.cast("long"))))
        elif fmt == "epoch_milliseconds":
            parsed_options.append(
                F.to_date(
                    F.from_unixtime(
                        (raw_col.cast("double") / F.lit(1000)).cast("long")
                    )
                )
            )

    if not parsed_options:
        parsed_options.append(F.to_date(raw_col, "yyyy-MM-dd"))

    return F.coalesce(*parsed_options)


def any_null_condition(required_fields):
    conditions = [F.col(field).isNull() for field in required_fields]
    if not conditions:
        return F.lit(False)
    return reduce(or_, conditions)


def write_dq_report(
    output_path,
    dq_rules,
    start_time,
    accounts_raw_count,
    customers_raw_count,
    transactions_raw_count,
    duplicate_count,
    orphaned_count,
    type_mismatch_count,
    date_format_count,
    currency_variant_count,
    null_required_count,
    gold_counts=None
):
    duration_seconds = int(time.time() - start_time)

    def percentage(count, denominator):
        if denominator == 0:
            return 0.0
        return round((count / denominator) * 100, 2)

    required_issue_codes = dq_rules["dq_report"]["required_issue_codes"]

    issue_config = {
        "DUPLICATE_DEDUPED": {
            "issue_type": dq_rules["duplicate_checks"]["transactions"]["dq_flag"],
            "description": "duplicate_transactions",
            "records_affected": duplicate_count,
            "denominator": transactions_raw_count,
            "handling_action": dq_rules["duplicate_checks"]["transactions"]["handling_action"],
            "records_in_output": 0
        },
        "ORPHANED_ACCOUNT": {
            "issue_type": dq_rules["referential_integrity"]["transactions"]["dq_flag"],
            "description": "orphaned_transactions",
            "records_affected": orphaned_count,
            "denominator": transactions_raw_count,
            "handling_action": "QUARANTINED",
            "records_in_output": 0
        },
        "TYPE_MISMATCH": {
            "issue_type": "amount_or_domain_type_mismatch",
            "description": "type_mismatch",
            "records_affected": type_mismatch_count,
            "denominator": transactions_raw_count,
            "handling_action": "EXCLUDED_IF_UNCASTABLE_OR_INVALID_DOMAIN",
            "records_in_output": 0
        },
        "DATE_FORMAT": {
            "issue_type": "date_format_inconsistency",
            "description": "date_format",
            "records_affected": date_format_count,
            "denominator": transactions_raw_count,
            "handling_action": "NORMALISED_DATE",
            "records_in_output": date_format_count
        },
        "CURRENCY_VARIANT": {
            "issue_type": "currency_variant",
            "description": "currency_variant",
            "records_affected": currency_variant_count,
            "denominator": transactions_raw_count,
            "handling_action": "NORMALISED_CURRENCY",
            "records_in_output": currency_variant_count
        },
        "NULL_REQUIRED": {
            "issue_type": "null_required_key",
            "description": "null_required",
            "records_affected": null_required_count,
            "denominator": transactions_raw_count,
            "handling_action": "EXCLUDED_NULL_REQUIRED_FIELD",
            "records_in_output": 0
        }
    }

    dq_issues = []

    for issue_code in required_issue_codes:
        config = issue_config[issue_code]

        dq_issues.append({
            "issue_code": issue_code,
            "issue_type": config["description"],
            "records_affected": int(config["records_affected"]),
            "percentage_of_total": percentage(
                config["records_affected"],
                config["denominator"]
            ),
            "handling_action": config["handling_action"],
            "records_in_output": int(config["records_in_output"])
        })

    report = {
        "$schema": dq_rules["dq_report"]["schema"],
        "run_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": "2",
        "source_record_counts": {
            "accounts_raw": int(accounts_raw_count),
            "transactions_raw": int(transactions_raw_count),
            "customers_raw": int(customers_raw_count)
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_counts or {},
        "execution_duration_seconds": duration_seconds
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)


def run_transformation():
    start_time = time.time()

    pipeline_config = load_pipeline_config()
    dq_rules = load_dq_rules()

    spark = get_spark()
    paths = pipeline_config["paths"]

    accounts_raw = spark.read.format("delta").load(paths["bronze"]["accounts"])
    customers_raw = spark.read.format("delta").load(paths["bronze"]["customers"])
    transactions_raw = spark.read.format("delta").load(paths["bronze"]["transactions"])

    accounts_raw_count = accounts_raw.count()
    customers_raw_count = customers_raw.count()
    transactions_raw_count = transactions_raw.count()

    transaction_required_fields = dq_rules["null_checks"]["transactions"]["required_fields"]

    transactions_prepared = transactions_raw

    for field in transaction_required_fields:
        transactions_prepared = ensure_column(transactions_prepared, field, None)

    transactions_prepared = ensure_column(transactions_prepared, "merchant_subcategory", None)
    transactions_prepared = ensure_column(transactions_prepared, "merchant_category", None)
    transactions_prepared = ensure_column(transactions_prepared, "transaction_time", "00:00:00")

    transactions_prepared = get_nested_or_null(
        transactions_prepared,
        "location",
        "province",
        "province"
    )

    null_required_condition = any_null_condition(transaction_required_fields)

    null_required_count = transactions_prepared.filter(null_required_condition).count()

    accounts_clean = (
        accounts_raw
        .filter(F.col("account_id").isNotNull())
        .dropDuplicates(["account_id"])
        .withColumn(
            "open_date",
            F.to_date(F.col("open_date").cast("string"), "yyyy-MM-dd")
        )
        .withColumn(
            "last_activity_date",
            F.to_date(F.col("last_activity_date").cast("string"), "yyyy-MM-dd")
        )
        .withColumn(
            "credit_limit",
            F.col("credit_limit").cast(DecimalType(18, 2))
        )
        .withColumn(
            "current_balance",
            F.col("current_balance").cast(DecimalType(18, 2))
        )
    )

    customers_clean = (
        customers_raw
        .dropDuplicates(["customer_id"])
        .withColumn(
            "dob",
            F.to_date(F.col("dob").cast("string"), "yyyy-MM-dd")
        )
        .withColumn(
            "risk_score",
            F.col("risk_score").cast("int")
        )
    )

    duplicate_rule = dq_rules["duplicate_checks"]["transactions"]
    duplicate_key = duplicate_rule["natural_key"]

    duplicate_counts = (
        transactions_prepared
        .groupBy(duplicate_key)
        .count()
        .withColumnRenamed("count", "transaction_duplicate_count")
    )

    transactions_with_duplicate_count = (
        transactions_prepared
        .join(duplicate_counts, duplicate_key, "left")
    )

    duplicate_group_rows = (
        transactions_with_duplicate_count
        .filter(F.col("transaction_duplicate_count") > 1)
        .count()
    )

    duplicate_group_count = (
        transactions_with_duplicate_count
        .filter(F.col("transaction_duplicate_count") > 1)
        .select(duplicate_key)
        .distinct()
        .count()
    )

    duplicate_count = duplicate_group_rows - duplicate_group_count

    transactions_dedup = transactions_with_duplicate_count.dropDuplicates([duplicate_key])

    original_amount = F.col("amount")
    amount_as_decimal = (
        F.regexp_replace(original_amount.cast("string"), ",", "")
        .cast(DecimalType(18, 2))
    )

    amount_type_mismatch_condition = (
        original_amount.isNotNull()
        & amount_as_decimal.isNull()
    )

    transaction_type_allowed = dq_rules["domain_checks"]["transaction_type"]["allowed"]
    channel_allowed = dq_rules["domain_checks"]["channel"]["allowed"]

    transaction_type_domain_condition = (
        F.col("transaction_type").isNotNull()
        & ~F.col("transaction_type").isin(transaction_type_allowed)
    )

    channel_domain_condition = (
        F.col("channel").isNotNull()
        & ~F.col("channel").isin(channel_allowed)
    )

    type_mismatch_condition = (
        amount_type_mismatch_condition
        | transaction_type_domain_condition
        | channel_domain_condition
    )

    type_mismatch_count = transactions_dedup.filter(type_mismatch_condition).count()

    currency_rules = dq_rules["currency_normalisation"]
    target_currency = currency_rules["target_value"]
    accepted_currency_variants = currency_rules["accepted_variants"]

    raw_currency = F.upper(F.trim(F.col("currency").cast("string")))

    currency_variant_condition = (
        raw_currency.isin(accepted_currency_variants)
        & (raw_currency != F.lit(target_currency))
    )

    currency_variant_count = transactions_dedup.filter(currency_variant_condition).count()

    date_rules = dq_rules["date_format_checks"]["transactions"]
    accepted_date_formats = date_rules["accepted_formats"]

    parsed_transaction_date = parse_date_from_rules(
        "transaction_date",
        accepted_date_formats
    )

    date_parse_failed_condition = (
        F.col("transaction_date").isNotNull()
        & parsed_transaction_date.isNull()
    )

    date_format_variant_condition = (
        F.col("transaction_date").isNotNull()
        & parsed_transaction_date.isNotNull()
        & (
            F.col("transaction_date").cast("string")
            != F.date_format(parsed_transaction_date, "yyyy-MM-dd")
        )
    )

    date_format_condition = (
        date_parse_failed_condition
        | date_format_variant_condition
    )

    date_format_count = transactions_dedup.filter(date_format_condition).count()

    transactions_standardised = (
        transactions_dedup
        .withColumn("amount", amount_as_decimal)
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
            "currency",
            F.when(
                raw_currency.isin(accepted_currency_variants),
                F.lit(target_currency)
            ).otherwise(raw_currency)
        )
    )

    referential_rule = dq_rules["referential_integrity"]["transactions"]
    reference_field = referential_rule["reference_field"]

    account_keys = accounts_clean.select(
        F.col(reference_field).alias("account_id")
    ).distinct()

    transactions_with_orphan_flag = (
        transactions_standardised
        .join(
            account_keys.withColumn("account_exists", F.lit(True)),
            "account_id",
            "left"
        )
    )

    orphaned_condition = F.col("account_exists").isNull()

    orphaned_count = transactions_with_orphan_flag.filter(orphaned_condition).count()

    transactions_flagged = (
        transactions_with_orphan_flag
        .withColumn(
            "dq_flag",
            F.concat_ws(
                "|",
                F.when(
                    F.col("transaction_duplicate_count") > 1,
                    F.lit(dq_rules["duplicate_checks"]["transactions"]["dq_flag"])
                ),
                F.when(
                    orphaned_condition,
                    F.lit(dq_rules["referential_integrity"]["transactions"]["dq_flag"])
                ),
                F.when(
                    type_mismatch_condition,
                    F.lit(dq_rules["type_checks"]["transactions"]["amount"]["dq_flag"])
                ),
                F.when(
                    date_format_condition,
                    F.lit(dq_rules["date_format_checks"]["transactions"]["dq_flag"])
                ),
                F.when(
                    currency_variant_condition,
                    F.lit(dq_rules["currency_normalisation"]["dq_flag"])
                ),
                F.when(
                    null_required_condition,
                    F.lit(dq_rules["null_checks"]["transactions"]["dq_flag"])
                )
            )
        )
        .withColumn(
            "dq_flag",
            F.when(F.col("dq_flag") == "", F.lit(None)).otherwise(F.col("dq_flag"))
        )
        .drop("account_exists", "transaction_duplicate_count")
    )

    transactions_clean = (
        transactions_flagged
        .filter(~null_required_condition)
        .filter(~amount_type_mismatch_condition)
        .filter(~date_parse_failed_condition)
        .filter(~orphaned_condition)
    )

    (
        accounts_clean.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths["silver"]["accounts"])
    )

    (
        customers_clean.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths["silver"]["customers"])
    )

    (
        transactions_clean.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths["silver"]["transactions"])
    )

    write_dq_report(
        output_path=dq_rules["dq_report"]["output_path"],
        dq_rules=dq_rules,
        start_time=start_time,
        accounts_raw_count=accounts_raw_count,
        customers_raw_count=customers_raw_count,
        transactions_raw_count=transactions_raw_count,
        duplicate_count=duplicate_count,
        orphaned_count=orphaned_count,
        type_mismatch_count=type_mismatch_count,
        date_format_count=date_format_count,
        currency_variant_count=currency_variant_count,
        null_required_count=null_required_count
    )
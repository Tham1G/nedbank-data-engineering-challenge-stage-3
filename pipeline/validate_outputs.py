import json
import os
from spark import get_spark


def check_path(path):
    if not os.path.exists(path):
        print(f"Missing: {path}")
        return False

    print(f"Found: {path}")
    return True


def show_table(spark, name, path):
    print("\n" + "=" * 80)
    print(f"TABLE: {name}")
    print(f"PATH: {path}")

    df = spark.read.format("delta").load(path)

    print("Schema:")
    df.printSchema()

    count = df.count()
    print(f"Row count: {count}")

    print("Sample:")
    df.show(5, truncate=False)


def main():
    spark = get_spark()

    paths = {
        "bronze_accounts": "/data/output/bronze/accounts",
        "bronze_customers": "/data/output/bronze/customers",
        "bronze_transactions": "/data/output/bronze/transactions",
        "silver_accounts": "/data/output/silver/accounts",
        "silver_customers": "/data/output/silver/customers",
        "silver_transactions": "/data/output/silver/transactions",
        "gold_dim_accounts": "/data/output/gold/dim_accounts",
        "gold_dim_customers": "/data/output/gold/dim_customers",
        "gold_fact_transactions": "/data/output/gold/fact_transactions",
    }

    print("Checking output paths...")

    all_paths_ok = True
    for name, path in paths.items():
        all_paths_ok = check_path(path) and all_paths_ok

    dq_report_path = "/data/output/dq_report.json"
    dq_exists = check_path(dq_report_path)

    if not all_paths_ok:
        raise RuntimeError("One or more output paths are missing")

    print("\nReading Delta tables...")

    for name, path in paths.items():
        show_table(spark, name, path)

    if dq_exists:
        print("\n" + "=" * 80)
        print("DQ REPORT")
        with open(dq_report_path, "r") as f:
            report = json.load(f)
        print(json.dumps(report, indent=2))

    print("\nValidation completed successfully")


if __name__ == "__main__":
    main()
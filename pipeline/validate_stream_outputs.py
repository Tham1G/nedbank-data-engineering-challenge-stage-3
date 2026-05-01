from spark import get_spark


def show_table(spark, name, path):
    print("\n" + "=" * 80)
    print(f"TABLE: {name}")
    print(f"PATH: {path}")

    df = spark.read.format("delta").load(path)

    df.printSchema()
    print(f"Row count: {df.count()}")
    df.show(20, truncate=False)


def main():
    spark = get_spark()

    show_table(
        spark,
        "stream_current_balances",
        "/data/output/stream_gold/current_balances"
    )

    show_table(
        spark,
        "stream_recent_transactions",
        "/data/output/stream_gold/recent_transactions"
    )

    print("\nStream validation completed successfully")


if __name__ == "__main__":
    main()
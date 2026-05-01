import os
from pyspark.sql import SparkSession


def get_spark():
    spark_home = "/usr/local/lib/python3.11/site-packages/pyspark"

    os.environ["SPARK_HOME"] = spark_home
    os.environ["PATH"] = os.environ.get("PATH", "") + f":{spark_home}/bin"

    delta_jars = ",".join([
        "/opt/delta/jars/delta-spark_2.12-3.1.0.jar",
        "/opt/delta/jars/delta-storage-3.1.0.jar",
        "/opt/delta/jars/antlr4-runtime-4.9.3.jar",
    ])

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("nedbank-de-pipeline")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.jars", delta_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

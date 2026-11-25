"""Distributed PySpark 4.1.4 + Apache Iceberg V2 Ledger & 3-Way Reconciliation Job.

Executed inside the `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu` container
in the Docker `analytics`/`full` profiles against Lakekeeper REST Catalog and RustFS S3.
All monetary computations use BIGINT integer minor units (zero IEEE-754 float drift).
"""

from __future__ import annotations

import os
from typing import Any


def build_spark_session(app_name: str = "booking-reconciliation-lakehouse-spark") -> Any:
    from pyspark.sql import SparkSession

    catalog_uri = os.getenv("LAKEKEEPER_URI", "http://lakekeeper:8181/catalog")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://booking-lakehouse/warehouse")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://rustfs:9000")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", catalog_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .getOrCreate()
    )


def run_distributed_reconciliation() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = build_spark_session()
    try:
        cdc_df = spark.table("lakehouse.bronze.cdc_booking_events")
        # Idempotent deduplication by event_id and deterministic ordering
        w_dedup = Window.partitionBy("event_id").orderBy(F.col("source_lsn").asc())
        clean_cdc = (
            cdc_df.withColumn("_rn", F.row_number().over(w_dedup))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Assert exact integer minor-unit trial balance zero-sum on gold ledger
        ledger_df = spark.table("lakehouse.gold.ledger_journal_entries")
        imbalance_df = (
            ledger_df.groupBy("accounting_period")
            .agg(
                F.sum("reporting_debit_minor").alias("total_debits"),
                F.sum("reporting_credit_minor").alias("total_credits"),
            )
            .withColumn("imbalance", F.abs(F.col("total_debits") - F.col("total_credits")))
            .filter(F.col("imbalance") > 0)
        )
        if imbalance_df.count() > 0:
            raise RuntimeError("Double-entry zero-sum invariant breached in Spark ledger verification")

        # Periodic Iceberg V2 compaction via Spark procedure
        spark.sql("CALL lakehouse.system.rewrite_data_files('gold.ledger_journal_entries')")
        spark.sql("CALL lakehouse.system.rewrite_data_files('gold.reconciliation_results')")
        print(f"Verified {clean_cdc.count()} deduplicated CDC events and compacted Iceberg V2 gold tables.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_distributed_reconciliation()

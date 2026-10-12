"""
Apache Iceberg Table Management & Optimization Suite
Handles:
1. Snapshot History & Metadata Inspection
2. Time-Travel Queries (As of Snapshot ID & Timestamp)
3. Bin-pack Compaction (rewrite_data_files)
4. Snapshot Expiration & Orphan File Vacuuming
"""

import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("IcebergManager")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_WAREHOUSE_DIR = str(PROJECT_ROOT / "lakehouse" / "warehouse")

CATALOG_TYPE = os.getenv("CATALOG_TYPE", "hadoop")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")


def get_spark():
    """Initializes a SparkSession with Iceberg extensions and automatic version detection."""
    import pyspark
    version = pyspark.__version__
    major = int(version.split(".")[0])

    if major >= 4:
        packages = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    else:
        packages = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0"

    builder = (
        SparkSession.builder.appName("IcebergTableManager")
        .config("spark.jars.packages", packages)
    )

    try:
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        )
        if CATALOG_TYPE == "rest":
            builder = (
                builder
                .config("spark.sql.catalog.lakehouse.type", "rest")
                .config("spark.sql.catalog.lakehouse.uri", ICEBERG_REST_URI)
                .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
                .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
            )
        else:
            builder = (
                builder
                .config("spark.sql.catalog.lakehouse.type", "hadoop")
                .config("spark.sql.catalog.lakehouse.warehouse", LOCAL_WAREHOUSE_DIR)
            )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        logger.warning(f"Iceberg extension initialization failed ({e}). Returning base SparkSession.")
        fallback = SparkSession.builder.appName("IcebergTableManager").getOrCreate()
        fallback.sparkContext.setLogLevel("WARN")
        return fallback


def inspect_table_snapshots(table_name="lakehouse.db.gold_warehouse_products"):
    """Lists snapshots, commits, and manifest files for an Iceberg table."""
    spark = get_spark()
    logger.info(f"Inspecting snapshots for {table_name}...")
    try:
        snapshots_df = spark.sql(f"SELECT committed_at, snapshot_id, parent_id, operation, summary FROM {table_name}.snapshots ORDER BY committed_at DESC")
        snapshots_df.show(truncate=False)

        history_df = spark.sql(f"SELECT * FROM {table_name}.history")
        logger.info("Table History:")
        history_df.show(truncate=False)
        return snapshots_df.collect()
    except Exception as e:
        logger.error(f"Failed to inspect snapshots for {table_name}: {e}")
        return []


def query_time_travel_by_snapshot(snapshot_id, table_name="lakehouse.db.gold_warehouse_products"):
    """Queries an Iceberg table as of a specific snapshot ID."""
    spark = get_spark()
    logger.info(f"Querying {table_name} VERSION AS OF {snapshot_id}...")
    df = spark.read.option("snapshot-id", snapshot_id).table(table_name)
    df.show(5)
    return df


def query_time_travel_by_timestamp(as_of_timestamp, table_name="lakehouse.db.gold_warehouse_products"):
    """Queries an Iceberg table as of a specific point in time."""
    spark = get_spark()
    logger.info(f"Querying {table_name} TIMESTAMP AS OF '{as_of_timestamp}'...")
    df = spark.read.option("as-of-timestamp", int(as_of_timestamp.timestamp() * 1000)).table(table_name)
    df.show(5)
    return df


def compact_table(table_name="lakehouse.db.gold_warehouse_products", target_size_bytes=134217728):
    """
    Executes Iceberg bin-pack rewrite procedure to compact small files created during streaming.
    Resolves small-file problems and drastically accelerates Trino scan query latency.
    """
    spark = get_spark()
    logger.info(f"Initiating compaction for {table_name} (target size: {target_size_bytes} bytes)...")
    result = spark.sql(f"""
        CALL lakehouse.system.rewrite_data_files(
            table => '{table_name}',
            strategy => 'binpack',
            options => map('target-file-size-bytes', '{target_size_bytes}')
        )
    """)
    logger.info("Compaction Result:")
    result.show(truncate=False)
    return result


def expire_old_snapshots(table_name="lakehouse.db.gold_warehouse_products", retain_last=5):
    """
    Expires old snapshots to prune outdated metadata files and commit history.
    """
    spark = get_spark()
    logger.info(f"Expiring snapshots for {table_name}, retaining last {retain_last}...")
    result = spark.sql(f"""
        CALL lakehouse.system.expire_snapshots(
            table => '{table_name}',
            retain_last => {retain_last}
        )
    """)
    logger.info("Snapshot Expiry Result:")
    result.show(truncate=False)
    return result


def vacuum_orphan_files(table_name="lakehouse.db.gold_warehouse_products"):
    """
    Removes orphan data files not tracked by any active Iceberg metadata manifest.
    """
    spark = get_spark()
    logger.info(f"Removing orphan files for {table_name}...")
    result = spark.sql(f"""
        CALL lakehouse.system.remove_orphan_files(
            table => '{table_name}'
        )
    """)
    logger.info("Vacuum Result:")
    result.show(truncate=False)
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Iceberg Table Operations")
    parser.add_argument("--action", choices=["snapshots", "compact", "expire", "vacuum"], default="snapshots")
    parser.add_argument("--table", default="lakehouse.db.gold_warehouse_products")
    args = parser.parse_args()

    if args.action == "snapshots":
        inspect_table_snapshots(args.table)
    elif args.action == "compact":
        compact_table(args.table)
    elif args.action == "expire":
        expire_old_snapshots(args.table)
    elif args.action == "vacuum":
        vacuum_orphan_files(args.table)


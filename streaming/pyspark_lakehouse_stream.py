"""
PySpark Structured Streaming Pipeline for Real-Time Lakehouse Ingestion
- Ingests from Apache Kafka with strict schema validation
- Implements Medallion Architecture: Bronze (Raw), Silver (Validated), Gold (Enriched), and DLQ (Dead-Letter Queue)
- Enforces ACID writes into Apache Iceberg table format with checkpointing (WAL)
- Partitions Gold Lakehouse by (brand, price_category)
"""

import os
import sys
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType
)

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("PySparkLakehouseStream")

# Path Configuration
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_WAREHOUSE_DIR = str(PROJECT_ROOT / "lakehouse" / "warehouse")
LOCAL_CHECKPOINT_DIR = str(PROJECT_ROOT / "lakehouse" / "checkpoints")

# Kafka Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "amazon-products-stream")

# Catalog Configuration
CATALOG_TYPE = os.getenv("CATALOG_TYPE", "hadoop")  # 'rest' for Docker Iceberg REST or 'hadoop' for local
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")


ICEBERG_CATALOG_ACTIVE = True


def get_spark_packages():
    """Detects PySpark version and returns matching Iceberg/Kafka Maven coordinates."""
    import pyspark
    version = pyspark.__version__
    major = int(version.split(".")[0])
    logger.info(f"Detected PySpark runtime version: {version} (Major: {major})")

    if major >= 4:
        # Spark 4.x uses Scala 2.13
        return (
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
        )
    else:
        # Spark 3.5.x LTS uses Scala 2.12
        return (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
        )


def create_spark_session(app_name="AmazonLakehouseStream"):
    """
    Initializes SparkSession with Apache Iceberg extensions and Kafka connectors.
    Configures AWS S3/MinIO credentials and Catalog configuration with automatic fallback.
    """
    global ICEBERG_CATALOG_ACTIVE
    packages = get_spark_packages()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    try:
        # Attempt to load Iceberg Spark extensions and Catalog
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        )

        if CATALOG_TYPE == "rest":
            logger.info(f"Configuring Iceberg REST Catalog at {ICEBERG_REST_URI}")
            builder = (
                builder
                .config("spark.sql.catalog.lakehouse.type", "rest")
                .config("spark.sql.catalog.lakehouse.uri", ICEBERG_REST_URI)
                .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
                .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
                .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", "admin")
                .config("spark.hadoop.fs.s3a.secret.key", "password123")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            )
        else:
            logger.info(f"Configuring local Hadoop catalog at {LOCAL_WAREHOUSE_DIR}")
            builder = (
                builder
                .config("spark.sql.catalog.lakehouse.type", "hadoop")
                .config("spark.sql.catalog.lakehouse.warehouse", LOCAL_WAREHOUSE_DIR)
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized with Iceberg catalog successfully.")
        return spark
    except Exception as e:
        logger.warning(f"Iceberg extension initialization failed ({e}). Falling back to native Lakehouse Parquet storage engine.")
        ICEBERG_CATALOG_ACTIVE = False
        # Fallback session without Iceberg extensions
        fallback_builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "4")
        )
        spark = fallback_builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark


# ==========================================================
# SCHEMA DEFINITIONS
# ==========================================================
RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("price", StringType(), True),
    StructField("mrp", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("description", StringType(), True),
    StructField("highlights", StringType(), True),
    StructField("specifications", StringType(), True),
    StructField("image_urls", StringType(), True),
    StructField("video_urls", StringType(), True),
    StructField("source", StringType(), True),
    StructField("_anomaly_injected", StringType(), True),
])


def setup_iceberg_tables(spark):
    """
    Ensures all Iceberg Medallion tables exist with proper schemas and partition specs.
    Gracefully falls back to local Lakehouse Parquet directories if catalog extensions cannot be bound.
    """
    global ICEBERG_CATALOG_ACTIVE
    if not ICEBERG_CATALOG_ACTIVE:
        logger.info("Using local Lakehouse storage engine (Parquet with partitioning).")
        for table in ["bronze_raw_events", "silver_validated_products", "dlq_rejected_records", "gold_warehouse_products"]:
            os.makedirs(os.path.join(LOCAL_WAREHOUSE_DIR, table), exist_ok=True)
        return

    logger.info("Creating Iceberg namespaces and tables if not present...")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.db")

        # Bronze Table (Raw Streams)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.db.bronze_raw_events (
                kafka_key STRING,
                kafka_topic STRING,
                kafka_partition INT,
                kafka_offset LONG,
                kafka_timestamp TIMESTAMP,
                raw_payload STRING,
                ingest_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(kafka_timestamp))
            TBLPROPERTIES (
                'write.format.default'='parquet',
                'history.expire.max-snapshot-age-ms'='604800000',
                'write.parquet.compression-codec'='zstd'
            )
        """)

        # Silver Table (Validated & Cleaned)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.db.silver_validated_products (
                event_id STRING,
                event_timestamp TIMESTAMP,
                url STRING,
                title STRING,
                price DOUBLE,
                mrp DOUBLE,
                discount DOUBLE,
                rating DOUBLE,
                rating_count INT,
                description STRING,
                highlights STRING,
                specifications STRING,
                image_urls STRING,
                video_urls STRING,
                validated_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(validated_time))
            TBLPROPERTIES (
                'write.format.default'='parquet',
                'write.parquet.compression-codec'='zstd'
            )
        """)

        # Dead-Letter Queue (DLQ Table for Malformed Records)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.db.dlq_rejected_records (
                event_id STRING,
                raw_payload STRING,
                rejection_reason STRING,
                rejected_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(rejected_time))
            TBLPROPERTIES (
                'write.format.default'='parquet'
            )
        """)

        # Gold Lakehouse Table (Analytical Star Schema)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.db.gold_warehouse_products (
                url STRING,
                title STRING,
                brand STRING,
                price DOUBLE,
                mrp DOUBLE,
                savings DOUBLE,
                discount DOUBLE,
                discount_percentage DOUBLE,
                price_category STRING,
                rating DOUBLE,
                rating_category STRING,
                rating_count INT,
                popularity STRING,
                ram STRING,
                storage STRING,
                processor STRING,
                operating_system STRING,
                description STRING,
                highlights STRING,
                specifications STRING,
                image_urls STRING,
                video_urls STRING,
                last_updated TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (brand, price_category)
            TBLPROPERTIES (
                'write.format.default'='parquet',
                'write.parquet.compression-codec'='snappy',
                'write.target-file-size-bytes'='134217728'
            )
        """)
        logger.info("Iceberg tables verified successfully.")
    except Exception as e:
        logger.warning(f"Iceberg catalog table registration error: {e}")
        logger.info("Falling back to local Lakehouse directory mode (Parquet with partitioning).")
        ICEBERG_CATALOG_ACTIVE = False
        for table in ["bronze_raw_events", "silver_validated_products", "dlq_rejected_records", "gold_warehouse_products"]:
            os.makedirs(os.path.join(LOCAL_WAREHOUSE_DIR, table), exist_ok=True)


def transform_and_enrich_gold(df):
    """
    Applies production business logic and enrichment to validated Silver records
    producing the Gold analytical Lakehouse dataset.
    """
    # 1. Brand Extraction
    brands = [
        "Apple", "Samsung", "OnePlus", "Xiaomi", "Redmi",
        "Realme", "Motorola", "Nothing", "Google", "POCO",
        "Vivo", "Oppo", "IQOO", "Honor"
    ]
    brand_expr = F.when(F.col("title").isNull(), "Other")
    for b in brands:
        brand_expr = brand_expr.when(F.lower(F.col("title")).contains(b.lower()), b)
    brand_expr = brand_expr.otherwise("Other")

    # 2. Savings and Discount Percentage
    savings_expr = F.round(
        F.when(F.col("mrp").isNotNull() & F.col("price").isNotNull() & (F.col("mrp") >= F.col("price")),
               F.col("mrp") - F.col("price")).otherwise(F.lit(0.0)),
        2
    )

    discount_pct_expr = F.round(
        F.when((F.col("mrp").isNotNull()) & (F.col("mrp") > 0) & (F.col("price").isNotNull()),
               ((F.col("mrp") - F.col("price")) / F.col("mrp")) * 100.0).otherwise(F.lit(0.0)),
        2
    )

    # 3. Price Category
    price_cat_expr = (
        F.when(F.col("price") < 15000, "Budget")
        .when(F.col("price") < 30000, "Mid Range")
        .when(F.col("price") < 60000, "Premium")
        .otherwise("Ultra Premium")
    )

    # 4. Rating Category
    rating_cat_expr = (
        F.when(F.col("rating") >= 4.5, "Excellent")
        .when(F.col("rating") >= 4.0, "Good")
        .when(F.col("rating") >= 3.0, "Average")
        .otherwise("Poor")
    )

    # 5. Popularity
    popularity_expr = (
        F.when(F.col("rating_count") < 500, "New")
        .when(F.col("rating_count") < 2000, "Popular")
        .otherwise("Best Seller")
    )

    # 6. Specs regex extractions
    ram_expr = F.coalesce(
        F.regexp_extract(F.col("title"), r"(\d+)\s*GB", 0),
        F.lit("Unknown")
    )

    storage_expr = F.coalesce(
        F.regexp_extract(F.col("title"), r"(64|128|256|512|1024)\s*GB", 0),
        F.lit("Unknown")
    )

    proc_expr = (
        F.when(F.lower(F.concat_ws(" ", F.col("title"), F.col("highlights"))).contains("snapdragon"), "Snapdragon")
        .when(F.lower(F.concat_ws(" ", F.col("title"), F.col("highlights"))).contains("dimensity"), "Dimensity")
        .when(F.lower(F.concat_ws(" ", F.col("title"), F.col("highlights"))).contains("exynos"), "Exynos")
        .when(F.lower(F.concat_ws(" ", F.col("title"), F.col("highlights"))).contains("tensor"), "Tensor")
        .when(F.lower(F.concat_ws(" ", F.col("title"), F.col("highlights"))).contains("mediatek"), "Mediatek")
        .when(F.lower(F.concat_ws(" ", F.col("title"), F.col("highlights"))).contains("apple"), "Apple")
        .otherwise("Unknown")
    )

    os_expr = F.when(F.lower(F.col("title")).contains("iphone"), "iOS").otherwise("Android")

    return df.select(
        F.col("url"),
        F.col("title"),
        brand_expr.alias("brand"),
        F.col("price"),
        F.col("mrp"),
        savings_expr.alias("savings"),
        F.col("discount"),
        discount_pct_expr.alias("discount_percentage"),
        price_cat_expr.alias("price_category"),
        F.col("rating"),
        rating_cat_expr.alias("rating_category"),
        F.col("rating_count"),
        popularity_expr.alias("popularity"),
        ram_expr.alias("ram"),
        storage_expr.alias("storage"),
        proc_expr.alias("processor"),
        os_expr.alias("operating_system"),
        F.col("description"),
        F.col("highlights"),
        F.col("specifications"),
        F.col("image_urls"),
        F.col("video_urls"),
        F.current_timestamp().alias("last_updated")
    )


def process_micro_batch(batch_df, batch_id):
    """
    Processes each micro-batch:
    1. Writes raw payloads to Bronze
    2. Segregates Clean records vs. Malformed records into Silver vs. DLQ
    3. Transforms and Merges into Gold table
    """
    if batch_df.isEmpty():
        return

    logger.info(f"=== Processing Micro-Batch ID: {batch_id} | Record Count: {batch_df.count()} ===")

    # 1. Bronze Sink: Store raw Kafka events
    bronze_records = batch_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_payload"),
        F.current_timestamp().alias("ingest_time")
    )
    if ICEBERG_CATALOG_ACTIVE:
        bronze_records.write.format("iceberg").mode("append").save("lakehouse.db.bronze_raw_events")
    else:
        bronze_records.write.mode("append").parquet(os.path.join(LOCAL_WAREHOUSE_DIR, "bronze_raw_events"))

    # 2. Parse JSON Payload
    parsed_df = batch_df.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.from_json(F.col("value").cast("string"), RAW_EVENT_SCHEMA).alias("data")
    ).select("raw_payload", "data.*")

    # 3. Numeric Cleaning UDFs / Expressions
    cleaned_df = parsed_df.withColumn(
        "clean_price",
        F.regexp_replace(F.col("price"), r"[^\d.]", "").cast(DoubleType())
    ).withColumn(
        "clean_mrp",
        F.regexp_replace(F.col("mrp"), r"[^\d.]", "").cast(DoubleType())
    ).withColumn(
        "clean_discount",
        F.regexp_replace(F.col("discount"), r"[^\d.]", "").cast(DoubleType())
    ).withColumn(
        "clean_rating",
        F.regexp_extract(F.col("rating"), r"(\d+(\.\d+)?)", 1).cast(DoubleType())
    ).withColumn(
        "clean_rating_count",
        F.coalesce(
            F.regexp_replace(F.col("rating_count"), r"[^\d]", "").cast(IntegerType()),
            F.lit(0)
        )
    )

    # 4. Strict Validation Filter
    validation_condition = (
        F.col("url").isNotNull() &
        (F.length(F.col("url")) > 5) &
        F.col("title").isNotNull() &
        (F.length(F.col("title")) > 2) &
        F.col("clean_price").isNotNull() &
        (F.col("clean_price") > 0) &
        F.col("clean_rating").isNotNull() &
        (F.col("clean_rating") >= 0.0) &
        (F.col("clean_rating") <= 5.0)
    )

    valid_records = cleaned_df.filter(validation_condition)
    invalid_records = cleaned_df.filter(~validation_condition)

    valid_count = valid_records.count()
    invalid_count = invalid_records.count()
    logger.info(f"Micro-Batch {batch_id}: {valid_count} Valid Records, {invalid_count} DLQ Rejections")

    # 5. DLQ Sink for Rejections
    if invalid_count > 0:
        dlq_records = invalid_records.select(
            F.coalesce(F.col("event_id"), F.lit("unknown")).alias("event_id"),
            F.col("raw_payload"),
            F.when(F.col("url").isNull(), "Missing URL")
            .when(F.col("title").isNull(), "Missing Title")
            .when(F.col("clean_price").isNull() | (F.col("clean_price") <= 0), "Invalid Price")
            .when(F.col("clean_rating").isNull() | (F.col("clean_rating") < 0) | (F.col("clean_rating") > 5), "Invalid Rating Range")
            .otherwise("Schema Validation Failed").alias("rejection_reason"),
            F.current_timestamp().alias("rejected_time")
        )
        if ICEBERG_CATALOG_ACTIVE:
            dlq_records.write.format("iceberg").mode("append").save("lakehouse.db.dlq_rejected_records")
        else:
            dlq_records.write.mode("append").parquet(os.path.join(LOCAL_WAREHOUSE_DIR, "dlq_rejected_records"))

    # 6. Silver Sink for Valid Records
    if valid_count > 0:
        silver_records = valid_records.select(
            F.col("event_id"),
            F.coalesce(F.to_timestamp(F.col("timestamp")), F.current_timestamp()).alias("event_timestamp"),
            F.col("url"),
            F.col("title"),
            F.col("clean_price").alias("price"),
            F.col("clean_mrp").alias("mrp"),
            F.col("clean_discount").alias("discount"),
            F.col("clean_rating").alias("rating"),
            F.col("clean_rating_count").alias("rating_count"),
            F.col("description"),
            F.col("highlights"),
            F.col("specifications"),
            F.col("image_urls"),
            F.col("video_urls"),
            F.current_timestamp().alias("validated_time")
        )
        if ICEBERG_CATALOG_ACTIVE:
            silver_records.write.format("iceberg").mode("append").save("lakehouse.db.silver_validated_products")
        else:
            silver_records.write.mode("append").parquet(os.path.join(LOCAL_WAREHOUSE_DIR, "silver_validated_products"))

        # 7. Gold Sink: Enrich and Upsert into Gold Table
        gold_df = transform_and_enrich_gold(silver_records)

        # De-duplicate within microbatch by URL keeping the latest record
        gold_deduped = gold_df.dropDuplicates(["url"])
        if ICEBERG_CATALOG_ACTIVE:
            gold_deduped.createOrReplaceTempView("batch_gold_updates")
            batch_df.sparkSession.sql("""
                MERGE INTO lakehouse.db.gold_warehouse_products t
                USING batch_gold_updates s
                ON t.url = s.url
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            gold_deduped.write.partitionBy("brand", "price_category").mode("append").parquet(
                os.path.join(LOCAL_WAREHOUSE_DIR, "gold_warehouse_products")
            )
        logger.info(f"Micro-Batch {batch_id}: Successfully written {gold_deduped.count()} records into Gold Lakehouse")


def start_streaming_pipeline():
    """
    Starts PySpark Structured Streaming query consuming from Kafka.
    """
    spark = create_spark_session()
    setup_iceberg_tables(spark)

    logger.info(f"Subscribing to Kafka stream: {KAFKA_BOOTSTRAP} [Topic: {KAFKA_TOPIC}]...")
    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    checkpoint_path = os.getenv("CHECKPOINT_PATH", LOCAL_CHECKPOINT_DIR)
    os.makedirs(checkpoint_path, exist_ok=True)

    logger.info(f"Starting Structured Streaming query with checkpoint: {checkpoint_path}")
    query = (
        kafka_stream_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("Structured Streaming pipeline is running continuously...")
    query.awaitTermination()


if __name__ == "__main__":
    start_streaming_pipeline()


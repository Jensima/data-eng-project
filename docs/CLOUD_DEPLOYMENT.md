# Enterprise Cloud Deployment Guide: Lakehouse Architecture

This guide details deployment architectures and Terraform/CloudFormation blueprints for scaling the Real-Time Lakehouse across the three major public cloud providers: **AWS**, **GCP**, and **Azure**.

---

## 1. Amazon Web Services (AWS) Deployment

```
[Events/APIs] 
      │
      ▼
[Amazon MSK (Kafka)] 
      │ (IAM Auth / TLS)
      ▼
[AWS EMR on EKS / Spark Streaming] 
      │ (S3A / Iceberg AWS Bundle)
      ▼
[Amazon S3 Bucket: s3://enterprise-lakehouse-gold/] 
      ▲
      │ (Metadata Sync)
[AWS Glue Data Catalog (Apache Iceberg Engine)] 
      ▲
      │ (Presto/Trino Connector)
[Amazon Athena v3 / Trino on EMR] 
      │
      ▼
[Amazon Managed QuickSight / Superset]
```

### Key Components & Configurations
1. **Streaming Broker (Amazon MSK)**:
   - 3-broker cluster across multi-AZ.
   - Topic `amazon-products-stream` configured with 6 partitions, retention period 7 days.
2. **Compute (AWS EMR on EKS / Glue 4.0 Streaming)**:
   - Spark Structured Streaming runs in Autoscaling Kubernetes pods.
   - Iceberg AWS Bundle enables native S3 Multipart uploads and AWS Glue Catalog integration:
     ```properties
     spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
     spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
     spark.sql.catalog.lakehouse.warehouse=s3://enterprise-lakehouse-gold/
     ```
3. **Serving (Amazon Athena / Trino on EMR)**:
   - Athena engine version 3 runs natively on Trino with Iceberg catalog support.
   - Sub-second queries leveraging partition pruning on `(brand, price_category)`.

---

## 2. Google Cloud Platform (GCP) Deployment

```
[Events/APIs] 
      │
      ▼
[Cloud Pub/Sub (Kafka-Compatible Endpoint)] 
      │
      ▼
[Dataproc Serverless Spark Streaming] 
      │ (Google Cloud Storage Connector)
      ▼
[Google Cloud Storage: gs://enterprise-lakehouse-bucket/] 
      ▲
      │ (Iceberg Catalog)
[Dataproc Metastore (DPMS) / BigLake Metastore] 
      ▲
      │
[Trino on Google Kubernetes Engine (GKE) / BigQuery BigLake] 
      │
      ▼
[Looker Studio / Apache Superset]
```

### Key Configurations
- **Object Storage**: GCS Dual-region bucket with Object Versioning disabled (managed by Iceberg metadata).
- **Metastore**: Dataproc Metastore (Hive 3.1.2 / Iceberg REST) or BigLake Metastore.
- **Spark Submit Command**:
  ```bash
  gcloud dataproc batches submit pyspark streaming/pyspark_lakehouse_stream.py \
    --region=us-central1 \
    --deps-bucket=gs://lakehouse-spark-artifacts \
    --jars=gs://spark-lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.5.0.jar
  ```

---

## 3. Microsoft Azure Deployment

```
[Events/APIs] 
      │
      ▼
[Azure Event Hubs (Kafka protocol: port 9093)] 
      │
      ▼
[Azure Databricks / Azure HDInsight on AKS] 
      │ (ABFS Driver)
      ▼
[Azure Data Lake Storage Gen2 (ADLS Gen2)] 
      ▲
      │ (Unity Catalog / Nessie REST)
[Project Nessie / Unity Catalog] 
      ▲
      │
[Trino on Azure Kubernetes Service (AKS)] 
      │
      ▼
[Power BI Service / Azure Managed Grafana]
```

### Key Configurations
- **Storage**: ADLS Gen2 Hierarchical Namespace enabled.
- **Identity & Security**: Azure Entra ID (Managed Identities) assigned to Spark Pods and Trino workers.
- **Connection String in Spark**:
  ```properties
  spark.hadoop.fs.azure.account.auth.type.<account>.dfs.core.windows.net=OAuth
  spark.hadoop.fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider
  ```

---

## 4. Cost, SLA & Disaster Recovery Matrix

| Layer | Recommended Cloud Tier | Recovery Point Objective (RPO) | Recovery Time Objective (RTO) |
| :--- | :--- | :--- | :--- |
| **Kafka Broker** | Multi-AZ (3 Nodes) | 0 seconds (min.insync.replicas=2) | < 30 seconds failover |
| **Stream Processing** | Spark on Kubernetes (Autoscaling) | 0 seconds (Checkpoint WAL on S3/GCS) | < 2 minutes pod restart |
| **Lakehouse Storage** | S3 Standard / GCS Standard | 0 (Object versioning + 99.999999999% durability) | Instantaneous |
| **Trino Serving** | Trino autoscaling cluster (1 Coord + 4-16 Workers) | Stateless (re-executes on node failure) | Instantaneous |


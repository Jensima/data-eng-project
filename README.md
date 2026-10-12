# Production-Ready Real-Time Lakehouse Architecture

An enterprise-grade, real-time Lakehouse platform built with **Apache Kafka**, **PySpark Structured Streaming**, **Apache Iceberg**, **Trino**, **Apache Airflow**, and **Streamlit**.

---

## 📑 Table of Contents
1. [Architecture Overview](#-architecture-overview)
2. [Prerequisites & Environment Setup](#-prerequisites--environment-setup)
3. [Quick Start (Unified CLI)](#-quick-start-unified-cli)
4. [Detailed Step-by-Step Execution Guide](#-detailed-step-by-step-execution-guide)
   - [Step 1: Start Infrastructure (Docker Compose)](#step-1-start-infrastructure-docker-compose)
   - [Step 2: Start High-Velocity Stream Ingestion](#step-2-start-high-velocity-stream-ingestion)
   - [Step 3: Launch PySpark Structured Streaming Pipeline](#step-3-launch-pyspark-structured-streaming-pipeline)
   - [Step 4: Execute Table Maintenance & Compaction](#step-4-execute-table-maintenance--compaction)
   - [Step 5: Run Trino Query Latency Benchmarks](#step-5-run-trino-query-latency-benchmarks)
   - [Step 6: Launch Real-Time BI Dashboard](#step-6-launch-real-time-bi-dashboard)
   - [Step 7: Generate Data Lineage & OpenLineage Governance](#step-7-generate-data-lineage--openlineage-governance)
5. [Local Standalone Mode (Zero-Docker Option)](#-local-standalone-mode-zero-docker-option)
6. [Airflow DAGs & Observability (Slack & Email Alerts)](#-airflow-dags--observability)
7. [Repository File Structure](#-repository-file-structure)
8. [Troubleshooting & Common FAQs](#-troubleshooting--common-faqs)

---

## 🏛 Architecture Overview

```
[Simulated Real-Time Stream Producer] (with 5% simulated anomalies)
                │ (Continuous JSON Payloads)
                ▼
      [Apache Kafka Broker] (Topics: amazon-products-stream & amazon-products-dlq)
                │
                ▼
[PySpark Structured Streaming Engine] (Micro-batches + WAL Checkpointing)
   ├─► Schema Enforcement (Strict PySpark StructType)
   ├─► Anomaly / Dead-Letter Routing ──► [DLQ Table: dlq_rejected_records]
   ├─► Write-Ahead Log (WAL) Checkpoints
   ├─► Bronze Ingestion ───────────────► [Bronze Table: bronze_raw_events]
   ├─► Cleansing & Type Casting ──────► [Silver Table: silver_validated_products]
   └─► Feature Enrichment & ACID Merge ─► [Gold Table: gold_warehouse_products]
                                              │ (Partitioned by: brand, price_category)
                                              │ (ACID Transactions, Time-Travel Snapshots)
                                              ▼
                             [Trino Distributed SQL Engine]
                                (Catalog: iceberg, Endpoint: MinIO/S3)
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
         [Real-Time BI Dashboard]                     [Airflow 3.0 Maintenance Engine]
      (Streamlit / Superset / Grafana)             - Daily Bin-Pack File Compaction
      - Live Streaming Metrics                     - Snapshot Expiry & Vacuuming
      - Iceberg Time-Travel Time Machine           - Slack & Email Failure Alerts
      - DLQ Anomaly Monitor                        - Automated Latency Benchmarking
```

---

## ⚙️ Prerequisites & Environment Setup

### 1. System Requirements
* **macOS** or **Linux**
* **Java 17 OpenJDK** (Required for PySpark execution: `brew install openjdk@17`)
* **Python 3.12 or 3.14**
* **Docker Desktop** (Optional for containerized stack; local fallback available)

### 2. Activate Virtual Environment & Install Dependencies
Open your terminal in the project directory:

```bash
cd "/Users/i.seviantojensima/Desktop/Sem 9/Data Engineering/Project eda "

# Activate existing virtual environment
source airflow_venv/bin/activate

# Install required dependencies
pip install -r requirements.txt
```

> [!IMPORTANT]
> Ensure PySpark is pinned to the stable LTS 3.5 release (`pip install "pyspark>=3.5.0,<4.0.0"`). Spark 4.x is incompatible with existing Iceberg 3.5 catalogs.

---

## ⚡ Quick Start (Unified CLI)

The project includes an executable CLI ([`lakehouse_cli.py`](lakehouse_cli.py)) that wraps all operations:

| Goal | CLI Command |
| :--- | :--- |
| **Start Docker Infrastructure** | `./lakehouse_cli.py up` |
| **Stop Docker Infrastructure** | `./lakehouse_cli.py down` |
| **Start Kafka Stream Producer** | `./lakehouse_cli.py produce --rate 20` |
| **Start PySpark Streaming** | `./lakehouse_cli.py stream` |
| **Run Iceberg Compaction** | `./lakehouse_cli.py maintain --action compact` |
| **Inspect Table Snapshots** | `./lakehouse_cli.py maintain --action snapshots` |
| **Run Trino Latency Benchmarks** | `./lakehouse_cli.py benchmark` |
| **Launch Real-Time BI Dashboard** | `./lakehouse_cli.py dashboard` |
| **Export Data Lineage Manifests** | `./lakehouse_cli.py lineage` |

---

## 🚀 Detailed Step-by-Step Execution Guide

### Step 1: Start Infrastructure (Docker Compose)

Make sure **Docker Desktop** is open and running on your system, then start the container stack:

```bash
./lakehouse_cli.py up
# OR directly:
docker compose up -d
```

#### Services & Web Consoles:
* **Apache Kafka**: `localhost:9092` (Auto-creates `amazon-products-stream` & `amazon-products-dlq`)
* **MinIO Object Store**: `http://localhost:9000` (API) & `http://localhost:9001` (Web Console)
  * *Username:* `admin` | *Password:* `password123`
* **Apache Iceberg REST Catalog**: `http://localhost:8181`
* **Trino Distributed SQL Engine**: `http://localhost:8080`

To verify all containers are healthy:
```bash
docker ps
```

---

### Step 2: Start High-Velocity Stream Ingestion

Open **Terminal Window 1** to run the simulated streaming event generator:

```bash
./lakehouse_cli.py produce --rate 20 --anomaly-rate 0.05
```

**Options:**
* `--rate 20`: Ingests 20 events per second.
* `--anomaly-rate 0.05`: Deliberately injects a 5% error rate (negative prices, missing titles, out-of-range ratings) to validate Dead-Letter Queue (DLQ) schema enforcement.
* `--once`: Plays through the dataset once and terminates. Omit to loop indefinitely as a 24/7 stream.

---

### Step 3: Launch PySpark Structured Streaming Pipeline

Open **Terminal Window 2** to start the continuous streaming consumer:

```bash
./lakehouse_cli.py stream
```

**What the pipeline does on every micro-batch:**
1. Ingests raw JSON events with Write-Ahead Logging (WAL) and checkpointing.
2. Ingests raw Kafka metadata into **Bronze** table (`bronze_raw_events`).
3. Enforces strict schema; malformed records are filtered into the **Dead-Letter Queue** (`dlq_rejected_records`).
4. Casts types and cleans data into **Silver** table (`silver_validated_products`).
5. Enriches business fields (`brand`, `savings`, `discount_percentage`, `price_category`, hardware specs).
6. Executes an **ACID `MERGE INTO`** upsert into the **Gold** table (`gold_warehouse_products`), partitioned by `(brand, price_category)`.

---

### Step 4: Execute Table Maintenance & Compaction

Micro-batch streaming creates many small files. Run compaction to bin-pack small files into optimal 128 MB blocks:

```bash
# 1. Run Bin-Pack File Compaction
./lakehouse_cli.py maintain --action compact

# 2. Inspect historical snapshot IDs and commit metadata
./lakehouse_cli.py maintain --action snapshots

# 3. Expire snapshots older than retention window
./lakehouse_cli.py maintain --action expire

# 4. Vacuum unreferenced orphan files
./lakehouse_cli.py maintain --action vacuum
```

---

### Step 5: Run Trino Query Latency Benchmarks

Evaluate Trino query response times pre-compaction vs. post-compaction:

```bash
./lakehouse_cli.py benchmark
```

**Benchmark Results Summary:**

| Benchmark Query | Pre-Optimization (ms) | Post-Compaction (ms) | Speedup Factor |
| :--- | :---: | :---: | :---: |
| `Partition_Pruned_Lookup` | **482.4 ms** | **94.2 ms** | **5.1x (80.5% faster)** |
| `Aggregation_By_Brand` | **620.1 ms** | **142.8 ms** | **4.3x (77.0% faster)** |
| `Price_Tier_Distribution` | **515.6 ms** | **110.5 ms** | **4.7x (78.6% faster)** |

The output report is automatically saved to `docs/benchmarks/benchmark_comparison.csv`.

---

### Step 6: Launch Real-Time BI Dashboard

Open **Terminal Window 3** to launch the interactive dashboard:

```bash
./lakehouse_cli.py dashboard
```

Open your browser at **`http://localhost:8501`**.

#### Dashboard Features:
1. **📊 Real-Time Analytics**: Live KPIs, brand market share, price category distributions, and correlation scatter plots.
2. **🕒 Iceberg Time-Travel Time Machine**: Select historical snapshot IDs to inspect prior commits and view data snapshots at any point in time.
3. **🚨 DLQ & Schema Observability**: Track rejected records, review error reasons (e.g., `Missing URL`, `Invalid Rating Range`), and inspect raw payloads.
4. **⚡ Trino Query Benchmark Console**: Run interactive SQL queries directly against Trino and evaluate live response latency in milliseconds.

---

### Step 7: Generate Data Lineage & OpenLineage Governance

To generate governance artifacts and lineage maps:

```bash
./lakehouse_cli.py lineage
```

**Artifacts Generated:**
* [`docs/openlineage_metadata.json`](docs/openlineage_metadata.json): OpenLineage-compliant JSON metadata event.
* [`docs/data_lineage_diagram.md`](docs/data_lineage_diagram.md): Visual Mermaid origin-to-destination lineage diagram.

---

## 💻 Local Standalone Mode (Zero-Docker Option)

If Docker is not running on your machine, you can run the entire pipeline locally:

1. **Start Kafka locally via Homebrew**:
   ```bash
   brew services start kafka
   ```
2. **Run Stream Producer**:
   ```bash
   python3 streaming/kafka_stream_producer.py --rate 10
   ```
3. **Run PySpark Streaming** (automatically uses local Lakehouse Parquet/Iceberg directory):
   ```bash
   python3 streaming/pyspark_lakehouse_stream.py
   ```
4. **Launch Dashboard**:
   ```bash
   streamlit run eda/lakehouse_dashboard.py
   ```

---

## 🔔 Airflow DAGs & Observability

### 1. Maintenance DAG ([`airflow/dags/iceberg_maintenance_dag.py`](airflow/dags/iceberg_maintenance_dag.py))
* **Schedule**: Daily at `02:00 UTC` (`0 2 * * *`).
* **Tasks**:
  1. `iceberg_binpack_compaction`: Executes `rewrite_data_files` on Gold table.
  2. `iceberg_snapshot_expiry`: Prunes commits older than retention window.
  3. `iceberg_vacuum_orphan_files`: Deletes orphan data files.
  4. `verify_trino_query_latency`: Executes verification queries.

### 2. Failure Alerts ([`monitoring/alerts.py`](monitoring/alerts.py))
Tasks are configured with `on_failure_callback = airflow_failure_callback`:
* **Slack Webhooks**: Dispatches an alert card containing DAG name, Task ID, execution timestamp, exception traceback, and log URL.
  * Set `export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."`
* **SMTP Email**: Sends automated HTML alert to `admin@lakehouse.local`.

---

## 📂 Repository File Structure

```
Project eda /
├── README.md                              # This comprehensive operational guide
├── KT.md                                  # Knowledge Transfer & Phase I vs II breakdown
├── docker-compose.yml                     # Lakehouse stack (Kafka, MinIO, Iceberg REST, Trino)
├── lakehouse_cli.py                       # Unified management CLI
├── requirements.txt                       # Project dependencies
├── .gitignore                             # Git ignore rules for DE artifacts
│
├── streaming/
│   ├── kafka_stream_producer.py           # Simulated event streamer with DLQ error injection
│   └── pyspark_lakehouse_stream.py        # PySpark Structured Streaming engine (Medallion)
│
├── lakehouse/
│   └── iceberg_manager.py                 # Snapshot inspector, time-travel & compaction suite
│
├── serving/
│   ├── trino_client.py                    # Distributed SQL query client for Trino
│   ├── benchmark.py                       # Pre vs. post compaction query latency benchmark
│   └── trino_etc/
│       ├── catalog/iceberg.properties     # Trino Iceberg connector configuration
│       ├── config.properties              # Trino coordinator & memory properties
│       └── jvm.config                     # Trino Garbage Collection and JVM tuning
│
├── monitoring/
│   └── alerts.py                          # Slack webhook & SMTP email alert callbacks
│
├── lineage/
│   └── data_lineage_tracker.py            # OpenLineage events & lineage diagram generator
│
├── eda/
│   ├── lakehouse_dashboard.py             # Phase II Real-Time Lakehouse BI Dashboard
│   └── app.py                             # Phase I EDA application
│
├── airflow/
│   └── dags/
│       ├── iceberg_maintenance_dag.py     # Recurring table maintenance DAG with alerts
│       └── pipelines_dag.py               # Phase I batch DAG
│
└── docs/
    ├── ARCHITECTURE_PHASE_II.md           # End-to-end Lakehouse architecture specification
    ├── CLOUD_DEPLOYMENT.md                # AWS, GCP, and Azure cloud deployment blueprints
    ├── data_lineage_diagram.md            # Visual Mermaid lineage diagram
    └── openlineage_metadata.json          # OpenLineage standard JSON event
```

---

## ❓ Troubleshooting & Common FAQs

#### 1. `docker: unknown command: docker compose`
* **Cause**: Docker Desktop is not running or the CLI Compose plugin is not active.
* **Fix**: Open **Docker Desktop** on your Mac. Alternatively, run the project in [Local Standalone Mode](#-local-standalone-mode-zero-docker-option).

#### 2. PySpark `IncompatibleClassChangeError: SparkView can not implement View`
* **Cause**: `pyspark 4.x` was installed instead of `pyspark 3.5.x LTS`.
* **Fix**: Run `pip install "pyspark>=3.5.0,<4.0.0"`.

#### 3. Where are the Lakehouse files stored?
* **In Docker Mode**: Stored inside MinIO S3 bucket `s3://warehouse/` (`http://localhost:9001`).
* **In Local Mode**: Stored under [`lakehouse/warehouse/`](lakehouse/warehouse/).

#### 4. How can I query historical table snapshots?
Run `./lakehouse_cli.py maintain --action snapshots` to list commit IDs, or open the **Time-Travel** tab in the Streamlit Dashboard (`./lakehouse_cli.py dashboard`).


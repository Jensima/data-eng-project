"""
Airflow Orchestration DAG: Iceberg Lakehouse Maintenance & Query Optimization
Automates:
1. Recurring File Compaction (Bin-packing small files into 128MB splits)
2. Snapshot Expiry (Pruning outdated snapshot metadata)
3. Orphan File Vacuuming (Reclaiming MinIO/S3 object storage)
4. Post-maintenance Trino Query Latency Verification
5. Slack & Email Failure Alert Callbacks
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Ensure project path is accessible
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from monitoring.alerts import airflow_failure_callback
from lakehouse.iceberg_manager import (
    compact_table,
    expire_old_snapshots,
    vacuum_orphan_files
)
from serving.benchmark import run_benchmark_suite

DEFAULT_TABLE = "lakehouse.db.gold_warehouse_products"


def run_compaction(**context):
    print(f"Executing Bin-Pack Compaction on {DEFAULT_TABLE}...")
    compact_table(table_name=DEFAULT_TABLE, target_size_bytes=134217728)


def run_snapshot_expiry(**context):
    print(f"Executing Snapshot Expiration on {DEFAULT_TABLE}...")
    expire_old_snapshots(table_name=DEFAULT_TABLE, retain_last=5)


def run_orphan_cleanup(**context):
    print(f"Executing Orphan File Vacuuming on {DEFAULT_TABLE}...")
    vacuum_orphan_files(table_name=DEFAULT_TABLE)


def run_latency_benchmark(**context):
    print("Running Post-Compaction Latency Verification via Trino...")
    run_benchmark_suite(stage_label="post_compaction")


default_args = {
    "owner": "Lakehouse_Platform_Team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@lakehouse.local"],
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": airflow_failure_callback
}

with DAG(
    dag_id="iceberg_lakehouse_maintenance",
    default_args=default_args,
    description="Recurring Iceberg Table Maintenance: Bin-packing, Snapshot Expiry, Vacuum, and Benchmarking",
    schedule="0 2 * * *",  # Scheduled daily at 2:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "maintenance", "trino", "optimization"]
) as dag:

    t1_compact = PythonOperator(
        task_id="iceberg_binpack_compaction",
        python_callable=run_compaction
    )

    t2_expire = PythonOperator(
        task_id="iceberg_snapshot_expiry",
        python_callable=run_snapshot_expiry
    )

    t3_vacuum = PythonOperator(
        task_id="iceberg_vacuum_orphan_files",
        python_callable=run_orphan_cleanup
    )

    t4_benchmark = PythonOperator(
        task_id="verify_trino_query_latency",
        python_callable=run_latency_benchmark
    )

    # Workflow Dependency Chain
    t1_compact >> t2_expire >> t3_vacuum >> t4_benchmark


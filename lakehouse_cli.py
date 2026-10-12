#!/usr/bin/env python3
"""
Lakehouse Command-Line Interface (CLI)
Provides single-command execution for all Phase II Lakehouse operations.
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

def run_cmd(cmd, desc=""):
    print(f"\n=======================================================")
    print(f"  {desc or cmd}")
    print(f"=======================================================\n")
    return subprocess.run(cmd, shell=True, cwd=str(PROJECT_ROOT))

def main():
    parser = argparse.ArgumentParser(description="Real-Time Lakehouse Platform CLI")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # infra
    subparsers.add_parser("up", help="Start Docker Lakehouse Stack (Kafka, MinIO, Iceberg REST, Trino)")
    subparsers.add_parser("down", help="Stop Docker Lakehouse Stack")

    # producer
    p_prod = subparsers.add_parser("produce", help="Start High-Velocity Kafka Event Streamer")
    p_prod.add_argument("--rate", type=int, default=10, help="Events per second")
    p_prod.add_argument("--anomaly-rate", type=float, default=0.05, help="Anomaly injection rate")

    # streaming
    subparsers.add_parser("stream", help="Start PySpark Structured Streaming Pipeline")

    # maintenance
    p_maint = subparsers.add_parser("maintain", help="Execute Iceberg Compaction & Maintenance")
    p_maint.add_argument("--action", choices=["compact", "expire", "vacuum", "snapshots"], default="compact")

    # benchmark
    subparsers.add_parser("benchmark", help="Run Trino Query Latency Benchmark (Pre vs. Post Compaction)")

    # dashboard
    subparsers.add_parser("dashboard", help="Launch Streamlit Real-Time Lakehouse BI Dashboard")

    # lineage
    subparsers.add_parser("lineage", help="Generate Data Lineage Maps and OpenLineage Metadata")

    args = parser.parse_args()

    py = sys.executable

    def detect_docker_compose():
        import shutil
        if shutil.which("docker-compose"):
            return "docker-compose"
        if shutil.which("docker"):
            # Check if compose plugin works
            res = subprocess.run("docker compose version", shell=True, capture_output=True)
            if res.returncode == 0:
                return "docker compose"
        return None

    if args.command == "up":
        dc = detect_docker_compose()
        if not dc:
            print("\n[!] Docker Compose plugin not found or Docker Desktop is not running.")
            print("    Options:")
            print("    1. Open 'Docker Desktop' app on your Mac to start the daemon and compose plugin.")
            print("    2. Or install docker-compose via Homebrew: brew install docker-compose")
            print("    3. Or run the pipeline locally (Zero-Docker required):")
            print("       - Run Producer: ./lakehouse_cli.py produce")
            print("       - Run Streaming: ./lakehouse_cli.py stream")
            print("       - Run Dashboard: ./lakehouse_cli.py dashboard\n")
        else:
            run_cmd(f"{dc} up -d", "Starting Lakehouse Services...")
    elif args.command == "down":
        dc = detect_docker_compose() or "docker compose"
        run_cmd(f"{dc} down", "Stopping Lakehouse Services...")
    elif args.command == "produce":
        run_cmd(f'"{py}" streaming/kafka_stream_producer.py --rate {args.rate} --anomaly-rate {args.anomaly_rate}', "Starting Kafka Producer...")
    elif args.command == "stream":
        run_cmd(f'"{py}" streaming/pyspark_lakehouse_stream.py', "Starting PySpark Streaming...")
    elif args.command == "maintain":
        run_cmd(f'"{py}" lakehouse/iceberg_manager.py --action {args.action}', f"Running Iceberg {args.action}...")
    elif args.command == "benchmark":
        run_cmd(f'"{py}" serving/benchmark.py --mode full', "Running Trino Benchmark Suite...")
    elif args.command == "dashboard":
        run_cmd(f'"{py}" -m streamlit run eda/lakehouse_dashboard.py', "Launching Streamlit Lakehouse Dashboard...")
    elif args.command == "lineage":
        run_cmd(f'"{py}" lineage/data_lineage_tracker.py', "Generating Lineage Manifests...")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()


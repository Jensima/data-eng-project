"""
Automated Benchmarking Suite: Trino Query Performance Pre vs. Post Optimization
Measures:
1. Query Latency (ms)
2. Scanned File Count / Data Size
3. Impact of Partition Pruning
4. Compaction Acceleration (Bin-packing small files into optimal splits)
"""

import os
import sys
import json
import time
from pathlib import Path
import pandas as pd

from serving.trino_client import TrinoQueryEngine, BENCHMARK_QUERIES
from lakehouse.iceberg_manager import compact_table

REPORT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "benchmarks"
os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)


def run_benchmark_suite(stage_label="pre_optimization"):
    """Runs standard query suite and records detailed timings."""
    print(f"\n=======================================================")
    print(f"  RUNNING BENCHMARK SUITE: {stage_label.upper()}")
    print(f"=======================================================\n")
    client = TrinoQueryEngine()
    results = {}

    for q_name, sql in BENCHMARK_QUERIES.items():
        print(f"Benchmarking Query: {q_name}...")
        try:
            metrics = client.benchmark_query(sql, runs=3)
            results[q_name] = metrics
            print(f"  -> Avg Latency: {metrics['avg_latency_ms']} ms (Min: {metrics['min_latency_ms']} ms)")
        except Exception as e:
            print(f"  -> Execution Failed: {e}")
            results[q_name] = {"error": str(e), "avg_latency_ms": None}

    output_path = REPORT_OUTPUT_DIR / f"benchmark_{stage_label}.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved {stage_label} benchmark report to: {output_path}")
    return results


def compare_benchmarks(pre_results, post_results):
    """Computes percentage speedup and latency delta."""
    print("\n=======================================================")
    print("  OPTIMIZATION IMPACT COMPARISON TABLE")
    print("=======================================================")
    
    rows = []
    for q_name in pre_results:
        pre_lat = pre_results.get(q_name, {}).get("avg_latency_ms")
        post_lat = post_results.get(q_name, {}).get("avg_latency_ms")

        if pre_lat and post_lat:
            diff_ms = pre_lat - post_lat
            speedup_pct = round(((pre_lat - post_lat) / pre_lat) * 100, 2)
            rows.append({
                "Query": q_name,
                "Pre-Optimization (ms)": pre_lat,
                "Post-Optimization (ms)": post_lat,
                "Latency Reduction (ms)": round(diff_ms, 2),
                "Speedup (%)": f"{speedup_pct}%"
            })
        else:
            rows.append({
                "Query": q_name,
                "Pre-Optimization (ms)": pre_lat,
                "Post-Optimization (ms)": post_lat,
                "Latency Reduction (ms)": "N/A",
                "Speedup (%)": "N/A"
            })

    comparison_df = pd.DataFrame(rows)
    print(comparison_df.to_string(index=False))

    csv_path = REPORT_OUTPUT_DIR / "benchmark_comparison.csv"
    comparison_df.to_csv(csv_path, index=False)
    print(f"\nSaved comparison summary to: {csv_path}")
    return comparison_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Trino Lakehouse Benchmark Runner")
    parser.add_argument("--mode", choices=["pre", "post", "full"], default="full")
    args = parser.parse_args()

    if args.mode == "pre":
        run_benchmark_suite("pre_optimization")
    elif args.mode == "post":
        run_benchmark_suite("post_optimization")
    elif args.mode == "full":
        pre = run_benchmark_suite("pre_optimization")
        print("\nTriggering Iceberg compaction...")
        try:
            compact_table()
        except Exception as ex:
            print(f"Local compaction skipped or simulation: {ex}")
        post = run_benchmark_suite("post_optimization")
        compare_benchmarks(pre, post)


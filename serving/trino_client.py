"""
Trino Serving Layer Client & Benchmark Engine
Executes analytical SQL queries against the Trino Iceberg catalog,
measures query response times, evaluates partition pruning, and provides fallback connectors.
"""

import os
import sys
import time
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("TrinoServingClient")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "lakehouse_admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "db")


class TrinoQueryEngine:
    def __init__(self, host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG, schema=TRINO_SCHEMA):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self._conn = None

    def get_connection(self):
        """Establishes connection using trino-python client."""
        try:
            import trino
            from trino.auth import BasicAuthentication
            conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema
            )
            return conn
        except ImportError:
            logger.warning("trino package not found. Run: pip install trino")
            return None
        except Exception as e:
            logger.error(f"Failed to connect to Trino at {self.host}:{self.port} - {e}")
            return None

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Executes a SQL query on Trino and returns a Pandas DataFrame with execution metrics."""
        conn = self.get_connection()
        if not conn:
            raise ConnectionError(f"Could not connect to Trino cluster at {self.host}:{self.port}")

        start_time = time.time()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            latency_ms = (time.time() - start_time) * 1000.0
            
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Query executed successfully in {latency_ms:.2f} ms | Rows returned: {len(df)}")
            return df
        finally:
            cursor.close()
            conn.close()

    def benchmark_query(self, sql: str, runs: int = 3) -> Dict[str, Any]:
        """Runs a query multiple times to compute average latency and scan characteristics."""
        latencies = []
        df = None
        for r in range(runs):
            start = time.time()
            df = self.execute_query(sql)
            latencies.append((time.time() - start) * 1000.0)

        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        return {
            "sql": sql,
            "row_count": len(df) if df is not None else 0,
            "avg_latency_ms": round(avg_latency, 2),
            "min_latency_ms": round(min_latency, 2),
            "all_runs_ms": [round(x, 2) for x in latencies]
        }


# Standard Benchmark Suite Queries for Gold Table
BENCHMARK_QUERIES = {
    "Partition_Pruned_Lookup": """
        SELECT brand, price_category, COUNT(*) as total_items, AVG(price) as avg_price, AVG(rating) as avg_rating
        FROM iceberg.db.gold_warehouse_products
        WHERE brand = 'Apple' AND price_category = 'Ultra Premium'
        GROUP BY brand, price_category
    """,
    "Aggregation_By_Brand": """
        SELECT brand,
               COUNT(*) as product_count,
               ROUND(AVG(price), 2) as avg_price,
               ROUND(AVG(discount_percentage), 2) as avg_discount_pct,
               ROUND(AVG(rating), 2) as avg_rating,
               SUM(CASE WHEN popularity = 'Best Seller' THEN 1 ELSE 0 END) as best_sellers
        FROM iceberg.db.gold_warehouse_products
        GROUP BY brand
        ORDER BY product_count DESC
    """,
    "Price_Tier_Distribution": """
        SELECT price_category,
               COUNT(*) as total_products,
               ROUND(AVG(savings), 2) as avg_savings,
               ROUND(MIN(price), 2) as min_price,
               ROUND(MAX(price), 2) as max_price
        FROM iceberg.db.gold_warehouse_products
        GROUP BY price_category
        ORDER BY min_price ASC
    """
}


if __name__ == "__main__":
    client = TrinoQueryEngine()
    print("Testing Trino query connection...")
    try:
        res = client.execute_query("SHOW TABLES FROM iceberg.db")
        print("Available Tables in Iceberg Catalog:")
        print(res)
    except Exception as err:
        print(f"Trino not reachable directly ({err}). Ensure Docker Compose stack is started.")


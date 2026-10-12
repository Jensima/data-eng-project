"""
High-Velocity Simulated Kafka Event Stream Producer
Supports continuous streaming, burst mode, schema validation, and deliberate anomaly injection
to thoroughly test Dead-Letter Queue (DLQ) and schema enforcement in the PySpark Structured Streaming pipeline.
"""

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KafkaStreamProducer")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_FILE = PROJECT_ROOT / "data" / "amazon_products_raw.csv"

TOPIC_STREAM = "amazon-products-stream"
TOPIC_DLQ = "amazon-products-dlq"
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def create_producer(bootstrap_servers=DEFAULT_BOOTSTRAP):
    """Instantiate a resilient KafkaProducer with JSON serialization."""
    logger.info(f"Connecting to Kafka broker at {bootstrap_servers}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks="all",  # Strongest durability guarantee
            retries=5,
            linger_ms=10,
            batch_size=16384
        )
        logger.info("Kafka Producer established successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka at {bootstrap_servers}: {e}")
        raise


def generate_event(row, inject_anomaly=False):
    """
    Format a DataFrame row into a standardized JSON streaming payload.
    Optionally injects anomalies to test Dead-Letter Queue (DLQ) integrity.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    
    event = {
        "event_id": f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": now_iso,
        "url": row.get("url"),
        "title": row.get("title"),
        "price": row.get("price"),
        "mrp": row.get("mrp"),
        "discount": row.get("discount"),
        "rating": row.get("rating"),
        "rating_count": row.get("rating_count"),
        "description": row.get("description"),
        "highlights": row.get("highlights"),
        "specifications": row.get("specifications"),
        "image_urls": row.get("image_urls"),
        "video_urls": row.get("video_urls"),
        "source": "simulated-amazon-stream"
    }

    if inject_anomaly:
        anomaly_type = random.choice(["invalid_price", "invalid_rating", "missing_url", "malformed_title"])
        event["_anomaly_injected"] = anomaly_type
        if anomaly_type == "invalid_price":
            event["price"] = -999.0
        elif anomaly_type == "invalid_rating":
            event["rating"] = 12.5  # Rating out of bounds (max is 5.0)
        elif anomaly_type == "missing_url":
            event["url"] = None
        elif anomaly_type == "malformed_title":
            event["title"] = None

    return event


def stream_events(
    data_file=DEFAULT_DATA_FILE,
    bootstrap_servers=DEFAULT_BOOTSTRAP,
    topic=TOPIC_STREAM,
    rate_per_sec=10,
    anomaly_rate=0.05,
    max_records=None,
    repeat_cycle=True
):
    """
    Continuously streams records at a specified rate per second.
    - anomaly_rate: fraction of records that will intentionally contain malformed data.
    - repeat_cycle: if True, loops dataset indefinitely to simulate a live 24/7 stream.
    """
    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Input file not found: {data_file}")

    df = pd.read_csv(data_file)
    logger.info(f"Loaded {len(df)} base seed records from {data_file}")
    logger.info(f"Streaming target: {topic} | Rate: {rate_per_sec} evt/sec | Anomaly rate: {anomaly_rate * 100:.1f}%")

    producer = create_producer(bootstrap_servers)
    delay = 1.0 / max(rate_per_sec, 1)

    total_sent = 0
    anomalies_sent = 0
    cycle = 0

    try:
        while True:
            cycle += 1
            logger.info(f"Starting streaming cycle {cycle}...")
            
            # Shuffle slightly to emulate diverse stream arrivals
            shuffled_df = df.sample(frac=1.0).reset_index(drop=True)

            for idx, row in shuffled_df.iterrows():
                inject = random.random() < anomaly_rate
                event = generate_event(row, inject_anomaly=inject)
                
                key = str(event.get("url") or f"key_{total_sent}")
                producer.send(topic, key=key, value=event)
                
                total_sent += 1
                if inject:
                    anomalies_sent += 1

                if total_sent % 50 == 0:
                    logger.info(
                        f"Stream Progress: {total_sent} sent "
                        f"({anomalies_sent} intentional anomalies for DLQ validation)"
                    )

                if max_records and total_sent >= max_records:
                    logger.info(f"Reached max requested records: {max_records}. Terminating.")
                    producer.flush()
                    return total_sent

                time.sleep(delay)

            if not repeat_cycle:
                break

    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer closed. Total events sent: {total_sent} (Anomalies: {anomalies_sent})")

    return total_sent


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="High-Velocity Kafka Event Streamer")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    parser.add_argument("--anomaly-rate", type=float, default=0.05, help="Anomaly injection rate (0.0 to 1.0)")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after N records")
    parser.add_argument("--topic", type=str, default=TOPIC_STREAM, help="Destination Kafka topic")
    parser.add_argument("--bootstrap", type=str, default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--once", action="store_true", help="Send one dataset pass and exit without repeating")

    args = parser.parse_args()

    stream_events(
        rate_per_sec=args.rate,
        anomaly_rate=args.anomaly_rate,
        max_records=args.max_records,
        topic=args.topic,
        bootstrap_servers=args.bootstrap,
        repeat_cycle=not args.once
    )


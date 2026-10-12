import json
import os
import sys
import time

import pandas as pd
from kafka import KafkaProducer


PROJECT_ROOT = "/Users/i.seviantojensima/Desktop/Sem 9/Data Engineering/Project eda"

DEFAULT_FILE = os.path.join(
    PROJECT_ROOT,
    "data",
    "amazon_products_raw.csv",
)

TOPIC = "amazon-products"


def producer_main(input_file):

    # --------------------------------------------------
    # GET INPUT FILE FROM AIRFLOW
    # --------------------------------------------------

    input_file = DEFAULT_FILE

    try:
        from airflow.sdk import get_current_context

        context = get_current_context()
        dag_run = context["dag_run"]

        conf = dag_run.conf or {}

        input_file = conf.get(
            "input_file",
            DEFAULT_FILE,
        )

    except Exception:
        # Allows the producer to run normally outside Airflow
        input_file = DEFAULT_FILE

    print(f"Kafka Producer input file: {input_file}")

    # --------------------------------------------------
    # CHECK FILE
    # --------------------------------------------------

    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Input file not found: {input_file}"
        )

    # --------------------------------------------------
    # READ CSV
    # --------------------------------------------------

    df = pd.read_csv(input_file)

    print(f"Streaming {len(df)} records...")

    # --------------------------------------------------
    # CREATE KAFKA PRODUCER
    # --------------------------------------------------

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(
            x,
            default=str
        ).encode("utf-8")
    )

    # --------------------------------------------------
    # STREAM RECORDS
    # --------------------------------------------------

    for index, row in df.iterrows():

        record = row.to_dict()

        producer.send(
            TOPIC,
            record
        )

        print(
            f"Sent Record {index + 1}/{len(df)}"
        )

        time.sleep(0.2)

    # --------------------------------------------------
    # FINISH
    # --------------------------------------------------

    producer.flush()
    producer.close()

    print("Streaming Completed")


if __name__ == "__main__":
    producer_main()
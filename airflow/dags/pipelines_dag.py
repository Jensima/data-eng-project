import os
import sys
import importlib.util
from datetime import datetime

from airflow import DAG
from airflow.sdk import get_current_context
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_ROOT = "/Users/i.seviantojensima/Desktop/Sem 9/Data Engineering/Project eda "


def add_project_to_path():
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)


# ---------------------------------------------------------
# EDA
# ---------------------------------------------------------

EDA_FILE = os.path.join(
    PROJECT_ROOT,
    "eda",
    "exploratory_analysis.py",
)


def load_eda():
    spec = importlib.util.spec_from_file_location(
        "exploratory_analysis",
        EDA_FILE,
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module.run_eda


# ---------------------------------------------------------
# EDA TASK
# ---------------------------------------------------------

def run_eda_from_conf():

    add_project_to_path()

    run_eda = load_eda()

    context = get_current_context()
    dag_run = context["dag_run"]

    conf = dag_run.conf or {}

    input_file = conf.get(
        "input_file",
        os.path.join(
            PROJECT_ROOT,
            "data",
            "amazon_products_raw.csv",
        ),
    )

    print(f"EDA input file: {input_file}")

    run_eda(input_file)


# ---------------------------------------------------------
# KAFKA PRODUCER TASK
# ---------------------------------------------------------

def run_producer_from_conf():

    add_project_to_path()

    from producer.kafka_producer import producer_main

    context = get_current_context()
    dag_run = context["dag_run"]

    conf = dag_run.conf or {}

    input_file = conf.get(
        "input_file",
        os.path.join(
            PROJECT_ROOT,
            "data",
            "amazon_products_raw.csv",
        ),
    )

    print(f"Kafka Producer input file: {input_file}")

    producer_main(input_file)


# ---------------------------------------------------------
# KAFKA CONSUMER TASK
# ---------------------------------------------------------

def run_consumer():

    add_project_to_path()

    from consumer.kafka_consumer import consumer_main

    consumer_main()


# ---------------------------------------------------------
# PROCESS STAGE TASK
# ---------------------------------------------------------

def run_process_stage():

    add_project_to_path()

    from warehouse.process_stage import process_stage

    process_stage()


# ---------------------------------------------------------
# MERGE TASK
# ---------------------------------------------------------

def run_merge():

    add_project_to_path()

    from warehouse.merge import merge_data

    merge_data()


# ---------------------------------------------------------
# REPLAY TASK
# ---------------------------------------------------------

def run_replay():

    add_project_to_path()

    from warehouse.replay import replay_errors

    replay_errors()


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

default_args = {
    "owner": "Jensi",
    "retries": 1,
}


with DAG(
    dag_id="amazon_etl_pipeline",
    description="Production Ready Amazon ETL Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=[
        "data-engineering",
        "kafka",
        "sqlite",
        "eda",
    ],
) as dag:

    eda = PythonOperator(
        task_id="eda_analysis",
        python_callable=run_eda_from_conf,
    )

    producer = PythonOperator(
        task_id="kafka_producer",
        python_callable=run_producer_from_conf,
    )

    consumer = PythonOperator(
        task_id="kafka_consumer",
        python_callable=run_consumer,
    )

    process = PythonOperator(
        task_id="process_stage",
        python_callable=run_process_stage,
    )

    merge = PythonOperator(
        task_id="merge_data",
        python_callable=run_merge,
    )

    replay = PythonOperator(
        task_id="replay_errors",
        python_callable=run_replay,
    )


    eda >> producer >> consumer >> process >> merge >> replay
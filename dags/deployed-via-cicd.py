from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def start_task():
    print("Starting the DAG")


def process_task():
    print("Processing data...")


def end_task():
    print("DAG completed successfully")


default_args = {
    "owner": "mubashir",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="sample_python_dag",
    default_args=default_args,
    description="Simple sample DAG",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sample", "training"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    process = PythonOperator(
        task_id="process",
        python_callable=process_task,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=end_task,
    )

    start >> process >> end

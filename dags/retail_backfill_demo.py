from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "platform-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="retail_backfill_demo",
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["backfill", "retail"],
    description="Retail backfill demonstration DAG"
)
def retail_backfill_demo():

    @task
    def process_sales():
        logging.info("Processing missed retail sales data")

    process_sales()

retail_backfill_demo()

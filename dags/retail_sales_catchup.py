from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "data-platform-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id="retail_sales_catchup",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,
    default_args=default_args,
    tags=["retail", "sales", "catchup"],
    description="Historical retail sales processing DAG with catchup enabled"
)
def retail_sales_catchup():

    @task
    def extract_sales():
        logging.info("Extracting historical retail sales data")

    @task
    def transform_sales():
        logging.info("Transforming retail sales data")

    @task
    def load_sales():
        logging.info("Loading retail sales data")

    extract_sales() >> transform_sales() >> load_sales()

retail_sales_catchup()

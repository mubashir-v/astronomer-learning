from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "analytics-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="retail_reporting_no_catchup",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["retail", "reporting"],
    description="Retail reporting DAG with catchup disabled"
)
def retail_reporting_no_catchup():

    @task
    def generate_report():
        logging.info("Generating retail report")

    @task
    def publish_report():
        logging.info("Publishing retail report")

    generate_report() >> publish_report()

retail_reporting_no_catchup()

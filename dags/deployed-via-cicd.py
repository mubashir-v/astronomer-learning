"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

A final verification task confirms the DAG ran successfully on the
new deployment by logging run metadata and environment info.
"""

from airflow.sdk import Asset, dag, task
from pendulum import datetime
import requests
import os


@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():

    @task(outlets=[Asset("current_astronauts")])
    def get_astronauts(**context) -> list[dict]:
        """
        Retrieves list of astronauts currently in space from Open Notify API.
        Falls back to hardcoded data if API is unavailable.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception:
            print("API currently not available, using hardcoded data instead.")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Oleg Kononenko"},
                {"craft": "ISS", "name": "Nikolai Chub"},
                {"craft": "ISS", "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS", "name": "Matthew Dominick"},
                {"craft": "ISS", "name": "Michael Barratt"},
                {"craft": "ISS", "name": "Jeanette Epps"},
                {"craft": "ISS", "name": "Alexander Grebenkin"},
                {"craft": "ISS", "name": "Butch Wilmore"},
                {"craft": "ISS", "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        Prints each astronaut's name and craft.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    @task
    def verify_deployment(**context) -> None:
        """
        Verification task — confirms the DAG ran successfully on the new deployment.
        Logs run metadata and environment info so you can confirm in the Airflow UI
        that the new deployment is live and working.
        """
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]
        logical_date = context["logical_date"]
        airflow_home = os.getenv("AIRFLOW_HOME", "not set")
        airflow_env = os.getenv("AIRFLOW_ENV", "not set")
        deployment_id = os.getenv("ASTRONOMER_DEPLOYMENT_ID", "not set")
        runtime_version = os.getenv("ASTRONOMER_RUNTIME_VERSION", "not set")

        print("=" * 50)
        print("✅ DEPLOYMENT VERIFICATION")
        print("=" * 50)
        print(f"DAG ID:             {dag_id}")
        print(f"Run ID:             {run_id}")
        print(f"Logical Date:       {logical_date}")
        print(f"Airflow Home:       {airflow_home}")
        print(f"Airflow Env:        {airflow_env}")
        print(f"Deployment ID:      {deployment_id}")
        print(f"Runtime Version:    {runtime_version}")
        print("=" * 50)
        print("✅ New deployment is live and DAG executed successfully!")

    # Dynamic task mapping
    astronauts = get_astronauts()
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronauts
    )

    # Verification runs after get_astronauts confirms data was fetched
    astronauts >> verify_deployment()


example_astronauts()

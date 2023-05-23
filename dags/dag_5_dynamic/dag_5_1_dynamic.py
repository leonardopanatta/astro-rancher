from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
from typing import Dict

from dag_5_dynamic.group.process_tasks import process_tasks

partners = {
    "partner_snowflake": {
        "name": "snowflake",
        "path": "/partners/snowflake",
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix",
    },
    "partner_astronomer": {
        "name": "astronomer",
        "path": "/partners/astronomer",
    }
}

default_args = {
    "start_date": datetime(2023,5,20)
}
@dag(
    description="DAG in charge of processing customer data",
    default_args=default_args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1
)

def dag_5_1_dynamic():

    start = DummyOperator(task_id="start")

    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values)

dag = dag_5_1_dynamic()
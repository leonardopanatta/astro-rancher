from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
from typing import Dict

from dag_6_branching.group.process_tasks import process_tasks

import time

partners = {
    "partner_snowflake": {
        "name": "snowflake",
        "path": "/partners/snowflake",
        "priority": 2,
        "pool": "snowflake"
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix",
        "priority": 3,
        "pool": "netflix"
    },
    "partner_astronomer": {
        "name": "astronomer",
        "path": "/partners/astronomer",
        "priority": 1,
        "pool": "astronomer"
    }
}

def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    if (day == 1):
        return 'extract_partner_snowflake'
    if (day == 3):
        return 'extract_partner_netflix'
    if (day == 5):
        return 'extract_partner_astronomer'
    return 'stop'

default_args = {
    "start_date": datetime(2023,5,20),
    "retries":0
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

def dag_91_1_depends_on_past():

    start = DummyOperator(task_id="start", trigger_rule='all_success')

    #choosing_partner_based_on_day = BranchPythonOperator(
    #    task_id="choosing_partner_based_on_day",
    #    python_callable=_choosing_partner_based_on_day
    #)

    #stop = DummyOperator(task_id="stop")

    storing = DummyOperator(task_id="storing", trigger_rule='none_failed_or_skipped')

    #choosing_partner_based_on_day >> stop
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", depends_on_past=True, priority_weight=details['priority'], do_xcom_push=False, pool=details['pool'], multiple_outputs=True)
        def extract(partner_name, partner_path):
            time.sleep(3)
            raise ValueError("failed")
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values) >> storing

dag = dag_91_1_depends_on_past()
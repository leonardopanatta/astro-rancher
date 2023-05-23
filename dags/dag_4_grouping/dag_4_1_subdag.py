from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator

from datetime import datetime, timedelta
from typing import Dict

from dag_4_grouping.subdag.subdag_factory import subdag_factory

@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

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

def dag_4_1_subdag():

    process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory("dag_4_1_subdag", "process_tasks", default_args),
        poke_interval=15
        #mode="reschedule"
    )

    extract() >> process_tasks

dag = dag_4_1_subdag()
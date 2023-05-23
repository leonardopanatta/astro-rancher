from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

from datetime import datetime, timedelta
from typing import Dict

#@task.python(task_id="extract_partners", multiple_outputs=True)
#@task.python(task_id="extract_partners")
@task.python(task_id="extract_partners", do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@dag(
    description="DAG in charge of processing customer data",
    start_date=datetime(2023,5,20),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1
)

def dag_3_4_taskflow_xcom():

    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])

dag_3_4_taskflow_xcom()
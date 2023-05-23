from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from typing import Dict

from dag_4_grouping.group.process_tasks import process_tasks

@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

#@task.python
#def process_a(partner_name, partner_path):
#    print(partner_name)
#    print(partner_path)

#@task.python
#def process_b(partner_name, partner_path):
#    print(partner_name)
#    print(partner_path)

#@task.python
#def process_c(partner_name, partner_path):
#    print(partner_name)
#    print(partner_path)

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

def dag_4_2_grouping():

    partner_settings = extract()

    process_tasks(partner_settings)

    #with TaskGroup(group_id='process_tasks') as process_tasks:
    #    process_a(partner_settings['partner_name'], partner_settings['partner_path'])
    #    process_b(partner_settings['partner_name'], partner_settings['partner_path'])
    #    process_c(partner_settings['partner_name'], partner_settings['partner_path'])

dag = dag_4_2_grouping()
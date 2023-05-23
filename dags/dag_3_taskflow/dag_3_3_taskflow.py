from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

from datetime import datetime, timedelta

@task.python
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    #return {"partner_name": partner_name, "partner_path": partner_path}
    return partner_name

@task.python
def process(partner_name):
    print(partner_name)

@dag(
    description="DAG in charge of processing customer data",
    start_date=datetime(2023,5,20),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1
)

def dag_3_3_taskflow():

    #extract() >> process()
    process(extract())

dag_3_3_taskflow()
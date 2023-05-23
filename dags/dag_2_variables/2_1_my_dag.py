from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def _extract():
    partner = Variable.get("2_1_my_dag_partner")
    partner_secret = Variable.get("2_1_my_dag_secret")
    print(partner)
    print(partner_secret)

with DAG(
    "2_1_my_dag", 
    description="DAG in charge of processing customer data",
    start_date=datetime(2023,5,20),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1
) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract
    )
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def _extract():
    #print("extract")
    #partner_name = "netflix"
    #ti.xcom_push(key="partner_name", value=partner_name)
    #return partner_name
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

def _process(ti):
    #print("process")
    #partner_name = ti.xcom_pull(key="partner_name", task_ids="extract")
    #partner_name = ti.xcom_pull(key="return_value", task_ids="extract")
    #partner_name = ti.xcom_pull(task_ids="extract")
    #print(partner_name)
    partner_settings = ti.xcom_pull(task_ids="extract")
    print(partner_settings['partner_name'])

with DAG(
    "3_2_xcom", 
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

    process = PythonOperator(
        task_id="process",
        python_callable=_process
    )

    extract >> process
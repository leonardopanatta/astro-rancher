from airflow import DAG

from datetime import datetime, timedelta

with DAG(
    "1_my_dag", 
    description="DAG in charge of processing customer data",
    start_date=datetime(2023,5,20),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1
) as dag:
    None
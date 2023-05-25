from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import airflow.utils.dates

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}

with DAG(
    dag_id="cleaning_dag2",
    default_args=default_args,
    schedule_interval="@daily", catchup=False) as dag:

    cleaning_xcoms = PostgresOperator(
        task_id='cleaning_xcoms',
        sql='sql/CLEANING/XCOMS.sql',
        postgres_conn_id='postgres'
    )

    waiting_for_task >> cleaning_xcoms
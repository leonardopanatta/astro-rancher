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
    dag_id="cleaning_dag",
    default_args=default_args,
    schedule_interval="@daily", catchup=False) as dag:

    waiting_for_task = ExternalTaskSensor(
        task_id='waiting_for_task',
        external_dag_id='dag_94_1_external_sensor',
        external_task_id='storing'
        #execution_date_fn=
        #failed_states=['failed', 'skipped'],
        #failed_states=['success']
    )

    cleaning_xcoms = PostgresOperator(
        task_id='cleaning_xcoms',
        sql='sql/CLEANING/XCOMS.sql',
        postgres_conn_id='postgres'
    )

    waiting_for_task >> cleaning_xcoms
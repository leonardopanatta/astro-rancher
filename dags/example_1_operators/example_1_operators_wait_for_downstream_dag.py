from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def _sleep():
    time.sleep(120)

def _sleep_and_raise_error():
    time.sleep(120)
    raise ValueError("failed")

default_args = {
    'start_date': datetime(2023, 5, 26),
}

dag = DAG(
    'example_1_operators_wait_for_downstream_dag',
    schedule_interval=timedelta(minutes=1),
    default_args=default_args,
    catchup=False
)

extract = PythonOperator(
    task_id='extract',
    python_callable=_sleep,
    depends_on_past=True,
    wait_for_downstream=True,
    dag=dag
)

transform = PythonOperator(
    task_id='transform',
    python_callable=_sleep,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=_sleep,
    dag=dag
)

extract >> transform >> load

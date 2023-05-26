from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 5, 25)
}

dag = DAG(
    'example_1_operators_triggerdagrunoperator_dag_b',
    schedule_interval=None,
    default_args=default_args
)

dummy = DummyOperator(task_id="start" , dag=dag)
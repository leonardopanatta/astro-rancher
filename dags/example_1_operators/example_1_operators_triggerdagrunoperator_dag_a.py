from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 5, 25)
}

dag = DAG(
    'example_1_operators_triggerdagrunoperator_dag_a',
    schedule_interval=None,
    default_args=default_args
)

trigger_dag_b = TriggerDagRunOperator(
    task_id='trigger_dag_b',
    trigger_dag_id='example_1_operators_triggerdagrunoperator_dag_b',
    dag=dag
)
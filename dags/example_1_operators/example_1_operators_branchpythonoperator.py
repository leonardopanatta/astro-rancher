from airflow.decorators import task, dag
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import time

def _sleep():
    time.sleep(5)

def _choose_best_model():
    accuracy = 9
    if accuracy > 7:
        return ['super_accurate', 'accurate']
    elif accuracy > 5:
        return 'accurate'
    return 'inaccurate'

default_args = {
    'start_date': datetime(2023, 5, 26),
}

@dag(
    default_args=default_args,
    schedule_interval="@daily",
    tags=["example"],
    catchup=False
)

def example_1_operators_branchpythonoperator():
    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )
    
    super_accurate = DummyOperator(
        task_id='super_accurate'
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )
    
    storing = DummyOperator(
        task_id='storing',
        trigger_rule='none_failed_or_skipped'
    )

    choose_best_model >> [super_accurate, accurate, inaccurate] >> storing

dag = example_1_operators_branchpythonoperator()

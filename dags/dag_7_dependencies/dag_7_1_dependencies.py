from airflow.models import DAG
from airflow.models.baseoperator import cross_downstream, chain
from airflow.operators.dummy import DummyOperator

from datetime import datetime

default_args = {
    "start_date": datetime(2020,1,1)
}

with DAG(
    'dag_7_1_dependencies', 
    schedule_interval='@daily', 
    default_args=default_args,
    catchup=False
) as dag:

    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")
    t7 = DummyOperator(task_id="t7")

    #t2.set_upstream(t1)                                    # same as t2 << t1
    #t1.set_downstream(t2)                                  # same as t1 >> t2

    #[t1, t2, t3] >> t5                                     # ok

    #[t1, t2, t3] >> [t4, t5, t6]                           # error

    #[t1, t2, t3] >> t4                                     #ok
    #[t1, t2, t3] >> t5                                     #ok
    #[t1, t2, t3] >> t6                                     #ok

    #cross_downstream
    #cross_downstream([t1, t2, t3], [t4, t5, t6])            #ok
    #cross_downstream([t1, t2, t3], [t4, t5, t6]) >> t7     #error
    #[t4, t5, t6] >> t7                                      #ok

    #chain
    #chain(t1, [t2, t3], [t4, t5], t6, t7)

    # cross_downstream + chain
    cross_downstream([t2, t3], [t4, t5])
    chain(t1, t2, t5, t6, t7)

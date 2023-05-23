from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')

def _extract(partner_name):
    print(partner_name)

with DAG(
    "3_1_my_dag", 
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
        python_callable=_extract,
        op_args=["{{ var.json.my_dag_partner.name }}"]
    )

    fetching_data = PostgresOperator(
        task_id = "fetching_data",
        #sql="SELECT partner_name FROM partners WHERE date={{ ds }}"
        sql="sql/MY_REQUEST.sql"
    )

    fetching_data_custom = CustomPostgresOperator(
        task_id = "fetching_data_custom",
        sql="sql/MY_REQUEST.sql",
        parameters={
            'next_ds': '{{ next_ds }}',
            'prev_ds': '{{ prev_ds }}',
            'partner_name': '{{ var.json.my_dag_partner.name }}'
        }
    )
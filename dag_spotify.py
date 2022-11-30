from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from etl_spotify import run_etl_spotify

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 8),
    'email': ['example@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'daag_spotify',
    default_args=default_args,
    description='DAG for ETL',
    schedule_interval=timedelta(days=1),
)

def example_function():
    print("example")

etl_run = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=run_etl_spotify,
    dag=dag,
)

etl_run

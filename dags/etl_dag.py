from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_etl():
    print('Running ETL pipeline...')


default_args = {
    'start_date': datetime(2025, 5, 18),
    'retries': 1
}

dag = DAG(
    'etl_pupeline',
    default_args=default_args,
    schedule_interval='@daily'
)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag
)

etl_task

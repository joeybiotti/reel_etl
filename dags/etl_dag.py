from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.etl_pipeline import extract, transform, load
import tomli
from airflow.models import Variable
import pandas as pd

config_path = Variable.get('config_path')


def load_config():
    with open(config_path, 'rb') as f:
        return tomli.load(f)


config = load_config()


def preprocess_extracted_data(**kwargs):
    file_path = kwargs['file_path']
    df = pd.read_csv(file_path)
    return (df)


def run_etl():
    print('Running ETL pipeline...')


default_args = {
    'start_date': datetime(2025, 5, 18),
    'retries': 1
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule='@daily'
)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    op_kwargs={'file_path': config['paths']['raw_data_file']},
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
    op_kwargs={"df": preprocess_extracted_data(file_path=config["paths"]["raw_data_file"])}, 
    dag=dag
)


load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    op_kwargs={
        "df": transform(extract(file_path=config["paths"]["raw_data_file"])), 
        "db_path": config["paths"]["db_path"],
        "table_name": config["paths"]["table_name"]
    },
    dag=dag
)


etl_task >> extract_task >> transform_task >> load_task

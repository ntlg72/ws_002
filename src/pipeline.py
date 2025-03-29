from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.nodes.data_gathering import read_db, read_csv
from src.nodes.data_transform import transform_db, transform_csv, merge_data
from src.nodes.data_storage import load_data, store_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    'spotify_grammys_etl',
    default_args=default_args,
    description='ETL pipeline for Spotify and Grammys data',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=read_db,
    )

    transform_db_task = PythonOperator(
        task_id='transform_db',
        python_callable=transform_db,
    )

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
    )

    transform_csv_task = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv,
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=lambda: print("Extracting common features"),
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=lambda: print("Transforming combined data"),
    )

    merge_task = PythonOperator(
        task_id='merge',
        python_callable=merge_data,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    store_task = PythonOperator(
        task_id='store',
        python_callable=store_data,
    )

    # Define task dependencies
    read_db_task >> transform_db_task >> extract_task
    read_csv_task >> transform_csv_task >> extract_task
    extract_task >> transform_task >> merge_task >> load_task >> store_task
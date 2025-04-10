from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, os

# Agregar src al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar funciones de extracción
from src.etl.extraction import load_local_csv, read_table_from_db, process_audio_dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='etl_full_extraction_dag',
    default_args=default_args,
    description='Full ETL extraction DAG: CSV, DB, and Audio API',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    read_csv = PythonOperator(
        task_id='read_csv',
        python_callable=load_local_csv,
        op_kwargs={'csv_path': '../data/external/spotify_dataset.csv'},
    )

    read_db = PythonOperator(
        task_id='read_db',
        python_callable=read_table_from_db,
        op_kwargs={'table_name': 'grammys_raw'},
    )

    read_api = PythonOperator(
        task_id='read_api',
        python_callable=process_audio_dataset,
    )

    # Secuencia: puedes correrlas en paralelo o en serie
    [read_csv, read_db, read_api]

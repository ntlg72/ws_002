from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Agregar src al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar funciones y parámetros
from src.etl.extraction import load_local_csv, read_table_from_db, process_audio_dataset
from src.params import Params

params = Params()  # Instancia para acceder a rutas

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
        op_kwargs={'csv_path': str(params.SPOTIFY_DATASET_PATH)},  # ← ahora usando params
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

    [read_csv, read_db, read_api]

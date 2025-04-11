from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator
import sys, os

# Add src al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# import modules and params
from src.etl.extraction import load_local_csv, read_table_from_db, process_audio_dataset
from src.etl.transform_spotify import transform_spotify_dataset
from src.etl.transform_grammys import transform_grammy_dataset
from src.etl.merge import merge_and_enrich_datasets
from src.etl.load import *
from src.params import Params

params = Params()

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
    dag_id='etl_full_pipeline_dag',
    default_args=default_args,
    description='ETL pipeline: extract CSV/DB/API, transform, merge',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # --- Extraction tasks ---
    extract_spotify = PythonOperator(
        task_id='extract_spotify_csv',
        python_callable=load_local_csv,
        op_kwargs={'csv_path': str(params.SPOTIFY_DATASET_PATH)},
    )

    extract_grammy = PythonOperator(
        task_id='extract_grammy_db',
        python_callable=read_table_from_db,
        op_kwargs={'table_name': 'grammys_raw'},
    )

    extract_api = PythonOperator(
        task_id='extract_audio_features',
        python_callable=process_audio_dataset,
    )

    # --- Transformation tasks ---
    transform_spotify = PythonOperator(
        task_id='transform_spotify',
        python_callable=transform_spotify_dataset,
    )

    transform_grammy = PythonOperator(
        task_id='transform_grammy',
        python_callable=transform_grammy_dataset,
    )

    transform_api = EmptyOperator(task_id='transform_api')  # ← placeholder

    # --- Merge task ---
    merge_data = PythonOperator(
        task_id='merge_final_datasets',
        python_callable=merge_and_enrich_datasets,
    )

    load_db = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_database,
    )

    upload_to_drive = LocalFilesystemToGoogleDriveOperator(
    task_id='upload_to_drive',
    local_paths=[str(Path(params.processed_data) / 'final_data.csv')],  # Debe ser lista
    drive_folder='root',  # Usa 'root' si quieres subirlo a "Mi unidad", o especifica folder_id
    gcp_conn_id='google_cloud_default'
    )



    # --- Pipeline structure ---
    extract_spotify >> transform_spotify
    extract_grammy >> transform_grammy
    extract_api >> transform_api

    [transform_spotify, transform_grammy, transform_api] >> merge_data >> [load_db, upload_to_drive]

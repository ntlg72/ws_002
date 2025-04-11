import pandas as pd
import logging
from pathlib import Path
from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import sys


sys.path.append(str(Path(__file__).resolve().parents[2])) 

# Setup
setup_logging()
params = Params()
db_client = DatabaseClient(params)

def load_to_database():
    """
    Loads the final transformed dataset into a PostgreSQL database.
    """
    try:
        final_path = Path(params.processed_data) / "final_data.csv"
        df = pd.read_csv(final_path)
        
        df.to_sql('final_grammy_data', con=db_client.engine, if_exists='replace', index=False)
        logging.info("Final dataset successfully loaded into the database.")

    except Exception as e:
        logging.error(f"Failed to load data into the database: {e}")
    finally:
        try:
            db_client.close()
        except Exception as e:
            logging.warning(f"Failed to close database connection: {e}")


# Configura las credenciales
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/ruta/a/tu/archivo.json'

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('drive', 'v3', credentials=credentials)

# Listar archivos
results = service.files().list(
    pageSize=10, fields="files(id, name)").execute()
files = results.get('files', [])

if not files:
    print("No se encontraron archivos.")
else:
    print("Archivos encontrados:")
    for file in files:
        print(f"Nombre: {file['name']}, ID: {file['id']}")
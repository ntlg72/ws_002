import pandas as pd
import logging
from pathlib import Path
from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

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

def upload_to_drive():
    """
    Uploads the final transformed dataset to Google Drive using PyDrive.
    """
    try:
        final_path = Path(params.processed_data) / "final_data.csv"

        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()  # Authentication through browser
        drive = GoogleDrive(gauth)

        file_drive = drive.CreateFile({'title': 'final_data.csv'})
        file_drive.SetContentFile(str(final_path))
        file_drive.Upload()

        logging.info("Final dataset successfully uploaded to Google Drive.")

    except Exception as e:
        logging.error(f"Failed to upload file to Google Drive: {e}")

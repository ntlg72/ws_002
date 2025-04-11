import os
import re
import json
import logging
import subprocess
import pandas as pd
import requests
import shutil
from tqdm import tqdm
from pathlib import Path
import time

from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging

# Initial configuration
setup_logging()
params = Params()
db_client = DatabaseClient(params)

# Define a temporary directory under data/intermediate
TEMP_DIR = Path(params.external_data)
TEMP_DIR.mkdir(parents=True, exist_ok=True)



def load_local_csv(csv_path: str) -> str:
    """
    Loads a CSV file from a local path into a DataFrame and saves it to a temporary path.

    Args:
        csv_path (str): Absolute path to the CSV file.

    Returns:
        str: Path to the saved transformed CSV file.
    """
    df = pd.read_csv(csv_path)
    logging.info(f"CSV loaded from {csv_path} with shape {df.shape}")
    
    output_path = TEMP_DIR / "spotify_loaded.csv"
    df.to_csv(output_path, index=False)
    logging.info(f"CSV saved to temporary path: {output_path}")
    return str(output_path)

def load_local_api(csv_path: str) -> str:
    """
    Loads a CSV file from a local path into a DataFrame and saves it to a temporary path.

    Args:
        csv_path (str): Absolute path to the CSV file.

    Returns:
        str: Path to the saved transformed CSV file.
    """
    df = pd.read_csv(csv_path)
    logging.info(f"CSV loaded from {csv_path} with shape {df.shape}")
    
    output_path = TEMP_DIR / "api_loaded.csv"
    df.to_csv(output_path, index=False)
    logging.info(f"CSV saved to temporary path: {output_path}")
    return str(output_path)

def read_table_from_db(table_name: str) -> str:
    """
    Reads a table from the configured database into a DataFrame and saves it to a temporary path.

    Args:
        table_name (str): Name of the table to read.

    Returns:
        str: Path to the saved CSV file.
    """
    try:
        df = pd.read_sql_table(table_name, con=db_client.engine)
        logging.info(f"Table '{table_name}' read with shape {df.shape}")
        
        output_path = TEMP_DIR / "grammys_loaded.csv"
        df.to_csv(output_path, index=False)
        logging.info(f"Database table saved to: {output_path}")
        return str(output_path)

    except Exception as e:
        logging.warning(f"Failed to read from DB: {e}")
        return ""
    finally:
        try:
            db_client.close()
        except Exception as e:
            logging.error("Failed to close DB connection")
            logging.error(f"Error details: {e}")
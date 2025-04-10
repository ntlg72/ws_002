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

from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging

# Initial configuration
setup_logging()
params = Params()
db_client = DatabaseClient(params)

# Define a temporary directory under data/intermediate
TEMP_DIR = Path(params.intermediate_data) / "temp"
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Fixed directory where audio files already exist
AUDIO_DIR = params.DATA_DIR / "audio_files"

# Temporary subdirectories and file paths
TRIMMED_DIR = TEMP_DIR / "trimmed"
RECCOBEATS_JSON_PATH = TEMP_DIR / "reccobeats_features.json"
RECCOBEATS_CSV_PATH = TEMP_DIR / "reccobeats_features.csv"

def load_local_csv(csv_path: str) -> pd.DataFrame:
    """
    Loads a CSV file from a local path into a DataFrame.

    Args:
        csv_path (str): Absolute path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the loaded data.
    """
    df = pd.read_csv(csv_path)
    logging.info(f"CSV loaded from {csv_path} with shape {df.shape}")
    return df

def read_table_from_db(table_name: str) -> pd.DataFrame:
    """
    Reads a table from the configured database into a DataFrame.

    Args:
        table_name (str): Name of the table to read.

    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    try:
        df = pd.read_sql_table(table_name, con=db_client.engine)
        logging.info(f"Table '{table_name}' read with shape {df.shape}")
        return df
    except Exception as e:
        logging.warning(f"Failed to read from DB: {e}")
        return pd.DataFrame()
    finally:
        try:
            db_client.close()
        except Exception as e:
            logging.error("Failed to close DB connection")
            logging.error(f"Error details: {e}")

def safe_filename(title: str) -> str:
    """
    Generates a filesystem-safe filename from a given title.

    Args:
        title (str): Original title string.

    Returns:
        str: Safe filename.
    """
    return re.sub(r'[^\w\-_\(\)\s]', '', title).replace(" ", "_")

def trim_audio(audio_path: str, output_dir: Path = TRIMMED_DIR) -> str | None:
    """
    Trims the first 30 seconds of a given audio file using ffmpeg.

    Args:
        audio_path (str): Path to the original audio file.
        output_dir (Path): Directory where the trimmed audio will be saved.

    Returns:
        str | None: Path to the trimmed audio file, or None if trimming failed.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    base_name = os.path.basename(audio_path)
    trimmed_path = output_dir / f"{Path(base_name).stem}_trimmed.mp3"

    try:
        subprocess.run([
            "ffmpeg", "-y", "-i", str(audio_path),
            "-t", "30", "-acodec", "copy", str(trimmed_path)
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if trimmed_path.exists() and trimmed_path.stat().st_size > 0:
            return str(trimmed_path)
    except Exception as e:
        logging.error(f"Error trimming {audio_path}: {e}")

    return None

def analyze_with_reccobeats(trimmed_path: str) -> tuple[dict | None, str | None]:
    """
    Sends a trimmed audio file to the ReccoBeats API for analysis.

    Args:
        trimmed_path (str): Path to the trimmed audio file.

    Returns:
        tuple[dict | None, str | None]: Features from API or error message.
    """
    try:
        with open(trimmed_path, 'rb') as file:
            files = {'audioFile': (os.path.basename(trimmed_path), file, 'audio/mpeg')}
            headers = {'Accept': 'application/json'}

            response = requests.post(
                "https://api.reccobeats.com/v1/analysis/audio-features",
                files=files, headers=headers
            )

            if response.status_code == 200:
                return response.json(), None
            else:
                return None, f"{response.status_code} {response.reason}"
    except Exception as e:
        return None, str(e)

def process_audio_dataset() -> pd.DataFrame:
    """
    Processes a dataset of Grammy nominees, trims audio files, sends them to the ReccoBeats API,
    and returns a DataFrame with nominees and extracted audio features.

    Returns:
        pd.DataFrame: DataFrame containing nominees and their extracted audio features.
    """
    input_path = params.intermediate_data / "grammys.csv"
    df = pd.read_csv(input_path)
    filtered_df = df[df['normalized_category'].isin(['Song Of The Year', 'Record Of The Year'])]
    results = []

    for _, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Analyzing with ReccoBeats"):
        nominee = row["nominee"]
        filename = safe_filename(nominee) + ".mp3"
        audio_path = AUDIO_DIR / filename

        if not audio_path.exists():
            logging.warning(f"Audio file not found for {nominee}: {audio_path}")
            continue

        trimmed = trim_audio(str(audio_path))
        if trimmed:
            features, error = analyze_with_reccobeats(trimmed)
            Path(trimmed).unlink(missing_ok=True)
        else:
            features, error = None, "Trimmed audio not found"

        results.append({"nominee": nominee, "features": features, "error": error})

    with open(RECCOBEATS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved JSON results: {RECCOBEATS_JSON_PATH}")

    df_results = pd.DataFrame(results)
    features_df = df_results.dropna(subset=["features"]).copy()
    features_expanded = features_df["features"].apply(pd.Series)
    features_combined = pd.concat([features_df[["nominee"]], features_expanded], axis=1)

    features_combined.to_csv(RECCOBEATS_CSV_PATH, index=False)
    logging.info(f"Saved CSV features: {RECCOBEATS_CSV_PATH}")

    return features_combined

def clean_temp_dir():
    """
    Deletes the temporary directory used for trimmed audio and ReccoBeats outputs.
    """
    try:
        shutil.rmtree(TEMP_DIR)
        logging.info(f"Temporary directory deleted: {TEMP_DIR}")
    except Exception as e:
        logging.error(f"Could not delete temporary directory: {e}")

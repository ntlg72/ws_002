import os
import re
import json
import logging
import subprocess
import pandas as pd
import requests
import tempfile
import shutil
from tqdm import tqdm

from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging

# Configuración inicial
setup_logging()
params = Params()
db_client = DatabaseClient(params)

# Carpeta temporal
TEMP_DIR = os.path.join(tempfile.gettempdir(), "reccobeats_tmp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Rutas dentro de la carpeta temporal
AUDIO_DIR = "/home/bb-8/ws_002/data/audio_files"  # Este sigue fijo porque ya existen los archivos
TRIMMED_DIR = os.path.join(TEMP_DIR, "trimmed")
RECCOBEATS_JSON_PATH = os.path.join(TEMP_DIR, "reccobeats_features.json")
RECCOBEATS_CSV_PATH = os.path.join(TEMP_DIR, "reccobeats_features.csv")

def load_local_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    logging.info(f"CSV loaded from {csv_path} with shape {df.shape}")
    return df

def read_table_from_db(table_name: str) -> pd.DataFrame:
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
    return re.sub(r'[^\w\-_\(\)\s]', '', title).replace(" ", "_")

def trim_audio(audio_path: str, output_dir: str = TRIMMED_DIR) -> str | None:
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.basename(audio_path)
    trimmed_path = os.path.join(output_dir, f"{os.path.splitext(base_name)[0]}_trimmed.mp3")

    try:
        subprocess.run([
            "ffmpeg", "-y", "-i", audio_path,
            "-t", "30", "-acodec", "copy", trimmed_path
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if os.path.exists(trimmed_path) and os.path.getsize(trimmed_path) > 0:
            return trimmed_path
    except Exception as e:
        logging.error(f"Error trimming {audio_path}: {e}")

    return None

def analyze_with_reccobeats(trimmed_path: str) -> tuple[dict | None, str | None]:
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
    df = pd.read_csv("/home/bb-8/ws_002/data/intermediate/grammys.csv")
    filtered_df = df[df['normalized_category'].isin(['Song Of The Year', 'Record Of The Year'])]
    results = []

    for _, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Analyzing with ReccoBeats"):
        nominee = row["nominee"]
        filename = safe_filename(nominee) + ".mp3"
        audio_path = os.path.join(AUDIO_DIR, filename)

        if not os.path.exists(audio_path):
            logging.warning(f"Audio file not found for {nominee}: {audio_path}")
            continue

        trimmed = trim_audio(audio_path)
        if trimmed:
            features, error = analyze_with_reccobeats(trimmed)
            os.remove(trimmed)
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
    try:
        shutil.rmtree(TEMP_DIR)
        logging.info(f"Directorio temporal eliminado: {TEMP_DIR}")
    except Exception as e:
        logging.error(f"No se pudo eliminar el directorio temporal: {e}")
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
from src.logging_config import setup_logging

setup_logging()

def get_temp_paths(params: Params):
    """Returns important intermediate paths based on Params."""
    temp_dir = Path(params.intermediate_data) / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return {
        "TEMP_DIR": temp_dir,
        "TRIMMED_DIR": temp_dir / "trimmed",
        "RECCOBEATS_JSON": temp_dir / "reccobeats_features.json",
        "RECCOBEATS_CSV": temp_dir / "reccobeats_features.csv",
    }

def safe_filename(title: str) -> str:
    return re.sub(r'[^\w\-_\(\)\s]', '', title).replace(" ", "_")

def trim_audio(audio_path: str, output_dir: Path) -> str | None:
    output_dir.mkdir(parents=True, exist_ok=True)
    trimmed_path = output_dir / f"{Path(audio_path).stem}_trimmed.mp3"
    try:
        subprocess.run([
            "ffmpeg", "-y", "-i", str(audio_path),
            "-t", "30", "-acodec", "copy", str(trimmed_path)
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        return str(trimmed_path) if trimmed_path.exists() else None
    except Exception as e:
        logging.error(f"Error trimming {audio_path}: {e}")
        return None

def analyze_with_reccobeats(trimmed_path: str) -> tuple[dict | None, str | None]:
    try:
        with open(trimmed_path, 'rb') as f:
            response = requests.post(
                "https://api.reccobeats.com/v1/analysis/audio-features",
                files={'audioFile': (os.path.basename(trimmed_path), f, 'audio/mpeg')},
                headers={'Accept': 'application/json'}
            )
            if response.status_code == 200:
                return response.json(), None
            return None, f"{response.status_code} {response.reason}"
    except Exception as e:
        return None, str(e)

def process_audio_dataset():
    """
    Processes Grammy audio files with ReccoBeats and returns extracted features as DataFrame.
    """
    params = Params()
    paths = get_temp_paths(params)
    input_path = Path(params.intermediate_data) / "grammys.csv"
    audio_dir = Path(params.DATA_DIR) / "audio_files"

    if not input_path.exists():
        logging.error(f"Input CSV not found: {input_path}")
        return pd.DataFrame()

    df = pd.read_csv(input_path)
    filtered = df[df['normalized_category'].isin(['Song Of The Year', 'Record Of The Year'])]\
                .drop_duplicates(subset="nominee", keep="first")

    results = []

    for _, row in tqdm(filtered.iterrows(), total=len(filtered), desc="Analyzing with ReccoBeats"):
        nominee = row["nominee"]
        file_path = audio_dir / (safe_filename(nominee) + ".mp3")

        if not file_path.exists():
            logging.warning(f"Audio not found: {file_path}")
            continue

        trimmed = trim_audio(str(file_path), paths["TRIMMED_DIR"])
        if trimmed:
            features, error = analyze_with_reccobeats(trimmed)
            Path(trimmed).unlink(missing_ok=True)
        else:
            features, error = None, "Trimming failed"

        results.append({"nominee": nominee, "features": features, "error": error})

    # Save JSON output
    with open(paths["RECCOBEATS_JSON"], "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    df_results = pd.DataFrame(results)
    features_df = df_results.dropna(subset=["features"]).copy()
    features_expanded = features_df["features"].apply(pd.Series)
    features_combined = pd.concat([features_df[["nominee"]], features_expanded], axis=1)
    features_combined.to_csv(paths["RECCOBEATS_CSV"], index=False)

def clean_temp_dir():
    """Deletes the temporary directory."""
    params = Params()
    paths = get_temp_paths(params)
    try:
        shutil.rmtree(paths["TEMP_DIR"])
        logging.info("Temporary directory cleaned.")
    except Exception as e:
        logging.error(f"Failed to delete temp dir: {e}")

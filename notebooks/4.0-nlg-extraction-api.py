#!/usr/bin/env python
# coding: utf-8

# # Workshop #2 :
# ## Reccobeats API - Extraction
# 
# ------------------------------------------------------------
# 
# https://reccobeats.com/docs/apis/extract-audio-features

# In[4]:


import os
import sys
import re
import subprocess
import requests
import pandas as pd
import logging
import json
from tqdm import tqdm
import yt_dlp

sys.path.append(os.path.abspath('../'))
from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging


# In[5]:


setup_logging()


# # 🎯 Cargar canciones relevantes del CSV
# 

# In[6]:


df = pd.read_csv("../data/intermediate/grammys.csv")

# Filter categories
filtered_df = df[df['normalized_category'].isin(['Song Of The Year', 'Record Of The Year'])]

# Count how many rows there are
total_songs = len(filtered_df)
logging.info(f"Total relevant songs: {total_songs}")


# In[7]:


AUDIO_DIR = "../data/audio_files"


# In[8]:


def safe_filename(title):
    return re.sub(r'[^\w\-_\(\)\s]', '', title).replace(" ", "_")


#  Función para descargar audio desde YouTube como MP3

# In[9]:


def download_audio(query, output_dir=AUDIO_DIR):
    os.makedirs(output_dir, exist_ok=True)
    safe_name = safe_filename(query)
    output_path = os.path.join(output_dir, f"{safe_name}.%(ext)s")

    ydl_opts = {
        'format': 'bestaudio/best',
        'noplaylist': True,
        'quiet': True,
        'outtmpl': output_path,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"ytsearch1:{query}"])
        final_path = os.path.join(output_dir, f"{safe_name}.mp3")
        return final_path if os.path.exists(final_path) else None
    except Exception as e:
        print(f"Error wwhile downloading {query} con yt_dlp: {e}")
        return None


#  ✂️ Recortar audio a 30s

# In[10]:


def trim_audio(audio_path, output_dir="../data/audio_files/trimmed"):
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
        else:
            return None
    except Exception as e:
        logging.error(f"Error while trimming {audio_path}: {e}")
        return None


# # 🧠 Enviar archivo a la API de ReccoBeats

# In[11]:


def analyze_with_reccobeats(trimmed_path):
    try:
        with open(trimmed_path, 'rb') as file:
            files = {
                'audioFile': (os.path.basename(trimmed_path), file, 'audio/mpeg')
            }

            headers = {
                'Accept': 'application/json'
            }

            response = requests.post(
                "https://api.reccobeats.com/v1/analysis/audio-features",
                files=files,
                headers=headers
            )

            if response.status_code == 200:
                return response.json(), None
            else:
                return None, f"{response.status_code} {response.reason}"

    except Exception as e:
        return None, str(e)


# # 🚀 Procesar

# In[12]:


results = []

for _, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Analizyng with ReccoBeats"):
    nominee = row["nominee"]
    filename = safe_filename(nominee) + ".mp3"
    audio_path = os.path.join(AUDIO_DIR, filename)

    if not os.path.exists(audio_path):
        audio_path = download_audio(nominee)

    if audio_path and os.path.exists(audio_path):
        trimmed = trim_audio(audio_path)
        if trimmed:
            features, error = analyze_with_reccobeats(trimmed)
            os.remove(trimmed)
        else:
            features, error = None, "Trimmed audio not found"
    else:
        features, error = None, "Invalid file or not found"

    results.append({
        "nominee": nominee,
        "features": features,
        "error": error
    })


# # 💾 Guardar resultados

# In[13]:


# Guardamos como JSON
json_path = "../data/raw/reccobeats_features.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
logging.info(f"Results saved in JSON format: {json_path}")

# También como CSV (solo features en columnas si están disponibles)
df_results = pd.DataFrame(results)

# Expandimos los dicts de 'features' a columnas separadas
features_df = df_results.dropna(subset=["features"]).copy()
features_expanded = features_df["features"].apply(pd.Series)
features_combined = pd.concat([features_df[["nominee"]], features_expanded], axis=1)

csv_path = "../data/external/reccobeats_features.csv"
features_combined.to_csv(csv_path, index=False)
logging.info(f"Results saved in CSV format: {csv_path}")


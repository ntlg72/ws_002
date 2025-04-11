# src/etl/merge.py
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from src.params import Params

params = Params()
TEMP_DIR = Path(params.intermediate_data) / "temp"
MERGED_OUTPUT = Path(params.processed_data) / "final_data.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KEY_MAPPING = {
    -1: 'No Key Detected', 0: 'C', 1: 'C♯/D♭', 2: 'D', 3: 'D♯/E♭',
    4: 'E', 5: 'F', 6: 'F♯/G♭', 7: 'G', 8: 'G♯/A♭', 9: 'A', 10: 'A♯/B♭', 11: 'B', 'missing': 'Unknown'
}
MODE_MAPPING = {0: 'Minor', 1: 'Major', 'missing': 'Unknown'}

def merge_spotify_with_api(spotify: pd.DataFrame, api: pd.DataFrame) -> pd.DataFrame:
    api_renamed = api.rename(columns={'nominee': 'track_name'})
    merged = pd.merge(spotify, api_renamed, on='track_name', how='left', suffixes=('_spotify', '_api'))

    features = [
        'acousticness', 'danceability', 'energy', 'instrumentalness',
        'liveness', 'loudness', 'speechiness', 'tempo', 'valence'
    ]
    for feat in features:
        merged[feat] = merged[f'{feat}_api'].combine_first(merged.get(f'{feat}_spotify'))

    merged.drop(columns=[f"{f}_api" for f in features if f"{f}_api" in merged.columns] +
                      [f"{f}_spotify" for f in features if f"{f}_spotify" in merged.columns],
                inplace=True)

    return merged

def add_unmatched_api_nominees(merged: pd.DataFrame, api: pd.DataFrame, spotify_cols: list) -> pd.DataFrame:
    unmatched = api[~api["nominee"].isin(merged["track_name"])].copy()
    unmatched = unmatched.rename(columns={"nominee": "track_name"})
    for col in spotify_cols:
        if col not in unmatched:
            unmatched[col] = np.nan
    return pd.concat([merged, unmatched[spotify_cols]], ignore_index=True)

def merge_with_grammy(df: pd.DataFrame, grammy: pd.DataFrame) -> pd.DataFrame:
    grammy_renamed = grammy.rename(columns={'nominee': 'track_name', 'artist': 'artists'})
    return pd.merge(df, grammy_renamed, on=['track_name', 'artists'], how='left')

def enrich_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df['winner'] = df['winner'].fillna(False)
    df['artists'] = df['artists'].fillna('Unknown Artist')
    df['album_name'] = df['album_name'].fillna('Unspecified Album')

    defaults = {
        'popularity': -1,
        'duration_ms': df['duration_ms'].median(),
        'explicit': 'unknown',
        'key': 'missing',
        'mode': 'missing',
        'time_signature': -1,
        'track_genre': 'unknown_genre'
    }
    for col, default in defaults.items():
        df[col] = df[col].fillna(default)

    df['minutes'] = (df['duration_ms'] / 60000).round(2)
    df['key'] = df['key'].map(KEY_MAPPING)
    df['mode'] = df['mode'].map(MODE_MAPPING)

    df['data_source'] = np.where(df['artists'] == 'Unknown Artist', 'API Only', 'Spotify + API')
    df['data_quality'] = np.where(df['data_source'] == 'API Only', 'Partial Metadata (API Only)', 'Full Metadata (Spotify + API)')
    df['requires_spotify_verification'] = df['data_source'] == 'API Only'

    return df

def merge_and_enrich_datasets():
    """
    Loads datasets from TEMP_DIR, performs merging and enrichment, and saves result to processed folder.
    """
    spotify = pd.read_csv(TEMP_DIR / "spotify_transformed.csv")
    api = pd.read_csv(TEMP_DIR / "reccobeats_features.csv")
    grammy = pd.read_csv(TEMP_DIR / "grammys_transformed.csv")

    merged = merge_spotify_with_api(spotify, api)
    merged = add_unmatched_api_nominees(merged, api, list(spotify.columns))
    merged = merge_with_grammy(merged, grammy)
    final = enrich_metadata(merged)

    final_cols = [
        'track_name', 'artists', 'album_name', 'data_source', 'data_quality',
        'track_id', 'popularity', 'duration_ms', 'minutes', 'explicit',
        'key', 'mode', 'time_signature', 'track_genre',
        'acousticness', 'danceability', 'energy', 'instrumentalness',
        'liveness', 'loudness', 'speechiness', 'tempo', 'valence',
        'requires_spotify_verification', 'winner'
    ]

    final[final_cols].to_csv(MERGED_OUTPUT, index=False)
    logging.info(f"Final merged dataset saved to: {MERGED_OUTPUT}")
    return final

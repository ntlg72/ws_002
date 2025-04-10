
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Tuple, Dict
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Genre consolidation mapping
CONSOLIDATED_GENRES = {
    'agressive-fusion': ['dubstep', 'grunge', 'metal'],
    'industrial': ['goth', 'heavy-metal', 'industrial'],
    'punk-rock': ['alt-rock', 'garage', 'hard-rock', 'j-rock', 'punk', 'punk-rock'],
    # ... (include full mapping from original script)
}

NON_SOUND_BASED_GENRES = [
    'british', 'brazilian', 'french', 'german', 'iranian',
    'swedish', 'spanish', 'indian', 'malay', 'turkish',
    'world-music', 'gospel'
]

KEY_MAPPING = {
    -1: 'No Key Detected',
    0: 'C',
    1: 'C\u266f/D\u266d',
    # ... (include full key mapping from original script)
}

def initial_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df = df.dropna()
    df = df.drop_duplicates()
    logging.info("Performed initial cleaning")
    return df

def resolve_track_id_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    def _resolve_popularity(group: pd.Series) -> int:
        modes = group.mode()
        return modes[0] if len(modes) == 1 else group.median().astype(int)

    def _resolve_genre(group: pd.Series) -> str:
        modes = group.mode()
        return modes[0] if len(modes) == 1 else group.iloc[0]

    df['popularity'] = df.groupby('track_id')['popularity'].transform(_resolve_popularity)
    df['track_genre'] = df.groupby('track_id')['track_genre'].transform(_resolve_genre)
    df = df.drop_duplicates(subset='track_id', keep='first')
    logging.info("Resolved track_id duplicates")
    return df

def filter_genres(df: pd.DataFrame) -> pd.DataFrame:
    df = df[~df['track_genre'].isin(NON_SOUND_BASED_GENRES)]
    logging.info("Filtered non-sound-based genres")
    return df

def transform_features(df: pd.DataFrame) -> pd.DataFrame:
    df['duration_min'] = (df['duration_ms'] / 60000).round(2)
    df['mode'] = df['mode'].map({0: 'minor', 1: 'major'})
    df['key'] = df['key'].map(KEY_MAPPING)
    df['explicit'] = df['explicit'].map({True: 'Yes', False: 'No'})
    logging.info("Transformed features")
    return df

def consolidate_music_genres(df: pd.DataFrame) -> pd.DataFrame:
    genre_map = {old: new for new, old_list in CONSOLIDATED_GENRES.items() for old in old_list}
    df['track_genre'] = df['track_genre'].replace(genre_map)
    logging.info("Consolidated music genres")
    return df

def process_spotify_data(input_path: str = None, output_path: str = None) -> pd.DataFrame:
    """
    Main processing function. If no paths are given, uses temp paths.

    Args:
        input_path (str): Optional path to input file.
        output_path (str): Optional path to save output file.

    Returns:
        pd.DataFrame: Processed DataFrame
    """
    if input_path is None:
        input_path = str(Path(tempfile.gettempdir()) / "reccobeats_tmp" / "external" / "spotify_dataset.csv")
    if output_path is None:
        output_path = str(Path(tempfile.gettempdir()) / "reccobeats_tmp" / "intermediate" / "spotify_processed.csv")

    logging.info("Starting Spotify data processing")
    df = initial_cleaning(df)
    df = resolve_track_id_duplicates(df)
    df = filter_genres(df)
    df = consolidate_music_genres(df)
    df = transform_features(df)
    logging.info("Processing completed successfully")
    return df

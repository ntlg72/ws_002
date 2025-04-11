import pandas as pd
import numpy as np
import logging
from src.params import Params
from pathlib import Path

params = Params()

# Setup logging
# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Paths
TEMP_DIR = Path(params.intermediate_data) / "temp"
INPUT_PATH = TEMP_DIR / "spotify_loaded.csv"
OUTPUT_PATH = Path(params.intermediate_data) / "spotify_transformed.csv"

# --- Genre mapping ---
CONSOLIDATED_GENRES = {
    'agressive-fusion': ['dubstep', 'grunge', 'metal'],
    'industrial': ['goth', 'heavy-metal', 'industrial'],
    'punk-rock': ['alt-rock', 'garage', 'hard-rock', 'j-rock', 'punk', 'punk-rock'],
    'hardstyle': ['happy', 'hardstyle'],
    'disco-ska': ['disco', 'ska', 'synth-pop'],
    'rock': ['alternative', 'rock'],
    'anime': ['anime', 'club'],
    'edm-house': ['deep-house', 'electronic', 'progressive-house'],
    'edm': ['dub', 'edm', 'electro', 'groove', 'house'],
    'j-dance': ['dancehall', 'j-dance'],
    'funk-hip-hop': ['funk', 'hip-hop'],
    'latin': ['dance', 'latin', 'latino', 'reggae', 'reggaeton'],
    'pop': ['k-pop', 'pop', 'pop-film'],
    'brazilian': ['brazil', 'mpb'],
    'blues-rnb': ['blues', 'j-pop', 'r-n-b'],
    'indie': ['folk', 'indie', 'indie-pop', 'psych-rock'],
    'chill': ['chill', 'sad'],
    'pagode-samba': ['pagode', 'samba', 'sertanejo'],
    'country-soul': ['country', 'soul'],
    'rock-n-roll': ['rock-n-roll', 'rockabilly'],
    'chicago-house': ['chicago-house', 'detroit-techno'],
    'jazz-tango': ['honky-tonk', 'jazz', 'tango'],
    'vocal-pop': ['acoustic', 'cantopop', 'mandopop', 'singer-songwriter', 'songwriter'],
    'disney': ['disney', 'guitar'],
    'soundscape': ['ambient', 'new-age'],
    'hardstyle': ['happy', 'hardstyle'],
    'disco-ska': ['disco', 'ska', 'synth-pop'],
    'rock': ['alternative', 'rock'],
    'anime': ['anime', 'club'],
    'edm-house': ['deep-house', 'electronic', 'progressive-house'],
    'edm': ['dub', 'edm', 'electro', 'groove', 'house'],
    'j-dance': ['dancehall', 'j-dance'],
    'funk-hip-hop': ['funk', 'hip-hop'],
    'latin': ['dance', 'latin', 'latino', 'reggae', 'reggaeton'],
    'pop': ['k-pop', 'pop', 'pop-film'],
    'brazilian': ['brazil', 'mpb'],
    'blues-rnb': ['blues', 'j-pop', 'r-n-b'],
    'indie': ['folk', 'indie', 'indie-pop', 'psych-rock'],
    'chill': ['chill', 'sad'],
    'pagode-samba': ['pagode', 'samba', 'sertanejo'],
    'country-soul': ['country', 'soul'],
    'rock-n-roll': ['rock-n-roll', 'rockabilly'],
    'chicago-house': ['chicago-house', 'detroit-techno'],
    'jazz-tango': ['honky-tonk', 'jazz', 'tango'],
    'vocal-pop': ['acoustic', 'cantopop', 'mandopop', 'singer-songwriter', 'songwriter'],
    'disney': ['disney', 'guitar'],
    'soundscape': ['ambient', 'new-age']
}

NON_SOUND_BASED_GENRES = [
    'british', 'brazilian', 'french', 'german', 'iranian', 'swedish', 'spanish',
    'indian', 'malay', 'turkish', 'world-music', 'gospel'
]

KEY_MAPPING = {
    -1: 'No Key Detected', 0: 'C', 1: 'C♯/D♭', 2: 'D', 3: 'D♯/E♭',
    4: 'E', 5: 'F', 6: 'F♯/G♭', 7: 'G', 8: 'G♯/A♭', 9: 'A', 10: 'A♯/B♭', 11: 'B'
}

# --- Cleaning + Transformation pipeline ---

def clean_spotify_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df = df.dropna()
    df = df.drop_duplicates()
    return df

def resolve_popularity(group: pd.Series) -> int:
    modes = group.mode()
    if len(modes) == 1:
        return modes[0]
    elif len(modes) > 1:
        return max(modes)
    else:
        return int(group.median())
def resolve_popularity(group: pd.Series) -> int:
    modes = group.mode()
    if len(modes) == 1:
        return modes[0]
    elif len(modes) > 1:
        return max(modes)
    else:
        return int(group.median())

def resolve_track_genre(df: pd.DataFrame, id_col: str = 'track_id') -> pd.DataFrame:
    def resolve(group):
        modes = group['track_genre'].mode()
        return modes.iloc[0] if len(modes) == 1 else group['track_genre'].iloc[0]
    
    resolved = df.groupby(id_col).apply(resolve).reset_index(name='resolved_genre')
    df = df.merge(resolved, on=id_col)
    df['track_genre'] = df['resolved_genre']
    return df.drop(columns=['resolved_genre'])

def consolidate_genres(df: pd.DataFrame) -> pd.DataFrame:
    genre_map = {old: new for new, old_list in CONSOLIDATED_GENRES.items() for old in old_list}
    df['track_genre'] = df['track_genre'].replace(genre_map)
    df = df[~df['track_genre'].isin(NON_SOUND_BASED_GENRES)]
    return df

def drop_track_id_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df['popularity'] = df.groupby('track_id')['popularity'].transform(resolve_popularity).astype(int)
    df = resolve_track_genre(df)
    return df.drop_duplicates(subset='track_id', keep='first')

def transform_spotify_features(df: pd.DataFrame) -> pd.DataFrame:
    df['minutes'] = (df['duration_ms'] / 60000).round(2)
    df['mode_nominal'] = df['mode'].map({0: 'minor', 1: 'major'}).astype(str)
    df['key_nominal'] = df['key'].map(KEY_MAPPING)
    df['explicit'] = df['explicit'].map({True: 'Yes', False: 'No'})
    return df

def transform_spotify_dataset(**kwargs) -> None:
    """
    Loads the Spotify dataset from a temp file, transforms it, and saves the result.
    Meant for use in Airflow DAGs.
    """
    # Ensure the directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)
    df = clean_spotify_data(df)
    df = consolidate_genres(df)
    df = drop_track_id_duplicates(df)
    df = transform_spotify_features(df)

    df.to_csv(OUTPUT_PATH, index=False)
    logging.info(f"Transformed Spotify data saved to: {OUTPUT_PATH}")

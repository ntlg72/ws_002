import pandas as pd
import logging
import re
from rapidfuzz import process
from src.params import Params
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

params = Params()
TEMP_DIR = Path(params.intermediate_data) / "temp"
INPUT_PATH = TEMP_DIR / "grammys_loaded.csv"
OUTPUT_PATH = Path(params.intermediate_data) / "grammys_transformed.csv"

def update_columns(df, updates):
    for index, value in updates.items():
        df.loc[index, 'nominee'] = value
        df.loc[index, 'artist'] = value
    return df

def assign_nominee_to_artist(df, category):
    df.loc[(df['category'] == category) & (df['artist'].isnull()), 'artist'] = df['nominee']
    return df

def update_artist_from_workers_advanced(df):
    artist_roles = ["artist", "artists", "composer", "conductor", "conductors", "conductor/soloist", "soloist"]
    non_artist_roles = ["producer", "engineer", "songwriter", "arranger", "mastering", "remixer"]

    def extract_artists(workers):
        if not isinstance(workers, str):
            return None
        match = re.search(r'\((.*?)\)', workers)
        if match:
            return match.group(1).strip()
        if ";" in workers:
            return " / ".join(part.strip() for part in workers.split(";"))
        artists = []
        for part in [p.strip() for p in workers.split(",")]:
            lowered = part.lower()
            if any(role in lowered for role in artist_roles):
                artists.append(part)
            elif not any(role in lowered for role in non_artist_roles):
                artists.append(part)
        return " / ".join(artists) if artists else None

    df["artist"] = df["artist"].replace(["None", ""], pd.NA)
    df.loc[df["artist"].isna(), "artist"] = df["workers"].apply(extract_artists)
    df.drop(columns=["workers"], inplace=True)
    return df

def normalize_category(category, reference_list):
    try:
        if category.lower() in [ref.lower() for ref in reference_list]:
            return category
        result = process.extractOne(category, reference_list)
        match, score = result[0], result[1]
        return match if score >= 95 else category
    except Exception as e:
        logging.error(f"Error normalizing category: {category} | {e}")
        return category

def normalize_categories(df, reference_list):
    df['normalized_category'] = df['category'].apply(lambda x: normalize_category(x, reference_list))
    df.drop(columns=['category'], inplace=True)
    return df

def transform_grammy_dataset(**kwargs):
    """
    Transforms the raw Grammy dataset stored in TEMP_DIR and saves
    the cleaned version in intermediate_data/grammys_transformed.csv
    for later use in the DAG.
    """
    reference_categories = [
        'Record Of The Year', 'Album Of The Year', 'Song Of The Year', 'Best New Artist',
        'Producer Of The Year, Non-Classical', 'Songwriter Of The Year, Non-Classical',
        'Pop Solo Performance', 'Pop Duo/Group Performance', 'Pop Vocal Album',
        'Dance/Electronic Recording', 'Dance Pop Recording', 'Dance/Electronic Album',
        'Remixed Recording', 'Rock Performance', 'Metal Performance', 'Rock Song',
        'Rock Album', 'Alternative Music Performance', 'Alternative Music Album',
        'R&B Performance', 'Traditional R&B Performance', 'R&B Song', 'Progressive R&B Album',
        'R&B Album', 'Rap Performance', 'Melodic Rap Performance', 'Rap Song', 'Rap Album',
        'Spoken Word Poetry Album', 'Jazz Performance', 'Jazz Vocal Album', 'Jazz Instrumental Album',
        'Large Jazz Ensemble Album', 'Latin Jazz Album', 'Alternative Jazz Album',
        'Traditional Pop Vocal Album', 'Contemporary Instrumental Album',
        'Musical Theater Album', 'Country Solo Performance', 'Country Duo/Group Performance',
        'Country Song', 'Country Album', 'American Roots Performance', 'Americana Performance',
        'American Roots Song', 'Americana Album', 'Bluegrass Album', 'Traditional Blues Album',
        'Contemporary Blues Album', 'Folk Album', 'Regional Roots Music Album',
        'Gospel Performance/Song', 'Contemporary Christian Music Performance/Song',
        'Gospel Album', 'Contemporary Christian Music Album', 'Roots Gospel Album',
        'Latin Pop Album', 'Música Urbana Album', 'Latin Rock or Alternative Album',
        'Música Mexicana Album (Including Tejano)', 'Tropical Latin Album', 'Global Music Performance',
        'African Music Performance', 'Global Music Album', 'Reggae Album', 'New Age, Ambient, Or Chant Album',
        "Children's Music Album", 'Comedy Album', 'Audio Book, Narration and Storytelling Recording',
        'Compilation Soundtrack For Visual Media', 'Score Soundtrack For Visual Media',
        'Score Soundtrack For Video Games and Other Interactive Media', 'Song Written For Visual Media',
        'Music Video', 'Music Film', 'Recording Package', 'Boxed/Special Limited Edition Package',
        'Album Notes', 'Historical Album', 'Engineered Album, Non-Classical', 'Engineered Album, Classical',
        'Producer Of The Year, Classical', 'Immersive Audio Album', 'Instrumental Composition',
        'Arrangement, Instrumental Or A Cappella', 'Arrangement, Instruments And Vocals',
        'Orchestral Performance', 'Opera Recording', 'Choral Performance', 'Chamber Music/Small Ensemble Performance',
        'Classical Instrumental Solo', 'Classical Solo Vocal Album', 'Classical Compendium',
        'Contemporary Classical Composition'
    ]

    df = pd.read_csv(INPUT_PATH)

    df = df.drop(columns=['img', 'published_at', 'updated_at'], errors='ignore')
    df = update_columns(df, {
        2274: "Hex Hector", 2372: "Club 69 (Peter Rauhofer)", 2464: "David Morales",
        2560: "Frankie Knuckles", 4527: "The Statler Brothers", 4574: "Roger Miller"
    })

    for category in [
        "Producer Of The Year (Non-Classical)", "Best New Artist", "Best New Artist Of The Year",
        "Producer Of The Year, Non-Classical", "Producer Of The Year, Classical",
        "Producer Of The Year", "Classical Producer Of The Year"
    ]:
        df = assign_nominee_to_artist(df, category)

    df = update_artist_from_workers_advanced(df)
    df = normalize_categories(df, reference_categories)

    grammy = df.drop_duplicates(subset=['year', 'normalized_category'], keep='first')
    grammy.to_csv(OUTPUT_PATH, index=False)

    logging.info(f"Transformed Grammy dataset saved to: {OUTPUT_PATH}")

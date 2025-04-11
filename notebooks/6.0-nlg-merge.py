#!/usr/bin/env python
# coding: utf-8

# In[22]:


import os
import sys
import pandas as pd
import numpy as np
import logging


sys.path.append(os.path.abspath('../'))
from src.logging_config import setup_logging


# In[23]:


grammy = pd.read_csv("/home/bb-8/Dags/ws_002/data/intermediate/grammys.csv")
spotify = pd.read_csv("/home/bb-8/Dags/ws_002/data/intermediate/spotify.csv")
api = pd.read_csv("/home/bb-8/Dags/ws_002/data/intermediate/api.csv")


# ## Reviewing `features` in each dataset

# In[24]:


api.info()


# In[25]:


grammy.info()


# In[26]:


spotify.describe(include="all")


# 

# In[27]:


def merge_and_add_nominees(df_api, df_spotify, df_grammy=None):
    """
    Merges song data from API (nominees) with Spotify tracks, preserving artist information.
    Adds unmatched songs and optionally merges Grammy winner status.
    """
    # Merge on song names (nominee = track_name)
    merged_df = pd.merge(
        df_spotify,
        df_api.rename(columns={'nominee': 'track_name'}),
        on='track_name',
        how='left',
        suffixes=('_spotify', '_api')
    )

    # List of audio features to combine
    audio_features = ['acousticness', 'danceability', 'energy',
                    'instrumentalness', 'liveness', 'loudness',
                    'speechiness', 'tempo', 'valence']

    # Combine audio features (prioritize API data)
    for feature in audio_features:
        merged_df[feature] = merged_df[f'{feature}_api'].combine_first(merged_df[f'{feature}_spotify'])

    # Clean up columns
    merged_df.drop(columns=[f'{feature}_api' for feature in audio_features], inplace=True)
    merged_df.drop(columns=[f'{feature}_spotify' for feature in audio_features], inplace=True)

    # Identify unmatched songs (nominees not in Spotify)
    unmatched_songs = df_api[~df_api['nominee'].isin(df_spotify['track_name'])].copy()
    unmatched_songs = unmatched_songs.rename(columns={'nominee': 'track_name'})

    # Preserve original Spotify columns structure
    spotify_columns = df_spotify.columns.tolist()
    for col in spotify_columns:
        if col not in unmatched_songs:
            unmatched_songs[col] = np.nan  # Fill missing Spotify data

    # Combine datasets
    final_df = pd.concat([merged_df, unmatched_songs[spotify_columns]], ignore_index=True)

    # Add Grammy winner status if available
    if df_grammy is not None:
        final_df = pd.merge(
            final_df,
            df_grammy.rename(columns={'nominee': 'track_name'}),
            on='track_name',
            how='left'
        )

    return final_df


# In[28]:


result_df = merge_and_add_nominees(df_api=api, df_spotify=spotify, df_grammy=grammy)
print(result_df.shape)


# ## Reviewing nulls

# In[29]:


result_df.isnull().sum() 


# ### Key Observations
# 1. **Complete Columns (0 Nulls):**
#    - `track_name`, audio features (`acousticness`, `danceability`, etc.)
#    - **Why:** These exist in both API and Spotify data, with API values filling Spotify gaps
# 
# 2. **Spotify-Specific Nulls (126 Nulls):**
#    - `artists`, `album_name`, `popularity`, `duration_ms`, etc.
#    - **Cause:** These represent 126 API nominees not found in Spotify's catalog
#    - **Solution:** Manual artist/title disambiguation or accept missing Spotify metadata
# 
# 3. **Massive Nulls (79,631):**
#    - `year`, `title`, `artist`, `winner`
#    - **Root Cause:** These columns likely come from `df_grammy` and indicate:
#      - Grammy data only exists for `84,374 - 79,631 = 4,743` tracks
#      - Mismatched merge keys (track_name alone isn't sufficient for Grammy matching)
# 

# ## Action Plan

# ### Set default for missing winners (False)

# In[30]:


result_df['winner'] = result_df['winner'].fillna(False)


# ### Fix Grammy Merging

# #### Fix Grammy Merging:

# Merge using both track and artist for higher accuracy

# In[31]:


final_df = pd.merge(
    result_df,
    grammy.rename(columns={
        'nominee': 'track_name',
        'artist': 'artists'  # Match Spotify's column name
    }),
    on=['track_name', 'artists'],
    how='left'
)


# ### Remove columns with >90% nulls

# In[32]:


# Remove columns with >90% nulls
final_df = final_df.loc[:, final_df.isnull().mean() < 0.9]


# ### Prioritize Data Sources:

# In[33]:


# Create data source indicator
final_df['data_source'] = np.where(
    final_df['artists'].isnull(),
    'API Only',
    'Spotify + API'
)


# In[34]:


final_df.isnull().sum() 


# ## Reviewing nulls again

# **Spotify-Specific Missing Columns (126 nulls each)**
# 
# These represent API nominees not found in Spotify. Preserve these records while handling missingness:

# ### 1. Column Categorization & Treatment Strategy
# ### Core Identifiers (Preserve Structure)

# In[35]:


final_df['artists'] = final_df['artists'].fillna('Unknown Artist')
final_df['album_name'] = final_df['album_name'].fillna('Unspecified Album')


# ### Spotify Metadata (Impute with Meaningful Values)

# In[36]:


spotify_metadata = {
    'popularity': ('Missing', -1),
    'duration_ms': ('Median', final_df['duration_ms'].median()),
    'explicit': ('Unknown', 'unknown'),
    'key': ('Missing Category', 'missing'),
    'mode': ('Missing Category', 'missing'),
    'time_signature': ('Missing Code', -1),
    'track_genre': ('Unknown Genre', 'unknown_genre')
}

for col, (strategy, value) in spotify_metadata.items():
    if strategy == 'Median':
        final_df[col] = final_df[col].fillna(value)
    else:
        final_df[col] = final_df[col].fillna(value).astype(str if isinstance(value, str) else 'category')


# ### C. Derived Features (Recreate from Imputed Data)

# In[37]:


# Recalculate temporal features
final_df['minutes'] = (final_df['duration_ms'] / 60000).round(2)

# Re-encode categorical representations
key_mapping = {
    **{i: v for i, v in enumerate(['C', 'C♯/D♭', 'D', 'D♯/E♭', 'E', 'F',
                                  'F♯/G♭', 'G', 'G♯/A♭', 'A', 'A♯/B♭', 'B'])},
    'missing': 'Unknown'
}

mode_mapping = {
    **{0: 'Minor', 1: 'Major'},
    'missing': 'Unknown'
}

final_df['key'] = final_df['key'].map(key_mapping)
final_df['mode'] = final_df['mode'].map(mode_mapping)


# ### 2. Data Quality Preservation

# In[38]:


# Add data source metadata
final_df['data_quality'] = np.where(
    final_df['data_source'] == 'API Only',
    'Partial Metadata (API Only)',
    'Full Metadata (Spotify + API)'
)

# Create validation flags
final_df['requires_spotify_verification'] = final_df['data_source'] == 'API Only'


# ### 3. Analytical Safeguards

# In[39]:


# Protect core audio features
audio_features = ['acousticness', 'danceability', 'energy',
                'instrumentalness', 'liveness', 'loudness',
                'speechiness', 'tempo', 'valence']
# Column organization
final_cols = [
    # Core identifiers
    'track_name', 'artists', 'album_name', 'data_source', 'data_quality',

    # Spotify metadata
    'track_id', 'popularity', 'duration_ms', 'minutes', 'explicit',

    # Musical features
    'key_nominal', 'mode_nominal', 'time_signature',

    # Audio characteristics
    *audio_features,

    # Validation
    'requires_spotify_verification', 'winner_x'
]

final_df = final_df[final_cols]


# This approach maintains the 126 API-originated records while enabling:
# 
# - Mixed-source analysis
# 
# - Clear data quality auditing
# 
# - Transparent reporting of data limitations

# In[40]:


final_df = final_df.drop(['key_nominal', 'mode_nominal'], axis=1)


# In[41]:


final_df.isnull().sum() 


# In[42]:


final_df.to_csv('../data/processed/final_data.csv', index=False)

logging.info("DataFrame has been saved to '../data/intermediate/api.csv'")


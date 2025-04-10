#!/usr/bin/env python
# coding: utf-8

# # Workshop #2 :
# ## EDA - Spotify Tracks Dataset
# 
# ------------------------------------------------------------

# https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset

# In[1]:


import sys
import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from tabulate import tabulate
import logging

sys.path.append(os.path.abspath('../'))
from src.logging_config import setup_logging


# In[2]:


setup_logging()


# ## Data load
# -------------------

# In[3]:


# Path to the Spotify's dataset in the project directory
csv_file = '../data/external/spotify_dataset.csv'

df = pd.read_csv(csv_file)

df.head()


# ## Data Overview an Descriptive Statistics
# -------------------------------------------

# ### Overview

# The number of observations and features are obtained through Panda's `.shape` method. The "Spotify" dataset contains **114.000 observations (rows)** and **21 features (columns)**.

# In[4]:


df.shape


# The data types are obtained through Panda's `.dtypes` method. The Dataframe contains only 1 boolean feature, 5 object type features, 6 int64 type features and  9 float64 type features.
# 

# In[5]:


df.info()


# `Unnamed: 0` column is dropped as it is not part of the original dataset.

# In[6]:


df =  df.drop(columns=["Unnamed: 0"])


# The duplicated rows are obtained through Panda's `.duplicated` method. The Dataframe has 450 duplicate rows.

# In[7]:


df[df.duplicated()].shape[0]


# The missing values per feature are obtained through Panda's `.isnull().sum()` method. Only the features "artists", "album_name" and "track_name" have missing values, one missing value for each feature. This features, as indicated by their object datatype, are qualitative variables.

# In[8]:


df.isnull().sum()


# Rows with null values are filtered  using the `.isnull()` method combined with `.any(axis=1)`. Only one row contains the missing values.

# In[9]:


filtered_rows = df[df.isnull().any(axis=1)]

filtered_rows


# The percentage of missing data is aproximmately 0.0001%, which in itself is not very significative.

# In[10]:


round(df.isnull().sum().sum() / df.size * 100, 4) 


# ### Descriptive statistics

# #### Quantitative variables

# Descriptive statistics of quantitative are generated through Panda's `.describe` method.
# 
# 

# In[11]:


df.describe()


# #### Qualitative variables

# Panda's `.describe` method is used with the parameter `include='object'` for describing all qualitative columns of the DataFrame.

# In[12]:


df.describe(include='object') 


# 1. **Observation Count**: The column `track_id` matches the total number of observations (114,000), which indicates every row has a `track_id` entry.
# 
# 2. **Uniqueness**: Despite having 114,000 observations, `track_id` contains only 89,741 unique values. This suggests that some `track_id`s are repeated across multiple rows.
# 
# 4. **track_genre**: This column seems to have very few unique values (only 114), meaning it's highly categorical or repetitive.
# 
# 5. **Frequent Entries**: The `top` values show the most frequent entry for each column, and `freq` gives the count of that value.  "The Beatles" is the mode in `artists`; "Alternative Christmas 2022"	is the mode in `album_name`; and "Run Rudolph Run" is the mode in `track_name`; and acoustic is the mode in `track_genre`.

# ## Handling missing values
# ---------------------------

# As stablished previously, only one rows contains missing values, amounting to approximmately 0.0001%, of the data which in itself is not very significative.So this missing data is going to be dropped.

# In[13]:


df = df.dropna()


# Now the Dataframe has 113.999 rows and 20 columns.

# In[14]:


df.shape


# ## Handling duplicated values
# ---------------------------

# As stated previously the Dataframe has 450 duplicate rows.There are going to be dropped.

# In[15]:


df = df.drop_duplicates()


# In[16]:


logging.info(f"The Dataframe without duplicates has {df.shape[0]} rows and {df.shape[1]} columns")


# ### Inspecting the duplicated entries in `track_id`

# We create a `duplicates_id` Dataframe where containing only the rows where the column `track_id` has duplicates and check its dimensions to assert the number of duplicated rows.

# In[17]:


duplicates_id = df[df.duplicated(subset=['track_id'], keep=False)]
logging.info(f"The Dataframe with duplicates has {duplicates_id.shape[0]} rows.")


# Now we compute the difference in the number of rows and columns between our original DataFrame (`df`) and the filtered DataFrame of duplicates (`duplicates_id`). 

# In[18]:


difference = (df.shape[0] - duplicates_id.shape[0])

# Compute the percentage of non-duplicated rows
percentage_non_duplicated = (difference / df.shape[0]) * 100

# Print the result
logging.info(f"The number of non-duplicated rows is {difference}, which is {percentage_non_duplicated:.2f}% of the original Spotify DataFrame.")


# We need to check if pairs with duplicated `track_id` along with their respective `track_name` have a correspondence. To ensure that all pairs of duplicated `track_id`s have the same `track_name`, the data is grouped by `track_id` and we check if each group has only one unique `track_name`.
# 
# 1. **`groupby('track_id')`**: Groups the DataFrame by `track_id`.
# 2. **`nunique()`**: Counts the number of unique `track_name` values in each group.
# 3. **Check for inconsistencies**: Identifies `track_id`s where there is more than one unique `track_name`.

# In[19]:


grouped = df.groupby('track_id')['track_name'].nunique()

# Check for track_ids with more than one unique track_name
inconsistent = grouped[grouped > 1]

if inconsistent.empty:
    logging.info("All duplicated track_ids have the same track_name.")
else:
    logging.info("Some duplicated track_ids have inconsistent track_names.")


# As all duplicated `track_id`s have the same `track_name`s, we need to further inspect if there is anything that differentiates this duplicate tracks. 

# In[20]:


# Group by 'track_id' and check for identical rows within each group
identical_groups = duplicates_id.groupby('track_id').filter(
    lambda group: group.drop_duplicates().shape[0] == 1
)

if identical_groups.empty:
    logging.info("No duplicates are fully identical across all fields.")
else:
    logging.info("Fully identical rows:")
    logging.info(identical_groups.shape[0])


# Now we are going to check the inconsistencies with a function:
# 
# 
# 1. **Identify Duplicates**:
#    - The code creates a mask to find rows where the specified `id_col` (e.g., `track_id`) has duplicate values. It counts and prints the total number of duplicated rows.
# 
# 2. **Handle No Duplicates**:
#    - If no duplicates are found, it prints a message and returns an empty DataFrame with columns `id_col` and `inconsistent_columns`.
# 
# 3. **Analyze Inconsistencies**:
#    - For each unique duplicate identifier (`track_id`), it identifies columns where values differ across rows (`nunique() > 1`).
#    - It stores details about the inconsistencies, including:
#      - The identifier (`track_id`).
#      - The inconsistent columns.
#      - The number of duplicate entries for the identifier.
#      - Example values from one of the inconsistent columns.
# 
# 4. **Return Results**:
#    - If inconsistencies are found, it returns a DataFrame summarizing them.
#    - If all duplicates are consistent across columns, it prints a message and skips the detailed summary.

# In[21]:


def check_inconsistencies(df, id_col='track_id'):
    """
    Check for inconsistencies in duplicate records.

    This function identifies and analyzes duplicate entries in a DataFrame based on a unique identifier column (`id_col`).
    It checks for inconsistencies in other columns and returns a summary of the discrepancies.

    Parameters:
        df (pd.DataFrame): The DataFrame to analyze.
        id_col (str): The name of the column used to identify duplicates. Defaults to 'track_id'.

    Returns:
        pd.DataFrame: A DataFrame containing details about inconsistencies:
            - The identifier (`id_col`) of the duplicates.
            - The columns with inconsistent values.
            - The number of duplicate entries for each identifier.
            - Example inconsistent values for the first flagged column.

    Behavior:
        - Prints the total number of duplicate records.
        - If no duplicates are found, it prints a message and returns an empty DataFrame.
        - If duplicates are consistent across all columns, it prints a message.
        - Otherwise, it provides details about the inconsistencies in the duplicates.
    """

    dup_mask = df.duplicated(subset=id_col, keep=False)
    logging.info(f"Total duplicated registers: {dup_mask.sum()}")

    if not dup_mask.any():
        print("No duplicates to analyze")
        return pd.DataFrame(columns=[id_col, 'inconsistent_columns'])

    results = []
    for track_id in df.loc[dup_mask, id_col].unique():
        group = df[df[id_col] == track_id]
        inconsistent = [col for col in group.columns 
                       if col != id_col and group[col].nunique() > 1]
        if inconsistent:
            results.append({
                id_col: track_id,
                'inconsistent_columns': ', '.join(inconsistent),
                'n_duplicates': len(group),
                'example_values': str(group[inconsistent[0]].unique()[:3])  # Muestra primeros valores
            })

    if not results:
        logging.info("Duplicates found but consistent in all columns")

    return pd.DataFrame(results)


# The `check inconsistencies` function is applies to the `duplicates_id` Dataframe returning an `inconsistencies` Dataframe.

# In[22]:


inconsistencies = check_inconsistencies(duplicates_id)
if inconsistencies.empty:
    logging.info("No inconsistencies found in duplicates")
else:
    logging.info("Inconsistencies found:")
    display(inconsistencies)


# In the `inconsistencies` Dataframe the column `inconsistent_columns` contains information about which columns had inconsistencies for each duplicate entry. Through Panda's `.value_counts()` method we are going to count the occurrence of each unique value in the `inconsistent_columns` column to pinpoint wich are the inconsistent column or columns for duplicated tracks.

# In[23]:


logging.info(inconsistencies['inconsistent_columns'].value_counts())


# It seems only `track_genre` and `popularity` are the inconsistent columns between duplicated tracks. The feature `track_genre` is a qualitative nominal variable indicating the genre in which the track belongs; whereas `popularity` is an int64 value between 0 and 100, indicating the popularity of a track.
# 
# This entails the that we cannot handle duplicates for `popularity` using the mean, as it's not ideal for integers due to potential floating-point results. The median posses the same problem with even numbered duplicates, with two being the most commen number.
# 

# In[24]:


logging.info(inconsistencies['n_duplicates'].value_counts())


# #### Handling `popularity` duplicates

# In this case If each value in the duplicates is unique, there is no mode, and attempting to calculate it might result in an error. In such cases, we might need a fallback strategy, such as selecting the median, maximum, or arbitrarily choosing one value (e.g., the first).

# In[25]:


def resolve_popularity(x):

    modes = x.mode()
    if len(modes) == 1:  # Single mode
        return modes[0]
    elif len(modes) > 1:  # Multiple modes (tie)
        return max(modes)  # Choose the maximum (or any other criterion)
    else:  # No mode
        return x.median()  # Fallback to median


# In[26]:


df1 = df.copy() #copy of the dataset
df1['popularity'] = df1.groupby('track_id')['popularity'].transform(resolve_popularity).astype(int)


# #### Handling `track_genre` duplicates

# In[27]:


np.sort(df1.track_genre.unique())


# To focus on genre solely on sound characteristics, we will remove genres like ‘British’, ‘French’, or ‘German’ from the target variable. These classifications are based on origin or language, which aren’t captured by the audio features in the dataset.

# In[28]:


# Drop rows where the condition is True
non_sound_based_categories = ['british','brazilian','french','german','iranian','swedish','spanish','indian','malay','turkish','world-music','gospel']
df1 = df1.drop(df1[df1['track_genre'].isin(non_sound_based_categories)].index)


# Now we are going to consolidate genres into major genres and subgenders with a dictionary (this dictionary is the result of a machine learning excercise perfomed on this same dataset by Juan Francisco Leonhardt).It can be found in [Music Genre Classification: A Machine Learning Exercise](https://medium.com/@juanfraleonhardt/music-genre-classification-a-machine-learning-exercise-9c83108fd2bb)

# In[29]:


# Dictionary with descriptive names

consolidated_genres = {'agressive-fusion': ['dubstep', 'grunge', 'metal'],
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
                       'soundscape': ['ambient', 'new-age']}

# Create a dictionary to map old genres to new genres
genre_map = {old_genre: new_genre for new_genre, old_genres in consolidated_genres.items() for old_genre in old_genres}

# Replace the old genres with the new genres
df1['track_genre'] = df1['track_genre'].replace(genre_map)


# The remaining genres are the following.

# In[30]:


logging.info(np.sort(df1.track_genre.unique()))
logging.info(f"Total number of unique values: {df1['track_genre'].nunique()}")


# For `track_genre` we are going to handle duplicates with a function in the following way:
# 
# 
# 1. **Group by `track_id`**:
#    - Groups rows based on the unique identifier column (`track_id`).
# 
# 2. **Mode Check**:
#    - Uses `.mode()` to check for the most frequent value in the `track_genre` column.
#    - If there’s a single mode, it is selected as the resolved genre.
# 
# 3. **Fallback to First**:
#    - If there’s no mode (or multiple modes in a tie), it defaults to the first genre in the group (`iloc[0]`).
# 
# 4. **Reindexing**:
#    - Aligns the resolved genres with the original DataFrame indices for proper assignment.
# 

# In[31]:


def resolve_track_genre(df, id_col='track_id', genre_col='track_genre'):
    """
    Resolves inconsistencies in the `track_genre` column for duplicate entries.

    Parameters:
        df (pd.DataFrame): The DataFrame containing track data.
        id_col (str): The column representing unique identifiers (e.g., 'track_id').
        genre_col (str): The column containing track genres (e.g., 'track_genre').

    Returns:
        pd.DataFrame: The DataFrame with consistent `track_genre` for each `track_id`,
                      choosing the mode if it exists, or the first genre otherwise.
    """
    def resolve_genre(group):
        # Check if there is a mode
        modes = group[genre_col].mode()  # Returns a Series of modes
        if len(modes) == 1:
            # If there's one clear mode, return it
            return modes.iloc[0]
        else:
            # If there's no mode or multiple modes, return the first genre
            return group[genre_col].iloc[0]

    # Create a new column to store resolved genres
    resolved_genres = df.groupby(id_col).apply(
        lambda group: resolve_genre(group)
    )

    # Map the resolved genres back to the original DataFrame
    df[genre_col] = df[id_col].map(resolved_genres)

    return df


# In[32]:


# Resolve track_genre inconsistencies
df2 = df1.copy()
df2 = resolve_track_genre(df2, id_col='track_id', genre_col='track_genre')


# The final number of genres is 54.

# In[33]:


logging.info(np.sort(df2.track_genre.unique()))
logging.info(f"Total number of unique values: {df2['track_genre'].nunique()}")


# #### Final handling

# The functions for handling these columns only modify the data but do not remove duplicate rows automatically, so we need to verify and clean up duplicates explicitly.Bearing in mind this we verify the duplicates rows in `df2` are identical.

# In[34]:


# Group by 'track_id' and check for identical rows within each group
identical_groups = df2.groupby('track_id').filter(
    lambda group: group.drop_duplicates().shape[0] == 1
)

if identical_groups.empty:
    logging.info("No duplicates are fully identical across all fields.")
else:
    logging.info("Fully identical rows:")
    logging.info(identical_groups.shape[0])


# Now that we have verified it, we are going to drop the duplicates.We keep the first occurrence as the inconsistencies where already handled and duplicates rows contain the same data.

# In[35]:


df2 = df2.drop_duplicates(subset='track_id', keep='first')


# We verify the new dimensions of the `df2` Dataframe.

# In[36]:


logging.info(f"The new dimensions of the Dataframe after handling and dropping duplicates are {df2.shape[0]} rows and {df2.shape[1]} columns.")


#  ## Transforming `duration_ms` into minutes for readability
#  ----------------------------------------------------------

# We convert `durantion_ms` into minutes in a new column `minutes` for better understanding. 

# In[37]:


# Convert milliseconds to minutes
df2['minutes'] = (df2['duration_ms'] // 60000)  # Divide by 60000 to get minutes


# This might return floats instead of integers, so we have to ensure they are treated as such.

# In[38]:


df2['minutes'] = df2['duration_ms'].astype(float) 


# ## Transforming `mode`

# Mode indicates the modality (major or minor) of a track, the type of scale from which its melodic content is derived. Major is represented by 1 and minor is 0. We are going to verify if the data of this feature is boolean and if its values are "0" and "1". The result is that its values are indeed "0" and "1", but its data type is int64.

# In[39]:


logging.info(f"Number of unique values: {df2['mode'].nunique()}")
logging.info(f"Unique values: {df2['mode'].unique()}")
logging.info(f"Data type:{df2['mode'].dtype}")


# We are going to transformn this feature into a nominal feature for better undersatanding, mapping the ceros to "minor" and the ones to "major".

# In[40]:


df2['mode_nominal'] = df2['mode'].map({0: 'minor', 1: 'major'}).astype(str)


# We verify.

# In[41]:


logging.info(f"Number of unique values: {df2['mode_nominal'].nunique()}")
logging.info(f"Unique values: {df2['mode_nominal'].unique()}")
logging.info(f"Data type:{df2['mode_nominal'].dtype}")


# ## Transforming `key`

# The key the track is in. Integers map to pitches using standard Pitch Class notation. E.g. 0 = C, 1 = C♯/D♭, 2 = D, and so on. If no key was detected, the value is -1. We are going to verify if the data of this feature is int64 and if its values correspsond to the documented ones. The result is that its values are indeed int64 and numerical, and all keys are present (there are 12 chromatical `key`s), and no tracks without key detected.

# In[42]:


logging.info(f"Number of unique values: {df2['key'].nunique()}")
logging.info(f"Unique values: {np.sort(df2.key.unique())}") 
logging.info(f"Data type:{df2['key'].dtype}")


# We are going to transform this values into nominal ones for better understanding. This in done thrhough mapping and a dictionary constructed based on the documentation for this dataset. 

# In[43]:


# Define a mapping dictionary for keys
key_mapping = {
    -1: 'No Key Detected',
    0: 'C',
    1: 'C♯/D♭',
    2: 'D',
    3: 'D♯/E♭',
    4: 'E',
    5: 'F',
    6: 'F♯/G♭',
    7: 'G',
    8: 'G♯/A♭',
    9: 'A',
    10: 'A♯/B♭',
    11: 'B'
}

# Map the 'key' column to its corresponding pitch notation
df2['key_nominal'] = df2['key'].map(key_mapping)


# We verify again.

# In[44]:


logging.info(f"Number of unique values: {df2['key_nominal'].nunique()}")
logging.info(f"Unique values: {np.sort(df2.key_nominal.unique())}") 
logging.info(f"Data type:{df2['key_nominal'].dtype}")


# ## Transforming boolean values in `explicit`

# In[45]:


# Transform values to 'Yes' or 'No'
df2['explicit'] = df2['explicit'].map({True: 'Yes', False: 'No'})

# Log the data type of the column
logging.info(f"Data type of 'category_column': {df2['explicit'].dtype}")

# Log unique values in the column
unique_values = df2['explicit'].unique()
logging.info(f"Unique values in 'category_column': {unique_values}")


# In[47]:


# Save the DataFrame as a CSV file
df2.to_csv('../data/intermediate/spotify.csv', index=False)

logging.info("DataFrame has been saved to '../data/intermediate/spotify.csv'")


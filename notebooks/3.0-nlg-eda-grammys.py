#!/usr/bin/env python
# coding: utf-8

# # Workshop #2 :
# ## EDA - Grammy Awards Dataset
# 
# ------------------------------------------------------------

# https://www.kaggle.com/datasets/unanimad/grammy-awards

# ## Importing utilities & Setup

# In[169]:


import sys
import os
import pandas as pd
import numpy as np
import logging
import re


sys.path.append(os.path.abspath('../'))
from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging


# In[170]:


setup_logging()


# ## Data load

# A Parameters class centralizes anything that can be parametrized in the code. As we want to use the parameters for the connection to the PostgrSQL databases, params are instantiated as `params = Params()`.

# In[171]:


# Parames instance
params = Params()

# Connection to the database
db_client = DatabaseClient(params)


# The "Grammy Awards" data (in the table "grammys_raw") is fetched from our PostgreSQL database, using the `db_client`  `.engine` method.

# In[172]:


try:
    df = pd.read_sql_table("grammys_raw",  con=db_client.engine)
    logging.info("Data retrieved suscessfully.")

except Exception as e:

    logging.warning(f"An exception occurred: {e}")


# The connection to the dadtabase is close to save resources.

# In[173]:


try:
    db_client.close()
except Exception as e:
    logging.error(f"Failed to close connection to the database.")
    logging.error(f"Error details: {e}")


# ## Data Overview an Descriptive Statistics

# ### Overview

# The number of observations and features are obtained through Panda's `.shape` method. The "Spotify" dataset contains **4.810 observations (rows)** and **10 features (columns)**.

# In[174]:


df.shape


# The data types are obtained through Panda's `.dtypes` method. `Year` is the only numerical feature, and `winner` is the only boolean one. The other features are qualitative.
# 

# In[175]:


df.info()


# The duplicated rows are obtained through Panda's `.duplicated` method. 

# In[176]:


logging.info (f"There are {df[df.duplicated()].shape[0]} duplicated rows.")


# The missing values per feature are obtained through Panda's `.isnull().sum()` method. Only the features `artist`, `workers`, `nominee` and `img` have missing values. This features, as indicated by their object datatype, are qualitative variables.

# In[177]:


df.isnull().sum()


# Rows with null values are filtered  using the `.isnull()` method combined with `.any(axis=1)`. Only one row contains the missing values.

# In[178]:


filtered_rows = df[df.isnull().any(axis=1)]

logging.info(f"{filtered_rows.shape[0]} rows contain missing values")


# The percentage of missing data is aproximmately 11.23%.

# In[179]:


round(df.isnull().sum().sum() / df.size * 100, 4) 


# ### Descriptive statistics

# #### Quantitative variables

# Descriptive statistics of the only numeric feature (`year`) are generated through Panda's `.describe` method.
# 
# 

# In[180]:


df.describe()


# We need to review if the number of unique values of the column `year` is equal to the `title` of the Grammy's edition. It should be consistent.

# In[181]:


unique_years = df['year'].unique()

is_consistent = len(unique_years) == df['title'].nunique()

if is_consistent:
    logging.info("The number of unique years matches the Grammy's edition title.")
else:
    logging.info("Mismatch: The number of unique years does not match the Grammy's edition title.")


# #### Qualitative variables

# Panda's `.describe` method is used with the parameter `include='object'` for describing all qualitative columns of the DataFrame.

# In[182]:


df.describe(include='object') 


# 
#    - **`title`**: There are 62 unique titles, with the most frequent being *"62nd Annual GRAMMY Awards (2019)"* (appearing 433 times). This indicates repeated focus on specific award shows or events in the dataset.
#    - **`published_at`** and **`updated_at`**: Both are timestamps but differ significantly:
#      - `published_at` has only 4 unique dates, suggesting most records were initially published at the same time.
#      - `updated_at` shows 10 unique values, with the most frequent update timestamp appearing 778 times, indicating frequent revisions or modifications over time.
#    - **`category`**: Features 638 unique categories, with "Song Of The Year" being the most represented (70 times). This diversity highlights a rich variety of award categories.
#    - **`nominee`**: 4.131 unique nominees, with "Robert Woods" appearing most frequently (7 times). Some nominees likely appear multiple times across categories or events.
#    - **`artist`**: Only 1.658 unique artists, significantly fewer than nominees, with "(Various Artists)" being the most common entry (66 times). Due to the missing values in this feature, they could be less artists.
#    - **`workers`**: 2,366 unique worker entries (e.g., composers, engineers), with *John Williams, composer (John Williams)* appearing most often (20 times), reflecting repeated recognition for specific individuals.
#    - **`img`**: Contains 1,463 unique URLs, likely linking to visual media (e.g., images for nominees or events). However, 3443 entries in this column indicate incomplete visual data, that may not be relevant for our analysis.
# 

# Panda's `.describe` method is used with the parameter `include='boolean'` for describing the only boolean feature (`winner`) in the DataFrame.

# In[183]:


df.describe(include='boolean') 


# * There's only 1 unique value (`True`), meaning every entry in this column is the same.
# * The `winner` column might be redundant since it does not provide any variation or distinguishing information. If the purpose is to flag winners, this column's structure suggests that all entries are winners, and it may need to be reassessed or filtered further for meaningful analysis.

# ## Handling missing values

# ### `img`

# In this feature we are working with URLs for images and, regardless if the URL is valid or accessible, this type of content might not provide direct insights for now, so this column is going to be dropped. 

# This is applied to a copy of the original Dataframe.

# In[184]:


df1 = df.copy()
df1 = df1.drop('img', axis=1)


# ### `nominee` column

# The nominee column only presents 6 null records, which gives us the possibility to further inspect them in detail.

# In[185]:


filtered_rows = df1[df1['nominee'].isnull()]
filtered_rows


# We note that the missing values correspond to different years but mostly to the same category (Remixer of the Year, Non-Classical), and four consecutive years (1997-2000). To preserve the records about these entries we can input values by reviewing online information in the official Grammys website. In the cases ("Best" and "of the Year") the artist is the nominee at the same time.

# In[186]:


# Define the indices and their corresponding values
update = {
    2274: "Hex Hector",
    2372: "Club 69 (Peter Rauhofer)",
    2464: "David Morales",
    2560: "Frankie Knuckles",
    4527: "The Statler Brothers",
    4574: "Roger Miller"
}

# Update the 'nominee' column
for index, value in update.items():
    df1.loc[index, 'nominee'] = value

# Update the 'artist' column
for index, value in update.items():
    df1.loc[index, 'artist'] = value


# Now we review the changes.

# In[187]:


df1.loc[list(update.keys())]


# ### `artist` column

# In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time. However, it may not always be the case, so we are going to review this cases. 
# 

# ### Missing `artist` and `workers`

# We filter rows where `artist` and `workers` are null.

# In[188]:


# Filter rows where 'artist' and 'workers' are null
filtered_null_rows = df1[(df1['artist'].isnull()) & (df1['workers'].isnull())]

# Compute value counts for the 'category' column in these rows
category_counts = filtered_null_rows['category'].value_counts()

logging.info(category_counts)


# ### Value counts for the `category` column in these rows is one

# For the cases in which there are only one missing for category we are going to input data based on online information.

# In[189]:


#Filter categories where the count is 1
categories_with_one_count = category_counts[category_counts == 1].index

# Filter the original DataFrame using these categories
filtered_df = filtered_null_rows[filtered_null_rows['category'].isin(categories_with_one_count)]

# Display the filtered DataFrame
filtered_df


# We note that the missing values correspond to different years but mostly to the same category (Remixer of the Year, Non-Classical), and four consecutive years (1997-2000). To preserve the records about these entries we can input values by reviewing online information in the official Grammys website. In the cases ("Best" and "of the Year") the artist is the nominee at the same time.

# In[190]:


# Define the indices and their corresponding values
update1 = {
    3066: "David Foster",
    3076: "Dawn Upshaw",
    3366: "Narada Michael Walden",
    3515: "Chicago Pro Musica",
    4178: "Leontyne Price",
    4397: "Vladimir Horowitz",
    4569: "The Beatles",
    4628: "Ward Swingle(The Swingle Singers)",
    4667: "Robert Goulet",
    4699: "Peter Nero",
    4745: "Bob Newhart",
    4781: "Bobby Darin"
}


# Update the 'artist' column
for index, value in update1.items():
    df1.loc[index, 'artist'] = value


# Now we review the changes.

# In[191]:


df1.loc[list(update1.keys())]


# ### Value counts for the `category` column in these rows is more than one

# Of these categories with rows with missing `artist` and `workers` we need to filter those that do not contain a regular artist name to handle them separetely. In this case, the criteria is that they contain special characters in their `nominee` column. 

# In[192]:


# Filter rows where 'artist' and 'workers' are null
filtered_null_rows = df1[(df1['artist'].isnull()) & (df1['workers'].isnull())]

# Filter rows where 'nominee' contains special characters and 'artist' is null
filtered_rows = filtered_null_rows[(filtered_null_rows['nominee'].str.contains(r'[^\w\s]', regex=True))]

# Count the number of rows per 'category'
category_counts = filtered_rows['category'].value_counts()

# Display the counts
print(category_counts)


# As these columns may need reviewing and we operate on the assumption that those whitout special characters don't, we are going to input the `artist` value using the `nominee` value on the cases where `nominee` does not have special characters, and the `worker` is null.

# In[193]:


df1.loc[
    (df1["artist"].isnull()) & 
    (df1["workers"].isnull()) & 
    (~df1["nominee"].str.contains(r'[^\w\s]', regex=True)), 
    "artist"
] = df1["nominee"]


# ### Cases with special characters in `nominee`

# ##### Producer Of The Year (Non-Classical)

# In[194]:


def filter_rows_column_to_review(df, category):
    """
    Filters rows in the dataframe where the specified category matches
    and the 'artist' column is null.

    Args:
        df (DataFrame): The input dataframe.
        category (str): The category value to filter by.

    Returns:
        DataFrame: Filtered rows.
    """
    filtered_null_rows = df[(df['category'] == category) & (df['artist'].isnull())]
    return filtered_null_rows


# In[195]:


filter_rows_column_to_review(df1, "Producer Of The Year (Non-Classical)")


# It seems in this cases the special character is due to nominee being a duo, there is not major problem. When the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Producer Of The Year, Non-Classical".

# In[196]:


def assign_nominee_to_artist(df, category):
    """
    Assigns the value from the 'nominee' column to the 'artist' column
    for rows where the specified category matches and the 'artist' column is null.

    Args:
        df (DataFrame): The input dataframe.
        category (str): The category value to filter by.

    Returns:
        DataFrame: Updated dataframe with modifications.
    """
    df.loc[(df['category'] == category) & (df['artist'].isnull()), 'artist'] = df['nominee']
    return df


# In[197]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year (Non-Classical)")


# To review the changes we filter `category` for "Producer Of The Year (Non-Classical)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[198]:


def log_null_count(df, category):
    """
    Logs the count of null values in the 'artist' column for the specified category.

    Args:
        df (DataFrame): The input dataframe.
        category (str): The category value to filter by.

    Returns:
        int: The count of null values.
    """
    null_count = df[(df['category'] == category) & (df['artist'].isnull())].shape[0]
    logging.info(f"There are {null_count} null values in the 'artist' column for '{category}'.")
    return null_count


# In[199]:


log_null_count(df1, "Producer Of The Year (Non-Classical)")


# ##### 

# #### Best Classical Vocal Soloist Performance

# In[200]:


filter_rows_column_to_review(df1, "Best Classical Vocal Soloist Performance")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[201]:


def update_columns(df, updates):
    """
    Updates the 'nominee' and 'artist' columns in the dataframe based on the provided index-value mapping.

    Args:
        df (DataFrame): The input dataframe.
        updates (dict): A dictionary where keys are indices and values are the corresponding updates.

    Returns:
        DataFrame: Updated dataframe with modifications.
    """
    for index, value in updates.items():
        df.loc[index, 'nominee'] = value
        df.loc[index, 'artist'] = value
    return df


# In[202]:


def get_indices_list(df, category):
    """
    Retrieves the list of indices for rows filtered by the specified category.

    Args:
        df (DataFrame): The input dataframe.
        category (str): The category value to filter by.

    Returns:
        list: List of indices for the filtered rows.
    """
    indices_list = filter_rows_column_to_review(df, category).index.tolist()
    return indices_list


# In[203]:


get_indices_list(df1, "Best Classical Vocal Soloist Performance")


# In[204]:


updates_best_classical_vocal={1816:"Janet Baker",
                            3230:"Dawn Upshaw" ,
                            3303:"Luciano Pavarotti",
                            3374:"Kathleen Battle",
                            3443:"Kathleen Battle",
                            3514:"John Aler",
                            3585:"Heather Harper, Jessye Norman, Jose van Dam",
                            3654:"Marilyn Horne, Leontyne Price",
                            3718:"Leontyne Price",
                            3767:"Dietrich Fischer-Dieskau",
                            3775:"Marilyn Horne, Luciano Pavarotti, Joan Sutherland",
                            3835:"Leontyne Price",
                            3888:"Luciano Pavarotti",
                            3942:"Luciano Pavarotti",
                            4039:"Beverly Sills",
                            4088:"Janet Baker",
                            4183:"Leontyne Price",
                            4315:"Dietrich Fischer-Dieskau",
                            4450:"Leontyne Price",
                            4538:"Leontyne Price"

}

df1 = update_columns(df1, updates=updates_best_classical_vocal)


# To review the changes we filter `category` for "Best Classical Vocal Soloist Performance" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[205]:


log_null_count(df1, "Best Classical Vocal Soloist Performance")


# ##### 

# #### Best New Artist  

# In[206]:


filter_rows_column_to_review(df1, "Best New Artist")


# There does not appear to be anny issues, just `artist` that seem to be duos. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Best New Artist".

# In[207]:


df1 = assign_nominee_to_artist(df1, "Best New Artist")


# To review the changes we filter `category` for "Best New Artist" and check for nulls in the `artist` column. There are now none for this category.

# In[208]:


log_null_count(df1, "Best New Artist")


# #### Best Small Ensemble Performance (With Or Without Conductor) 

# In[209]:


filter_rows_column_to_review(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[210]:


get_indices_list(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# In[211]:


updates_best_small_essemble={2382:"Joseph Jennings (conductor) and Chanticleer", 
                             2475:"Steve Reich (conductor), Steve Reich and Musicians", 
                             2570:"Claudio Abbado (conductor), Berliner Philharmonic", 
                             2658:"Pierre Boulez (conductor) and the Ensemble Inter-Contemporain"

}

df1 = update_columns(df1, updates=updates_best_small_essemble)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[212]:


log_null_count(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# #### Best Classical Vocal Performance  

# In[213]:


filter_rows_column_to_review(df1, "Best Classical Vocal Performance")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[214]:


get_indices_list(df1, "Best Classical Vocal Performance")


# In[215]:


updates_best_vocal_classical_per={1213:"Cecilia Bartol",
 1322:"Renée Fleming,",
 1433:"Hila Plitmann",
 1543:"Lorraine Hunt Lieberson",
 1654:"Lorraine Hunt Lieberson",
 1762:"Thomas Quasthoff",
 1871:"Susan Graham",
 1977:"Thomas Quasthoff & Anne Sofie von Otter",
 2084:"Renée Fleming",
 2183:"Christoph Willibald Gluck",
 2286:"Cecilia Bartoli",
 2383:"Anne Sofie von Otter",
 2476:"Renée Fleming",
 2571:"Cecilia Bartoli",
 2659:"Bryn Terfel",
 2723:"Sylvia McNair",
 2834:"Cecilia Bartoli",
 2915:"Arleen Auger",
 2999:"Kathleen Battle",
 3154:"Placido Domingo"}

df1 = update_columns(df1, updates=updates_best_vocal_classical_per)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[216]:


log_null_count(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# #### Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)

# In[217]:


filter_rows_column_to_review(df1, "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[218]:


get_indices_list(df1, "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)")


# In[219]:


updates_best_soloist={3441: "Vladimir Horowitz", 
                    4312:"Cleveland Orchestra", 
                    4358: "Wendy Carlos", 
                    4446: "Vladimir Horowitz", 
                    4490: "Julian Bream"

}

df1 = update_columns(df1, updates=updates_best_soloist)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[220]:


log_null_count(df1, "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)")


# #### Most Promising New Classical Recording Artist

# In[221]:


filter_rows_column_to_review(df1, "Most Promising New Classical Recording Artist")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[222]:


get_indices_list(df1, "Most Promising New Classical Recording Artist")


# In[223]:


updates_best_promising_classical={4540:"Peter Serkin", 
                      4586:"Marilyn Horne", 
                      4616:"André Watts"

}

df1 = update_columns(df1, updates=updates_best_promising_classical)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[224]:


log_null_count(df1, "Most Promising New Classical Recording Artist")


# #### Producer Of The Year, Non-Classical

# In[225]:


filter_rows_column_to_review(df1, "Producer Of The Year, Non-Classical")


# There does not appear to be anny issues, just `artist` with dots or apostophres. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Best New Artist".

# In[226]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year, Non-Classical")


# To review the changes we filter `category` for "Producer Of The Year, Non-Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[227]:


log_null_count(df1, "Producer Of The Year, Non-Classical")


# #### Classical Producer Of The Year

# In[228]:


filter_rows_column_to_review(df1, "Classical Producer Of The Year")


# There does not appear to be anny issues, just and `artist` duo. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Classical Producer Of The Year".

# In[229]:


df1 = assign_nominee_to_artist(df1, "Classical Producer Of The Year")


# To review the changes we filter `category` for "Producer Of The Year, Non-Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[230]:


log_null_count(df1, "Classical Producer Of The Year")


# #### Producer Of The Year, Classical 

# In[231]:


filter_rows_column_to_review(df1, "Producer Of The Year, Classical")


# There does not appear to be anny issues, just and `artist` duo and an artist as a producer. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Producer Of The Year, Classical".

# In[232]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year, Classical")


# To review the changes we filter `category` for "Producer Of The Year, Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[233]:


log_null_count(df1, "Producer Of The Year, Classical")


# #### Producer Of The Year                                                                         

# In[234]:


filter_rows_column_to_review(df1, "Producer Of The Year")


# There does not appear to be anny issues, just and `artist` duo. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Producer Of The Year.

# In[235]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year")


# To review the changes we filter `category` for "Producer Of The Year" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[236]:


log_null_count(df1, "Producer Of The Year")


# #### Best New Artist Of The Year     

# In[237]:


filter_rows_column_to_review(df1, "Best New Artist Of The Year")


# There does not appear to be anny issues, just and `artist` duo. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Best New Artist Of The Year".

# In[238]:


df1 = assign_nominee_to_artist(df1, "Best New Artist Of The Year")


# To review the changes we filter `category` for "Producer Of The Year, Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[239]:


log_null_count(df1, "Best New Artist Of The Year")


# ### Missing `artist`

# We still have to handle the cases where `artist` is null, `worker` is not.

# In[240]:


# Filter rows where 'artist' is null and 'workers' is not null
filtered_null_rows = df1[(df1['artist'].isnull()) & (~df1['workers'].isnull())]

# Count null values specifically in the 'artist' column
null_count_artist = filtered_null_rows['artist'].isnull().sum()
logging.info(f"There are {null_count_artist} null values in the 'artist' column.")


# We inspect the firts 30 unique values in workers to asses the type of information available.

# In[241]:


df["workers"].dropna().unique()[:30]


# There seem to be roles that may help us input values in `artist` when `worker` is not null. Arround XXX of the records have special characters with parentheses that may help with these.

# In[242]:


# Filter rows where 'artist' is null and 'workers' is not null
filtered_rows = df1[(df1["artist"].isnull()) & (~df1["workers"].isnull())]

# Count how many rows in 'workers' contain parentheses
count_parentheses = filtered_rows["workers"].str.contains(r'\(.*?\)', na=False).sum()

# Log the count
logging.info(f"There are {count_parentheses} rows in 'workers' with parentheses where 'artist' is null.")


# Arround XXX of the records have the word "featuring", information that may be util for inputing data in `artist`.

# In[243]:


# Count how many rows in 'workers' contain the word 'featuring'
count_featuring = filtered_rows["workers"].str.contains(r'featuring', case=False, na=False).sum()

# Log the count
logging.info(f"There are {count_featuring} rows in 'workers' with the word 'featuring' where 'artist' is null.")


# Now, we are going to tetect roles (e.g., artist, composer, soloist) that frequently appear in the workers column to design extraction rules later.

# In[244]:


# Clean the 'workers' column and create a lowercase version
df1["workers_clean"] = df1["workers"].str.lower().fillna("")

# Filter rows where 'artist' is null and 'workers_clean' is not empty
filtered_rows = df1[(df1["artist"].isnull()) & (df1["workers_clean"] != "")]

# Extract roles from the 'workers_clean' column
roles_found = filtered_rows["workers_clean"].str.extractall(r',\s*([a-zA-Z/\s]+?)\b')
roles_found.columns = ["role_candidate"]

# Count occurrences of roles
role_counts = roles_found["role_candidate"].value_counts()

# Log all roles
logging.info(f"All roles:\n{role_counts.to_string()}")


# Note there are both strings separated by commmas and semicolons.

# In[245]:


df1[df1["workers"].str.contains(r'\(.*?\)', na=False)][["nominee", "artist", "workers"]].head(5)


# In[246]:


df1[df1["workers"].str.contains(r';', na=False)][["nominee", "artist", "workers"]].head(5)


# Through this identified traits, we are going to define a funtion to input values in `artist` in the cases when `artist` is null but worker is not.

# In[247]:


def update_artist_from_workers_advanced(df):
    """
    Enhances the 'artist' column by extracting artist names from the 'workers' column
    using common textual patterns and filtering by relevant roles.

    Applies the update only where 'artist' is null (missing).

    Extraction logic:
    1. Text inside parentheses (e.g., "(Billie Eilish)").
    2. Names separated by semicolons, prioritizing those with "featuring".
    3. Names separated by commas, filtering out known non-artist roles and keeping relevant ones.

    Args:
        df (pd.DataFrame): A DataFrame containing 'artist' and 'workers' columns.

    Returns:
        pd.DataFrame: Updated DataFrame with enriched 'artist' column and 'workers' column removed.
    """

    # Useful and non-useful roles for identifying main artists
    artist_roles = [
        "artist", "artists", "composer", "conductor", "conductors", "conductor/soloist", "choir director", "chorus master", "graphic designer", "soloist", "soloists", "ensembles", 
    ]


    non_artist_roles = [
        "producer", "producers",  "engineer", "engineers",
        "songwriter", "songwriters", "arranger", "arrangers", "mastering",
        "remixer", "art", "album", "surround", "compilation", "david", "john"
    ]

    def extract_artists(workers):
        if not isinstance(workers, str):
            return None

        # Step 1: Parentheses
        match = re.search(r'\((.*?)\)', workers)
        if match:
            return match.group(1).strip()

        # Step 2: Semicolon-separated names
        if ";" in workers:
            parts = [part.strip() for part in workers.split(";")]
            featuring_parts = [p for p in parts if "featuring" in p.lower()]
            if featuring_parts:
                return " / ".join(featuring_parts)
            return " / ".join(parts)

        # Step 3: Comma-separated names with role filtering
        artists = []
        for part in [p.strip() for p in workers.split(",")]:
            lowered = part.lower()
            if any(role in lowered for role in artist_roles):
                artists.append(part)
            elif not any(role in lowered for role in non_artist_roles):
                artists.append(part)
        if artists:
            return " / ".join(artists)

        return None

    # Normalize artist null values
    df["artist"] = df["artist"].replace(["None", ""], pd.NA)

    # Apply extraction only where artist is missing
    missing_mask = df["artist"].isna()
    df.loc[missing_mask, "artist"] = df.loc[missing_mask, "workers"].apply(extract_artists)

    # Drop workers column
    df.drop(columns=["workers"], inplace=True)

    return df


# In[248]:


df1 = update_artist_from_workers_advanced(df1)


# We review the remaining rows where 'artist' column is null by filtering.

# In[249]:


# Filter rows where 'artist' column is null
null_rows_df = df1[df1["artist"].isnull()]
null_rows_df


# We manualy set the artist value for these cases.

# In[250]:


updates_best_chamber={402:"Attacca quartet", 
                      404:"Publiquartet", 

}

df1 = update_columns(df1, updates=updates_best_chamber)


# Finally we review if any row has null values.

# In[251]:


logging.info(df1.isnull().sum())


# ## Disposing of columns

# ### `published_at` and `updated_at` columns

# In the analysis of the dataset I could not apply a clear use of the values of these two columns. The dates have no relation with the date of the event, or with any ephemeris related to this event. Therefore, I am going to delete them from the dataframe.

# In[252]:


# Drop the specified columns
df1 = df1.drop(columns=['published_at', 'updated_at'])


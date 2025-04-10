#!/usr/bin/env python
# coding: utf-8

# # Workshop #2 :
# ## EDA - Grammy Awards Dataset
# 
# ------------------------------------------------------------

# https://www.kaggle.com/datasets/unanimad/grammy-awards

# ## Importing utilities & Setup

# In[460]:


import sys
import os
import pandas as pd
from rapidfuzz import process
import logging 
import re


sys.path.append(os.path.abspath('../'))
from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging


# In[461]:


setup_logging()


# ## Data load

# A Parameters class centralizes anything that can be parametrized in the code. As we want to use the parameters for the connection to the PostgrSQL databases, params are instantiated as `params = Params()`.

# In[462]:


# Parames instance
params = Params()

# Connection to the database
db_client = DatabaseClient(params)


# The "Grammy Awards" data (in the table "grammys_raw") is fetched from our PostgreSQL database, using the `db_client`  `.engine` method.

# In[463]:


try:
    df = pd.read_sql_table("grammys_raw",  con=db_client.engine)
    logging.info("Data retrieved suscessfully.")

except Exception as e:

    logging.warning(f"An exception occurred: {e}")


# The connection to the dadtabase is close to save resources.

# In[464]:


try:
    db_client.close()
except Exception as e:
    logging.error(f"Failed to close connection to the database.")
    logging.error(f"Error details: {e}")


# ## Data Overview an Descriptive Statistics

# ### Overview

# The number of observations and features are obtained through Panda's `.shape` method. The "Spotify" dataset contains **4.810 observations (rows)** and **10 features (columns)**.

# In[465]:


df.shape


# The data types are obtained through Panda's `.dtypes` method. `Year` is the only numerical feature, and `winner` is the only boolean one. The other features are qualitative.
# 

# In[466]:


df.info()


# The duplicated rows are obtained through Panda's `.duplicated` method. 

# In[467]:


logging.info (f"There are {df[df.duplicated()].shape[0]} duplicated rows.")


# The missing values per feature are obtained through Panda's `.isnull().sum()` method. Only the features `artist`, `workers`, `nominee` and `img` have missing values. This features, as indicated by their object datatype, are qualitative variables.

# In[468]:


df.isnull().sum()


# Rows with null values are filtered  using the `.isnull()` method combined with `.any(axis=1)`. Only one row contains the missing values.

# In[469]:


filtered_rows = df[df.isnull().any(axis=1)]

logging.info(f"{filtered_rows.shape[0]} rows contain missing values")


# The percentage of missing data is aproximmately 11.23%.

# In[470]:


round(df.isnull().sum().sum() / df.size * 100, 4) 


# ### Descriptive statistics

# #### Quantitative variables

# Descriptive statistics of the only numeric feature (`year`) are generated through Panda's `.describe` method.
# 
# 

# In[471]:


df.describe()


# We have data from 1958 to 1019.We need to review if the number of unique values of the column `year` is equal to the `title` of the Grammy's edition. It should be consistent.

# In[472]:


unique_years = df['year'].unique()

is_consistent = len(unique_years) == df['title'].nunique()

if is_consistent:
    logging.info("The number of unique years matches the Grammy's edition title.")
else:
    logging.info("Mismatch: The number of unique years does not match the Grammy's edition title.")


# #### Qualitative variables

# Panda's `.describe` method is used with the parameter `include='object'` for describing all qualitative columns of the DataFrame.

# In[473]:


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

# In[474]:


df.describe(include='boolean') 


# * There's only 1 unique value (`True`), meaning every entry in this column is the same.
# * The `winner` column might be redundant since it does not provide any variation or distinguishing information. If the purpose is to flag winners, this column's structure suggests that all entries are winners, and it may need to be reassessed or filtered further for meaningful analysis.

# ## Handling missing values

# ### `img`

# In this feature we are working with URLs for images and, regardless if the URL is valid or accessible, this type of content might not provide direct insights for now, so this column is going to be dropped. 

# This is applied to a copy of the original Dataframe.

# In[475]:


df1 = df.copy()
df1 = df1.drop('img', axis=1)


# ### `nominee` column

# The nominee column only presents 6 null records, which gives us the possibility to further inspect them in detail.

# In[476]:


filtered_rows = df1[df1['nominee'].isnull()]
filtered_rows


# We note that the missing values correspond to different years but mostly to the same category (Remixer of the Year, Non-Classical), and four consecutive years (1997-2000). To preserve the records about these entries we can input values by reviewing online information in the official Grammys website. In the cases ("Best" and "of the Year") the artist is the nominee at the same time.

# In[477]:


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

# In[478]:


df1.loc[list(update.keys())]


# ### `artist` column

# In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time. However, it may not always be the case, so we are going to review this cases. 
# 

# ### Missing `artist` and `workers`

# We filter rows where `artist` and `workers` are null.

# In[479]:


# Filter rows where 'artist' and 'workers' are null
filtered_null_rows = df1[(df1['artist'].isnull()) & (df1['workers'].isnull())]

# Compute value counts for the 'category' column in these rows
category_counts = filtered_null_rows['category'].value_counts()

logging.info(category_counts)


# ### Value counts for the `category` column in these rows is one

# For the cases in which there are only one missing for category we are going to input data based on online information.

# In[480]:


#Filter categories where the count is 1
categories_with_one_count = category_counts[category_counts == 1].index

# Filter the original DataFrame using these categories
filtered_df = filtered_null_rows[filtered_null_rows['category'].isin(categories_with_one_count)]

# Display the filtered DataFrame
filtered_df


# We note that the missing values correspond to different years but mostly to the same category (Remixer of the Year, Non-Classical), and four consecutive years (1997-2000). To preserve the records about these entries we can input values by reviewing online information in the official Grammys website. In the cases ("Best" and "of the Year") the artist is the nominee at the same time.

# In[481]:


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

# In[482]:


df1.loc[list(update1.keys())]


# ### Value counts for the `category` column in these rows is more than one

# Of these categories with rows with missing `artist` and `workers` we need to filter those that do not contain a regular artist name to handle them separetely. In this case, the criteria is that they contain special characters in their `nominee` column. 

# In[483]:


# Filter rows where 'artist' and 'workers' are null
filtered_null_rows = df1[(df1['artist'].isnull()) & (df1['workers'].isnull())]

# Filter rows where 'nominee' contains special characters and 'artist' is null
filtered_rows = filtered_null_rows[(filtered_null_rows['nominee'].str.contains(r'[^\w\s]', regex=True))]

# Count the number of rows per 'category'
category_counts = filtered_rows['category'].value_counts()

# Display the counts
print(category_counts)


# As these columns may need reviewing and we operate on the assumption that those whitout special characters don't, we are going to input the `artist` value using the `nominee` value on the cases where `nominee` does not have special characters, and the `worker` is null.

# In[484]:


df1.loc[
    (df1["artist"].isnull()) & 
    (df1["workers"].isnull()) & 
    (~df1["nominee"].str.contains(r'[^\w\s]', regex=True)), 
    "artist"
] = df1["nominee"]


# ### Cases with special characters in `nominee`

# ##### Producer Of The Year (Non-Classical)

# In[485]:


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


# In[486]:


filter_rows_column_to_review(df1, "Producer Of The Year (Non-Classical)")


# It seems in this cases the special character is due to nominee being a duo, there is not major problem. When the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Producer Of The Year, Non-Classical".

# In[487]:


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


# In[488]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year (Non-Classical)")


# To review the changes we filter `category` for "Producer Of The Year (Non-Classical)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[489]:


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


# In[490]:


log_null_count(df1, "Producer Of The Year (Non-Classical)")


# ##### 

# #### Best Classical Vocal Soloist Performance

# In[491]:


filter_rows_column_to_review(df1, "Best Classical Vocal Soloist Performance")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[492]:


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


# In[493]:


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


# In[494]:


get_indices_list(df1, "Best Classical Vocal Soloist Performance")


# In[495]:


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

# In[496]:


log_null_count(df1, "Best Classical Vocal Soloist Performance")


# ##### 

# #### Best New Artist  

# In[497]:


filter_rows_column_to_review(df1, "Best New Artist")


# There does not appear to be anny issues, just `artist` that seem to be duos. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Best New Artist".

# In[498]:


df1 = assign_nominee_to_artist(df1, "Best New Artist")


# To review the changes we filter `category` for "Best New Artist" and check for nulls in the `artist` column. There are now none for this category.

# In[499]:


log_null_count(df1, "Best New Artist")


# #### Best Small Ensemble Performance (With Or Without Conductor) 

# In[500]:


filter_rows_column_to_review(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[501]:


get_indices_list(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# In[502]:


updates_best_small_essemble={2382:"Joseph Jennings (conductor) and Chanticleer", 
                             2475:"Steve Reich (conductor), Steve Reich and Musicians", 
                             2570:"Claudio Abbado (conductor), Berliner Philharmonic", 
                             2658:"Pierre Boulez (conductor) and the Ensemble Inter-Contemporain"

}

df1 = update_columns(df1, updates=updates_best_small_essemble)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[503]:


log_null_count(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# #### Best Classical Vocal Performance  

# In[504]:


filter_rows_column_to_review(df1, "Best Classical Vocal Performance")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[505]:


get_indices_list(df1, "Best Classical Vocal Performance")


# In[506]:


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

# In[507]:


log_null_count(df1, "Best Small Ensemble Performance (With Or Without Conductor)")


# #### Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)

# In[508]:


filter_rows_column_to_review(df1, "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[509]:


get_indices_list(df1, "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)")


# In[510]:


updates_best_soloist={3441: "Vladimir Horowitz", 
                    4312:"Cleveland Orchestra", 
                    4358: "Wendy Carlos", 
                    4446: "Vladimir Horowitz", 
                    4490: "Julian Bream"

}

df1 = update_columns(df1, updates=updates_best_soloist)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[511]:


log_null_count(df1, "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)")


# #### Most Promising New Classical Recording Artist

# In[512]:


filter_rows_column_to_review(df1, "Most Promising New Classical Recording Artist")


# It seems in this cases we see that the `nominee` is a record. We are going to input `artist` values based on online information.

# In[513]:


get_indices_list(df1, "Most Promising New Classical Recording Artist")


# In[514]:


updates_best_promising_classical={4540:"Peter Serkin", 
                      4586:"Marilyn Horne", 
                      4616:"André Watts"

}

df1 = update_columns(df1, updates=updates_best_promising_classical)


# To review the changes we filter `category` for "Best Small Ensemble Performance (With Or Without Conductor)" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[515]:


log_null_count(df1, "Most Promising New Classical Recording Artist")


# #### Producer Of The Year, Non-Classical

# In[516]:


filter_rows_column_to_review(df1, "Producer Of The Year, Non-Classical")


# There does not appear to be anny issues, just `artist` with dots or apostophres. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Best New Artist".

# In[517]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year, Non-Classical")


# To review the changes we filter `category` for "Producer Of The Year, Non-Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[518]:


log_null_count(df1, "Producer Of The Year, Non-Classical")


# #### Classical Producer Of The Year

# In[519]:


filter_rows_column_to_review(df1, "Classical Producer Of The Year")


# There does not appear to be anny issues, just and `artist` duo. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Classical Producer Of The Year".

# In[520]:


df1 = assign_nominee_to_artist(df1, "Classical Producer Of The Year")


# To review the changes we filter `category` for "Producer Of The Year, Non-Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[521]:


log_null_count(df1, "Classical Producer Of The Year")


# #### Producer Of The Year, Classical 

# In[522]:


filter_rows_column_to_review(df1, "Producer Of The Year, Classical")


# There does not appear to be anny issues, just and `artist` duo and an artist as a producer. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Producer Of The Year, Classical".

# In[523]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year, Classical")


# To review the changes we filter `category` for "Producer Of The Year, Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[524]:


log_null_count(df1, "Producer Of The Year, Classical")


# #### Producer Of The Year                                                                         

# In[525]:


filter_rows_column_to_review(df1, "Producer Of The Year")


# There does not appear to be anny issues, just and `artist` duo. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Producer Of The Year.

# In[526]:


df1 = assign_nominee_to_artist(df1, "Producer Of The Year")


# To review the changes we filter `category` for "Producer Of The Year" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[527]:


log_null_count(df1, "Producer Of The Year")


# #### Best New Artist Of The Year     

# In[528]:


filter_rows_column_to_review(df1, "Best New Artist Of The Year")


# There does not appear to be anny issues, just and `artist` duo. In the cases when the `category` contains "Best" or "of the Year" and "artist" the `artist` is the `nominee` at the same time.  We are going to apply this logic to input values for rows with the category "Best New Artist Of The Year".

# In[529]:


df1 = assign_nominee_to_artist(df1, "Best New Artist Of The Year")


# To review the changes we filter `category` for "Producer Of The Year, Classical" and check for nulls in the `artist` column. There are now none for this category.
# 

# In[530]:


log_null_count(df1, "Best New Artist Of The Year")


# ### Missing `artist`

# We still have to handle the cases where `artist` is null, `worker` is not.

# In[531]:


# Filter rows where 'artist' is null and 'workers' is not null
filtered_null_rows = df1[(df1['artist'].isnull()) & (~df1['workers'].isnull())]

# Count null values specifically in the 'artist' column
null_count_artist = filtered_null_rows['artist'].isnull().sum()
logging.info(f"There are {null_count_artist} null values in the 'artist' column.")


# We inspect the firts 30 unique values in workers to asses the type of information available.

# In[532]:


df["workers"].dropna().unique()[:30]


# There seem to be roles that may help us input values in `artist` when `worker` is not null. Arround XXX of the records have special characters with parentheses that may help with these.

# In[533]:


# Filter rows where 'artist' is null and 'workers' is not null
filtered_rows = df1[(df1["artist"].isnull()) & (~df1["workers"].isnull())]

# Count how many rows in 'workers' contain parentheses
count_parentheses = filtered_rows["workers"].str.contains(r'\(.*?\)', na=False).sum()

# Log the count
logging.info(f"There are {count_parentheses} rows in 'workers' with parentheses where 'artist' is null.")


# Arround XXX of the records have the word "featuring", information that may be util for inputing data in `artist`.

# In[534]:


# Count how many rows in 'workers' contain the word 'featuring'
count_featuring = filtered_rows["workers"].str.contains(r'featuring', case=False, na=False).sum()

# Log the count
logging.info(f"There are {count_featuring} rows in 'workers' with the word 'featuring' where 'artist' is null.")


# Now, we are going to tetect roles (e.g., artist, composer, soloist) that frequently appear in the workers column to design extraction rules later.

# In[535]:


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

# In[536]:


df1[df1["workers"].str.contains(r'\(.*?\)', na=False)][["nominee", "artist", "workers"]].head(5)


# In[537]:


df1[df1["workers"].str.contains(r';', na=False)][["nominee", "artist", "workers"]].head(5)


# Through this identified traits, we are going to define a funtion to input values in `artist` in the cases when `artist` is null but worker is not.

# In[538]:


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


# In[539]:


df1 = update_artist_from_workers_advanced(df1)


# We review the remaining rows where 'artist' column is null by filtering.

# In[540]:


# Filter rows where 'artist' column is null
null_rows_df = df1[df1["artist"].isnull()]
null_rows_df


# We manualy set the artist value for these cases.

# In[541]:


updates_best_chamber={402:"Attacca quartet", 
                      404:"Publiquartet", 

}

df1 = update_columns(df1, updates=updates_best_chamber)


# Finally we review if any row has null values.

# In[542]:


logging.info(df1.isnull().sum())


# ## Disposing of columns

# ### `published_at` and `updated_at` columns

# In the analysis of the dataset I could not apply a clear use of the values of these two columns. The dates have no relation with the date of the event, or with any ephemeris related to this event. Therefore, I am going to delete them from the dataframe.

# In[543]:


# Drop the specified columns
df1 = df1.drop(columns=['published_at', 'updated_at', 'workers_clean'])


# ## `category` normalization

# Using the Grammy's oficial category listing we are going to normalize `category` to math current award categories, since they have changed through the years.

# In[544]:


def normalize_category(category, reference_list):
    """
    Normalize a category to its closest match in a reference list.

    Args:
        category (str): The category to normalize.
        reference_list (list): List of Grammy categories for matching.

    Returns:
        str: The normalized category if a confident match is found, otherwise the original category.
    """
    try:
        # Check for exact matches (case-insensitive)
        if category.lower() in [ref.lower() for ref in reference_list]:
            logging.info(f"Exact match found for category: {category}")
            return category

        # Fuzzy matching to find the closest match
        result = process.extractOne(category, reference_list)
        match, score = result[0], result[1]

        # Only normalize if the similarity score is high (e.g., ≥ 95)
        if score >= 95:
            logging.info(f"Category: {category} | Closest match: {match} | Score: {score}")
            return match
        else:
            logging.warning(f"No suitable match found for category: {category} | Score: {score}")
            return category
    except Exception as e:
        logging.error(f"Error occurred while normalizing category: {category} | Error: {e}")
        return category


def process_dataframe(df, column_name, reference_list):
    """
    Process a DataFrame to normalize categories in a specified column.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        column_name (str): The name of the column containing categories to normalize.
        reference_list (list): A list of Grammy categories for matching.

    Returns:
        pd.DataFrame: A DataFrame with a new column containing normalized categories.
    """
    try:
        logging.info(f"Starting normalization for column: {column_name}")
        df[f'normalized_{column_name}'] = df[column_name].apply(
            lambda x: normalize_category(x, reference_list)
        )
        logging.info("Normalization complete.")
        return df
    except Exception as e:
        logging.error(f"Error occurred while processing DataFrame: {e}")
        return df


# In[545]:


###Grammy categories extracted from the PDF

grammy_categories = [
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
    'Children\'s Music Album', 'Comedy Album', 'Audio Book, Narration and Storytelling Recording', 
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

# Normalize categories in the DataFrame
df1 = process_dataframe(df1, 'category', grammy_categories)


# In[546]:


df1 = df1.drop(['category'], axis=1)


# ## Duplicates based on `year` and `category` 

# The feature `winner` does not provide any useful information regarding winners as all its values are `True`, so it is better to find if there are ay rows where `year` and `category` are repeated more than twice (as there can be two winners), to inspect or handle them as needed.
# 

# In[547]:


duplicates = (
    df1.groupby(['year', 'normalized_category'])
    .filter(lambda group: len(group) > 2)
    .sort_values(['year', 'normalized_category'])
)

# Log the output
logging.info(f"There are {len(duplicates)} rows where `year` and `category` are repeated more than twice.")


# In this cases we will leave only the first record for each duplicate group where year and category are repeated more than twice.

# In[552]:


# Drop duplicates based on 'year' and 'category', keeping only the first occurrence
df_cleaned = df1.drop_duplicates(subset=['year', 'normalized_category'], keep='first')


# Finally save a copy of the transformed dataset.

# In[553]:


# Save the DataFrame as a CSV file
df_cleaned.to_csv('../data/intermediate/grammys.csv', index=False)

logging.info("DataFrame has been saved to '../data/intermediate/grammys.csv'")


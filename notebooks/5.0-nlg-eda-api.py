#!/usr/bin/env python
# coding: utf-8

# # Workshop #2 :
# ## Reccobeats API - EDA
# 
# ------------------------------------------------------------
# 
# https://reccobeats.com/docs/apis/extract-audio-features

# In[2]:


import os
import sys
import pandas as pd
import logging


sys.path.append(os.path.abspath('../'))
from src.params import Params
from src.client import DatabaseClient
from src.logging_config import setup_logging


# In[3]:


setup_logging()


# In[ ]:


df = pd.read_csv("../data/external/reccobeats_features.csv")


# In[6]:


df.shape


# In[14]:


df.info()


# **acousticness** `float`
# 
# Acousticness refers to how much of a song or piece of music is made up of natural, organic sounds rather than synthetic or electronic elements. In other words, it's a measure of how "acoustic" a piece of music sounds. A confidence measure from 0.0 to 1.0, greater value represents higher confidence the track is acoustic.
# 
# 
# **danceability** `float`
# 
# Danceability is a measure of how suitable a song is for dancing, ranging from 0 to 1. A score of 0 means the song is not danceable at all, while a score of 1 indicates it is highly danceable. This score takes into account factors like tempo, rhythm, beat consistency, and energy, with higher scores indicating stronger, more rhythmically engaging tracks.
# 
# 
# **energy** `float`
# 
# Energy in music refers to the intensity and liveliness of a track, with a range from 0 to 1. A score of 0 indicates a very calm, relaxed, or low-energy song, while a score of 1 represents a high-energy, intense track. It’s influenced by elements like tempo, loudness, and the overall drive or excitement in the music.
# 
# 
# **instrumentalness** `float`
# 
# Predicts whether a track contains no vocals. “Ooh” and “aah” sounds are treated as instrumental in this context. Rap or spoken word tracks are clearly “vocal”. The closer the instrumentalness value is to 1.0, the greater likelihood the track contains no vocal content. Values above 0.5 are intended to represent instrumental tracks, but confidence is higher as the value approaches 1.0.
# 
# 
# **liveness** `float`
# 
# Detects the presence of an audience in the recording. Higher liveness values represent an increased probability that the track was performed live. A value above 0.8 provides strong likelihood that the track is live.
# 
# 
# **loudness** `float`
# 
# The overall loudness of a track in decibels (dB). Loudness values are averaged across the entire track and are useful for comparing relative loudness of tracks. Loudness is the quality of a sound that is the primary psychological correlate of physical strength (amplitude). Values typical range between -60 and 0 db.
# 
# 
# **speechiness** `float`
# 
# Speechiness detects the presence of spoken words in a track. The more exclusively speech-like the recording (e.g. talk show, audio book, poetry), the closer to 1.0 the attribute value. Values above 0.66 describe tracks that are probably made entirely of spoken words. Values between 0.33 and 0.66 describe tracks that may contain both music and speech, either in sections or layered, including such cases as rap music. Values below 0.33 most likely represent music and other non-speech-like tracks.
# 
# 
# **tempo** `float`
# 
# The overall estimated tempo of a track in beats per minute (BPM). Values typical range between 0 and 250
# 
# 
# **valence** `float`
# 
# Valence in music measures the emotional tone or mood of a track, with a range from 0 to 1. A score of 0 indicates a song with a more negative, sad, or dark feeling, while a score of 1 represents a more positive, happy, or uplifting mood. Tracks with a high valence tend to feel joyful or energetic, while those with a low valence may evoke feelings of melancholy or sadness.

# In[5]:


df.isnull().sum()


# In[9]:


# Find duplicated rows
duplicated_rows = df[df.duplicated(keep=False)]  # Keep all duplicates (including the first occurrence)

duplicated_rows


# In[10]:


df1 = df.drop_duplicates(keep='first')


# In[15]:


df1.describe()


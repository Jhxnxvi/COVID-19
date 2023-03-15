import requests
import pandas as pd
import numpy as np
import os
import sqlite3
from tqdm.auto import tqdm

OWNER = 'CSSEGISandData'
REPO = 'COVID-19'
PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'
print(f'Downloading paths from {URL}')

# Extract
response = requests.get(URL)
download_urls = [data['download_url'] for data in response.json() if data['name'].endswith('.csv')]

# Transform
# reading the CSV data
dfs = [pd.read_csv(url) for url in download_urls]
df = pd.concat(dfs, ignore_index=True)
# dropping rows with missing data and filtering to relevant columns
df = df.dropna(how='all').filter(['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered'])
# renaming columns
df = df.rename(columns={'Province_State': 'Province/State', 'Country_Region': 'Country/Region', 'Last_Update': 'Last Update', 'Confirmed': 'Confirmed Cases'})

print(df.head())


# Load
# creating a SQLite database and connect to it
conn = sqlite3.connect('covid19.db')
# putting the cleaned and transformed data to a new table in the database
df.to_sql('daily_reports', conn, if_exists='replace', index=False)
# commit the changes to the database and closing the connection
conn.commit()
conn.close()



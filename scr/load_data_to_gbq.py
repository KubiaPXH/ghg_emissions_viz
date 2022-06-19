# import libraries
import os
import sys

import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

# change working directory to this Python file directory
dir_path = os.path.dirname(__file__)
os.chdir(dir_path)

credentials_path = '../../GCP projects keys/vietnam-climate-change-bigquery-sa-key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client(project='vietnam-climate-change')

# config load table as replace but not append
job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

# push historical ghg emissions to Google BigQuery
table_id = 'ghg_emissions_viz.historical_ghg_emissions'
df = pd.read_csv("../data/processed/historical_ghg_emissions_processed.csv")
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

# push historical ghg emissions to Google BigQuery
table_id = 'ghg_emissions_viz.socioeconomics'
df = pd.read_csv("../data/processed/socioeconomics_processed.csv")
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

# job.result()

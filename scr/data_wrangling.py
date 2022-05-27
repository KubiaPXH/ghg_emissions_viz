# import libraries
import os
import sys

import pandas as pd
import numpy as np

# create mising table with total mising values and proportion of missing values
def create_missing_table(df):
    missing_data = df.isna().sum().sort_values(ascending=False)
    missing_data_percent = (df.isna().sum().sort_values(ascending=False)/len(df.index)).round(3)

    missing_data_table = pd.concat([missing_data, missing_data_percent],axis=1)
    missing_data_table.columns = ['Total missing values', 'Proportion of missing values']
    return missing_data_table

# change working directory to this Python file directory
dir_path = os.path.dirname(__file__)
os.chdir(dir_path)

emissions_df = pd.read_csv("../data/raw/CW_HistoricalEmissions_CAIT.csv")


print(emissions_df.info())

print(create_missing_table(emissions_df))

print(emissions_df["Country"].value_counts())
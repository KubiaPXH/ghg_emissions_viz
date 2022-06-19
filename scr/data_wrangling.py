# import libraries
import os
import sys

import pandas as pd
import numpy as np

# change working directory to this Python file directory
dir_path = os.path.dirname(__file__)
os.chdir(dir_path)

#region Processing historical emissions data

# import raw data
emissions_df = pd.read_csv("../data/raw/CW_HistoricalEmissions_CAIT.csv")

# delete column Source as all of its values are the same (i.e. "CAIT")
emissions_df.drop(columns="Source", inplace=True)

# trim values in string columns
emissions_df["Country"] = emissions_df["Country"].str.strip()
emissions_df["Sector"] = emissions_df["Sector"].str.strip()
emissions_df["Gas"] = emissions_df["Gas"].str.strip()

# delete EU and WORLD lines
emissions_df = emissions_df[(emissions_df['Country'] != 'EUU') & (emissions_df['Country'] != 'WORLD')]

# unpivot all the year columns to transform emissions_df from wide to long format 
id_vars_tuple = ("Country", "Gas", "Sector")
value_vars_array = np.arange(1990, 2020, dtype=int).astype(str)
emissions_df = pd.melt(emissions_df, id_vars=id_vars_tuple, value_vars=value_vars_array, var_name="Year", value_name="GHG Emissions")

# drop all "Total..." sectors since they are redundant values
emissions_df = emissions_df[(emissions_df['Sector'] != 'Total excluding LUCF') & (emissions_df['Sector'] != 'Total including LUCF')]

# create Agg Sector column with "Energy" and "Non-energy" value
sectors = emissions_df['Sector'].unique().tolist()
agg_sectors = ['Energy', 'Non-energy', 'Non-energy', 'Non-energy', 'Non-energy', 'Non-energy',
               'Energy', 'Energy', 'Energy', 'Energy', 'Energy', 'Energy']
map_agg_sectors = dict(zip(sectors, agg_sectors))

emissions_df['Agg Sector'] = emissions_df['Sector'].map(map_agg_sectors)
emissions_df.insert(2, 'Agg Sector', emissions_df.pop('Agg Sector'))

# Calculate the GHG Emissions of Energy Unspecified sector
# Energy Unspecified is the difference between Energy and sum of all Energy sub-sectors

sum_energy = emissions_df.groupby(['Country', 'Gas', 'Year', 'Agg Sector'])['GHG Emissions'].sum().reset_index()
## sum_energy = GHG emissions of Energy + sum(GHG emissions of all Energy sub-sector)
sum_energy = sum_energy[sum_energy['Agg Sector'] != 'Non-energy']
sum_energy.rename(columns={'Agg Sector':'Sector', 'GHG Emissions':'Sum Emissions'}, inplace=True)

emissions_df = emissions_df.merge(sum_energy, how='left', on=['Country', 'Gas', 'Year', 'Sector'])
## As Sum Emissions = Energy Emissions + sum(all Energy sub-sector emissions) (aka Sub-Energies Emissions)
## => Sub-Energies Emissions = Sum Emissions - Energy Emissions
emissions_df['Sub-Energies Emissions'] = np.where(emissions_df['Sum Emissions'].notna(), 
                                                  emissions_df['Sum Emissions'] - emissions_df['GHG Emissions'], 
                                                  np.nan)
## Energy unspecified Emissions = Energy Emissions - Sub-Energies Emissions
emissions_df['GHG Emissions'] = np.where(emissions_df['Sub-Energies Emissions'].isna(),
                                         emissions_df['GHG Emissions'],
                                         np.where(emissions_df['GHG Emissions'] - emissions_df['Sub-Energies Emissions'] > 0, 
                                                  emissions_df['GHG Emissions'] - emissions_df['Sub-Energies Emissions'], 
                                                  0)
                                        )

## drop redundant columns and change Energy value in Sector column to Energy Unspecified
emissions_df.drop(columns=['Sum Emissions', 'Sub-Energies Emissions'], inplace=True)
emissions_df['Sector'] = np.where(emissions_df['Sector'] == 'Energy', 'Energy Unspecified', emissions_df['Sector'])

# drop all "All GHG" sectors since they are redundant values
emissions_df = emissions_df[(emissions_df['Gas'] != 'All GHG')]

# change to proper data type
emissions_df = emissions_df.astype({'Country':'category', 'Gas':'category', 'Agg Sector':'category', 'Sector':'category', 'Year':'int32'})

# change columns name to we can upload it to Google BigQuery
emissions_df.columns = ['country', 'gas', 'agg_sector', 'sector', 'year', 'ghg_emissions']

# save processed data file to data/processed folder
emissions_df.to_csv("../data/processed/historical_ghg_emissions_processed.csv", index=False)

#endregion

#region Processing Socioeconomics data

# Since the structure of GDP table and Pop Table are really similar 
# we can create a function to do the process for both of them
def process_socioecon_data(df):
    # drop 'Unnamed: 0' and 'Indicator code' columns
    df.drop(columns=['Unnamed: 0'], inplace=True)
    ## get indicator to identify it is gdp_df or pop_df that we are working on
    indicator = df['Indicator Code'].unique()[0]
    df.drop(columns=['Indicator Code'], inplace=True)

    # drop 1960 -> 1989 year columns
    df.drop(df.columns[[x for x in range(2,2+1989-1960+1)]], axis=1, inplace=True)

    # melt 1990 -> 2019 year columns to turn table from wide to long form
    id_vars_tuple = ('Country Name', 'Country Code')
    value_vars_array = np.arange(1990, 2020, dtype=int).astype(str)
    df = pd.melt(df, id_vars=id_vars_tuple, 
                    value_vars=value_vars_array, 
                    var_name='Year', 
                    value_name='GDP' if indicator == 'NY.GDP.MKTP.CD' else 'Population')

    df = df[(df['Country Code'] != 'EUU') & (df['Country Code'] != 'WORLD')]

    df.loc[df['Country Code'] == 'PRK', 'Country Name'] = 'North Korea'

    return df

# import raw data
gdp_df = pd.read_csv("../data/raw/CW_gdp.csv")
pop_df = pd.read_csv("../data/raw/CW_population.csv")

# process GDP and Population data
gdp_df = process_socioecon_data(gdp_df)
pop_df = process_socioecon_data(pop_df)

# merge the 2 datasets together
socioecon_df = gdp_df.merge(pop_df, how='inner', on=['Country Name', 'Country Code', 'Year'])

# change to proper data type
socioecon_df = socioecon_df.astype({'Country Name':'category', 'Country Code':'category', 'Year':'int32'})

# change columns name to we can upload it to Google BigQuery
socioecon_df.columns = ['country_name', 'country_code', 'year', 'gdp', 'population']

# save processed data file to data/processed folder
socioecon_df.to_csv("../data/processed/socioeconomics_processed.csv", index=False)

#endregion
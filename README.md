## GHG Emissions Country Profile Visualization

In this project, we create a dashboard (also [published in Tableau Public](https://public.tableau.com/app/profile/xuan.huy.pham/viz/ghgemissionsviz/CountryGHGProfile)) to visualize the GHG emissions by country and its evolution by year. This project uses [the CAIT dataset from Climate Watch](https://www.climatewatchdata.org/data-explorer/historical-emissions). Data is cleaned and transformed using Python and the databoard is built using Tableau.

## Table of Contents
- [data](https://github.com/KubiaPXH/ghg_emissions_viz/tree/main/data)
  - [processed](https://github.com/KubiaPXH/ghg_emissions_viz/tree/main/data/processed): final dataset for modeling
  - [raw](https://github.com/KubiaPXH/ghg_emissions_viz/tree/main/data/raw): raw, original data
- [notebook](https://github.com/KubiaPXH/ghg_emissions_viz/blob/main/notebooks/data_wrangling.ipynb): jupiter notebook with draft code for data wrangling
- [scr](https://github.com/KubiaPXH/ghg_emissions_viz/tree/main/scr): source code for use in this project
  - data_wrangling.py: Python code nippet for data wrangling (cleaning and transforming)
  - load_data_to_gbq.py: Python code nippet for load processed data to Google BigQuery
  - ghg emissions viz.twb: Tableau file with dashboard using offline data sources

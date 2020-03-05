# importing all the dependencies

import pandas as pd

# %%

# convering pickle files with pandas dataframe
# setting the display coloums and importing the files

pd.set_option('display.max_columns', 6)
pd.set_option('display.width', 100)
as_Data = pd.read_pickle('AS_dataset.pkl')
probe_data = pd.read_pickle('probe_dataset.pkl')

# %%

# Analysis the as_data
# looking at had and tail of as_data
print(as_Data.head())
print(as_Data.tail())

# checking the columns and see any missing values
print(as_Data.columns)
print(as_Data.isna().sum())

# Analysing the probe_data
# looking at head and coloum of probe_data
print(probe_data.head())
print(probe_data.tail())

# checking the columns and see any missing values
print(probe_data.columns)
print(probe_data.isna().sum())

# %%

"""" Question 1: With the AS and probe data set, nd the number m of AS's that can be used for
hosting in the EU and have probes in the RIPE data set. Sort the ASN's in ascending
order and include the rst and last three in your report (number, name and country).

This question can be divided in different steps
step 1: first filter out all the ASN from the AS_data for europe region only based on the EU code given in 
Bright space and for the type 'hosting'.

Step 2: Collecting the prb_id from the probe Data sets which have the same ASN
for the European Region.
"""

""""step 1: first gilter out all the ASN from the AS_data for europe region only and check 
if the ASN are the unique"""
# Creating New data for only European countries - it is obtained from google
countryCodesEU = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
                  "FR", "GB", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV",
                  "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

# %%
# Creating new dataset for ony EU region from as_Data
as_data_europe_only = as_Data[as_Data.Country.isin(countryCodesEU)]
as_data_europe_hosting = as_data_europe_only.loc[as_data_europe_only['type'] == 'hosting']  # filtering for hosting
as_data_europe_hosting.type.unique()  # veryfing the ype is only hosting

# %%%
"""" Step 2: Collecting the prb_id from the prode Datasets which have the same ASN
for the European Region. """
# merging the ASN number to the count of probe_id for EU hosting
m_AS_EU_hosting = as_data_europe_hosting.merge(probe_data, on='ASN')
m_AS_EU_hosting.count()
m_AS_EU_hosting.to_csv('m_AS_EU_hosting.csv')  # saving it in form of CSV

# %%
# Alternative meathod to do the above and cross verify the count.
probe_data_europe_hosting = probe_data[probe_data.ASN.isin(as_data_europe_hosting.ASN)]
probe_data_europe_hosting.info()

# %%
# verifying the count is same. which shows both the meathods lead to the same way.
probe_data_europe_hosting.info()
# %%
# to print the unique top 5 and bottom 5 ASN, country, Name and prb_id
m_AS_EU_hosting_sorted = m_AS_EU_hosting.sort_values(by='ASN', ascending=True)  # sorting the dataframe
m_AS_EU_hosting_sorted.drop_duplicates(subset='ASN')[['Country', 'Name', 'ASN', 'prb_id']][:5]  # for top 5 values
m_AS_EU_hosting_sorted.drop_duplicates(subset='ASN')[['Country', 'Name', 'ASN', 'prb_id']][:-5]  # for keeeping the last 5

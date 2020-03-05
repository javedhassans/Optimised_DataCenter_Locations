#%%

# importing the required modules
import pickle
import pandas as pd
import os
import json

#%%
# printing checking the working path
print(os.getcwd())
print(os.listdir())

#%%

# setting the display coloums and importing the files
pd.set_option('display.max_columns', 6)
pd.set_option('display.width', 100)
#%%
as_Data = pd.read_pickle('AS_dataset.pkl')
probe_data = pd.read_pickle('probe_dataset.pkl')

#%%
# looking at had and tail of as_data
print(as_Data.head())
print(as_Data.tail())
#%%
# checking the columns and see any missing values
print(as_Data.columns)
print(as_Data.isna().sum())

#%%
# looking at head and coloum of probe_data
print(probe_data.head())
print(probe_data.tail())

#%%
# checking the columns and see any missing values
print(probe_data.columns)
print(probe_data.isna().sum())

#%%

# opning the ip2location-lite file
import pandas as pd
ip2location_lite = pd.read_csv('IP2LOCATION-LITE-DB1.CSV',sep=',')
#%%
print(ip2location_lite.head())
print(ip2location_lite.tail())

#%%
ip2location_lite.info()



#%%
# file = open('ping-2020-02-20T0000','r', encoding="cp1252")
# pd.read_table(file)

decomFilename = 'ping-2020-02-20T0000'
decomFile     = open(decomFilename, 'rt')

#read first line and print
firstLine = decomFile.readline();
print(firstLine)

#the line appears to be json-formatted: pretty print json
firstLineJson = json.loads(firstLine)
print(json.dumps(firstLineJson, sort_keys=True, indent=4))



#%%
european_list_codes = ['BE', 'BG', 'CZ' ,'DK' , 'DE', 'EE','IE',
                       'ES','FR', 'HR','IT','CY','LV','LT','LU','HU',
                       'MT','NL','AT','PL','PT', 'RO','SI','SK','FI',
                       'SE','GB']


#%%
as_Data.Country.unique()
as_Data.isna().sum()
#%%
for i in range(len(european_list_codes)):
    # print(i)

#%%
if  'BE' in as_Data.Country:
    print('hay man')

#%%
for i in european_list_codes:
    if i in as_Data.Country:
        print(as_Data.ASN)

#%%
import matplotlib.pyplot as plt
as_Data.groupby(by='Country')['ASN'].count()

#%%
for i in european_list_codes:
    if as_Data.loc[as_Data['Country'] == i.any()]:
        print(as_Data['ASN'])

#%%%
as_Data.info()
probe_data.info()

#%%
european_list_codes
as_data_europe_only = as_Data[as_Data.Country.isin(european_list_codes)]

as_data_europe_only.info()
#%%
set1 = set(as_data_europe_only.Country.unique())
set2 = set(european_list_codes)

missing = list(sorted(set1 - set2))
print(missing)

#%%
print(set1, end='')
#%%
print(set2,end='')

#%%
as_Data[as_Data['Country'] == 'HU']

#%%
european_list_codes.remove('EL')
#%%
print(european_list_codes)
#%%
european_list_codes1 = {'BE', 'BG', 'CZ' ,'DK' , 'DE', 'EE','IE',
                       'ES','FR', 'HR','IT','CY','LV','LT','LU','HU',
                       'MT','NL','AT','PL','PT', 'RO','SI','SK','FI',
                       'SE','GB'}
#%%
list(sorted(european_list_codes1))

#%%

def initial_check(filename):
    filename = input('enter the filename')
    print(filename.head())
    print(filename.tail())
    print(as_Data.columns)
    print(as_Data.isna().sum())

initial_check()


#%%%
set_asn_europe = set(as_data_europe_only.ASN)
len(set_asn_europe)
#%%
as_data_europe_only.info()
len(as_data_europe_only.ASN.unique())

#%%%
prb_id_europe_only = probe_data[probe_data.ASN.isin(set_asn_europe)]

#%%%
prb_id_europe_only

#%%
# import pandas as pd

import csv
import sys
import os
import random
import sys


import sqlite3
df = pd.read_csv('IP2LOCATION-LITE-DB1.CSV',header=0)
df.head()
df.tail()
df.columns = ['ip_from', 'ip_to', 'country_code', 'country_name']
df.tail()
df.head()

#%%%
print(european_list_codes1, end='')
iplocations_europe = df[df.country_code.isin(european_list_codes1)]
iplocations_europe

#%%
""""Step 3 """


""""If we want to find what are locations are ip address from iplocation file"""

countryCodesEU = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
"FR", "GB", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV",
"MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

df = pd.read_csv('IP2LOCATION-LITE-DB1.CSV',header=0) #connecting the iplocation file
# print(countryCodesEU, end='') # checcking european codes
df.columns = ['ip_from', 'ip_to', 'country_code', 'country_name'] # renaming the colums based on the webpage of iplocation
iplocations_europe = df[df.country_code.isin(countryCodesEU)] # creatign dataframe containing only ipaddresses


#%%%

iplocations_europe.info()

#%%
iplocations_europe[iplocations_europe['ip_from'] != iplocations_europe['ip_to']]

#%%
iplocations_europe['ip_to'] - iplocations_europe['ip_from']










# %% OPTION 2:
# Libraries
import re
import time
import bz2
import os
import sys
import json
import time
import pickle
import pandas as pd

#%%


## Functions

def binarySearch(target_ip, ip_locations):
    low = 0
    high = len(ip_locations) - 1
    iterations = 0
    Country = "NA"
    while low <= high:
        middle = int((low + high) / 2)
        # check if we found it
        if target_ip >= ip_locations.IPmin[middle] and target_ip <= ip_locations.IPmax[middle]:
            Country = ip_locations.Countrycode[middle]
            break
        # check which side
        elif target_ip < ip_locations.IPmin[middle]:
            iterations += 1
            high = middle - 1
        else:
            iterations += 1
            low = middle + 1
    return (Country)

#%%


# # # Reading data

prb_ID_EU = pd.read_csv("m_AS_EU_hosting.csv")

header_names = ['IPmin', 'IPmax', 'Countrycode', 'Countryname']
ip_locations = pd.read_csv('IP2LOCATION-LITE-DB1.CSV', header=0, names=header_names)
print(ip_locations.head())

#%%

# european_list_codes1 = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
#                         "FR", "GB", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV",
#                         "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]
# ip_locations_europe_only = ip_locations[ip_locations.Countrycode.isin(european_list_codes1)]
# print(ip_locations_europe_only.head())

#%%
st = time.time()
bz2Filename = 'ping-2020-02-20T0000.bz2'
bz2File = bz2.open(bz2Filename, 'rt')
st = time.time()
ipv4 = []
count = 0
prb_id_list = list(prb_ID_EU["prb_id"])
st = time.time()
for line in bz2File:
    count = count + 1
    if count == 1000:
        break
    json_file = json.loads(line)
    ipv4.append(json_file)
    if "af" not in json_file.keys(): continue
    if "dst_addr" not in json_file.keys(): continue
    if json_file["af"] != 4: continue
    prb_id = json_file["prb_id"]

    #  # Find the prb_id in in the prb_ID_EU dataset
    if prb_id in prb_id_list and json_file["af"] == 4 : # added the filter to ensure only ipv4 are taken
        to_ip = json_file["dst_addr"]
        result_array = json_file["result"]
        #  # Find the ip_destination_address in the ip_location files
        target_ip = int(re.sub(r'\.', '', to_ip))
        country = binarySearch(target_ip, ip_locations)
        print(str(country) + "-" + str(count))
print(count)
bz2File.close()
dur = round(time.time() - st, 3)
print("processing lines: " + str(dur) + " seconds")


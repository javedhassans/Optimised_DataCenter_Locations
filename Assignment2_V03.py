# %% OPTION 2:
# Libraries

# %%
# importing the depending modules
import re
import time
import bz2
import os
import sys
import json
import time
import pickle
import pandas as pd
import sys
import lognsearch


# %%

# Functions to search country code with iplocations

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


# %%

# Reading data from the question1

prb_ID_EU = pd.read_csv("m_AS_EU_hosting.csv")

header_names = ['IPmin', 'IPmax', 'Countrycode', 'Countryname']
ip_locations = pd.read_csv('IP2LOCATION-LITE-DB1.CSV', header=0, names=header_names)
# print(ip_locations.head())

# %%

# creating the ist of european countries
european_list_codes1 = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
                        "FR", "GB", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV",
                        "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]


# ip_locations_europe_only = ip_locations[ip_locations.Countrycode.isin(european_list_codes1)]
# print(ip_locations_europe_only.head())

# %%

# check the names of the files in the folder
print(os.getcwd())
print(os.listdir())

#%%

# this function checks if the file already exists
# if this is not checked the file will keep on appeding duplicates
# Note: do not forget to enter with .txt
def checkfileexists():
    """"This function checks if the file exists, if yes it deltes it"""
    filename = input('enter the name of the file you want to check')
    try:
        os.remove(str(filename))
        print("file exists deleting the file")
    except IOError:
        print("no file")


checkfileexists()

#%%

# counting total number of lines
fname = "ping-2020-02-20T0000.bz2"
bz2File = bz2.open(bz2Filename, 'rt')
count_nrlines = 0
for line in bz2File:
    count_nrlines += 1
print("Total number of lines is:", count_nrlines)

# %% OPTION 2:
# #open .bz2 file directly
## todo every time when we run new file change the name of the bzfile
filename = input("enter the filename you want to work with")
print("name of file you entered is", filename)
bz2Filename = str(filename)

# this will create variable output filenae for saving the output
outputfile = bz2Filename+'output'+'.json'

bz2File = bz2.open(bz2Filename, 'rt')
# # read first line and print
# creating prbe_id list of EU countries and are hosting
prb_id_list = list(prb_ID_EU["prb_id"])


# read first 100k lines to estimate total loading time
count = 0;
st = time.time()
for line in bz2File:
    count = count + 1
    if count > 1000000 : #int(nrOfLines) #todo int(nrOfLines) I am not sure baout how to check the estimated time
        break
    json_file = json.loads(line)
    if "af" not in json_file.keys(): continue
    if "dst_addr" not in json_file.keys(): continue
    if json_file["af"] != 4: continue
    prb_id = json_file["prb_id"]

    #  # Find the prb_id in in the prb_ID_EU dataset
    # added the filter to ensure only ipv4 are taken
    if prb_id in prb_id_list and json_file["af"] == 4:
        # # crearting variable names to use in writing file
        to_ip = json_file["dst_addr"]
        result_array = json_file["result"]
        avg_latency = json_file["avg"]
        #  # Find the ip_destination_address in the ip_location files
        target_ip = int(re.sub(r'\.', '', to_ip))
        country = binarySearch(target_ip, ip_locations)
        # # storing the output of the file in jason file
        data = dict(country_code=country, prb_id=prb_id, avg_latency=avg_latency)
        with open(outputfile, 'a+') as outfile:
            json.dump(data, outfile)
            outfile.write('\n')

# finally close bz2File
bz2File.close()
end_time = time.time()
print("estimeated time for execution is ", end_time - st)

# finally close bz2File
bz2File.close()

#%%

## after running the file it filters only for EU countries
import json
# modifing file name
newfile = filename + 'only_eu'+'.json'
print(newfile)
prb_id_Eu_host = list(prb_ID_EU.prb_id.unique())
my_file = open(outputfile,'rt')
    for line in my_file:
        file = json.loads(line)
        # print(file)
        file["country_code"]
        file["prb_id"]
        file["avg_latency"]
        if file["country_code"] in european_list_codes1:
            # if file["prb_id"] in prb_id_Eu_host:
            data = dict(country_code=file["country_code"], prb_id=file["prb_id"], avg_latency=file["avg_latency"])
            with open(newfile, 'a+') as outfile:
                json.dump(data, outfile)
                outfile.write('\n')

my_file.close()

#%%

### not required as it is duplicate
# ## after running the file it filters only for probe_id
# import json
# newfile_01 = filename + 'euprb'+'.json'
# prb_id_Eu_host = list(prb_ID_EU.prb_id.unique())
# my_file = open(newfile,'rt')
#     for line in my_file:
#         file = json.loads(line)
#         # print(file)
#         file["country_code"]
#         file["prb_id"]
#         file["avg_latency"]
#         if file["prb_id"] in prb_id_Eu_host:
#             data = dict(country_code=file["country_code"], prb_id=file["prb_id"], avg_latency=file["avg_latency"])
#             with open(newfile_01, 'a+') as outfile:
#                 json.dump(data, outfile)
#                 outfile.write('\n')
#
# my_file.close()























#%%

df = pd.read_csv('ping-2020-02-20T0000.bz2output.txt',sep=',',names=['ccode','prb_id','avg_latency'])
df = df.dropna()
#%%
df_Eu = df[df.ccode.isin(prb_ID_EU.Country)]
df_Eu


#%%
# prb_ID_EU[prb_ID_EU.prb_id == 6310]

df_Eu[df_Eu.prb_id.isin(prb_ID_EU.prb_id)]




# %%
prb_id_Eu_host = prb_ID_EU.prb_id.unique()
prb_id_Eu_host

#%%
st = time.time()
bz2Filename = "ping-2020-02-20T0000.bz2"
# writing command that will close file once the clock of code is complete
# with open(bz2Filename,'rt') as bz2File:
#     # TODO
bz2File = bz2.open(bz2Filename, 'rt')
# st = time.time()
ipv4 = []
count = 0
prb_id_list = list(prb_ID_EU["prb_id"])
# st = time.time()
for line in bz2File:
    count = count + 1
    if count > 100000:  # orginal count is 18374119
        break
    json_file = json.loads(line)
    # print(json_file)
    ipv4.append(json_file)  # TODO I think this variable is useless
    if "af" not in json_file.keys(): continue
    if "dst_addr" not in json_file.keys(): continue
    if json_file["af"] != 4: continue
    prb_id = json_file["prb_id"]

    #  # Find the prb_id in in the prb_ID_EU dataset
    if prb_id in prb_id_list and json_file["af"] == 4:  # added the filter to ensure only ipv4 are taken
        to_ip = json_file["dst_addr"]
        # print(to_ip)
        result_array = json_file["result"]
        # print(result_array)
        avg_latency = json_file["avg"]
        # print(avg_latency)
        #  # Find the ip_destination_address in the ip_location files
        target_ip = int(re.sub(r'\.', '', to_ip))
        country = binarySearch(target_ip, ip_locations)
        data = dict(country_code=country, prb_id=prb_id, avg_latency=avg_latency)
        with open('data_03.json', 'a+') as outfile:
            json.dump(data, outfile)
            outfile.write('\n')
            # outfile
        # orig_stdout = sys.stdout
        # f = open('data_01.txt','a+')
        # sys.stdout = f
        # print(str(country) + "," + str(prb_id) + "," + str(avg_latency))
        # # file.close()
        # sys.stdout = orig_stdout
        # f.close()
bz2File.close()
end_time = time.time()
print("estimeated time for execution is ", end_time - st)


#%%



#%%






#%%

prb_ID_EU[prb_ID_EU["prb_id"].isin([21842, 6310,6667,1000024,6180])]

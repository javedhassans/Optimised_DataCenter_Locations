
# %%
# importing the depending modules
import re
import bz2
import json
import time
import pickle
import pandas as pd
import sys
import lognsearch

# %%
# creating the ist of european countries
european_list_codes1 = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
                        "FR", "GB", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV",
                        "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]
# %%
# readign ipfile and puting headers as required
header_names = ['IPmin', 'IPmax', 'Countrycode', 'Countryname']
ip_locations = pd.read_csv('IP2LOCATION-LITE-DB1.CSV', header=0, names=header_names)

# creating prbe_id list of EU countries and are hosting
prb_ID_EU = pd.read_csv("m_AS_EU_hosting.csv")
prb_id_list = list(prb_ID_EU["prb_id"])


# %%

#  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # in this part you can run for more than one ping file, keep in mind the time per file 
#  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
starttime = time.time()
listbz2files = ["ping-2020-02-20T0600.bz2", "ping-2020-02-20T0700.bz2", "ping-2020-02-20T0800.bz2", "ping-2020-02-20T0900.bz2", "ping-2020-02-20T1000.bz2","ping-2020-02-20T1100.bz2"]

for bz2Iteration in range(len(listbz2files)):
    # print(bz2Iteration)
    # st = time.time()
    list_latency = []
    bz2Filename = listbz2files[bz2Iteration]
    # print(bz2Filename)
    newfile = bz2Filename + 'only_eu' + '.json'
    print("starting with" + bz2Filename)
    # this will create variable output filename for saving the output
    outputfile = bz2Filename + 'output' + '.json'
    # opening the file with the filname entered by the user in above
    bz2File = bz2.open(bz2Filename, 'rt')

    # runing the main chuck
    count = 0;
    for line in bz2File:
        count = count + 1
        if count > 1000000:  # #todo this controls numner of lines you want to run or check
            break
        json_file = json.loads(line)
        if "af" not in json_file.keys(): continue
        if "dst_addr" not in json_file.keys(): continue
        # if json_file["af"] != 4: continue # todo not sure we need this or not
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
            country = lognsearch.binarySearch(target_ip, ip_locations)
            # # storing the output of the file in jason file
            data = dict(country_code=country, prb_id=prb_id, avg_latency=avg_latency)
            if data["country_code"] in european_list_codes1:
                list_latency.append(data)
                with open(newfile, 'a+') as outfile:
                    json.dump(data, outfile)
                    outfile.write('\n')
    bz2File.close()
    # # End of loop for binarysearch 

    # # convert the list in data frame and save it 
    # # save for every hour file with a different name 

    namefile = "df_rawLatency_" + str(bz2Iteration) + ".pkl"
    df_rawLatency = pd.DataFrame.from_dict(list_latency)
    df_rawLatency.to_pickle(namefile)
    # # clean the workspace
    del df_rawLatency

endtime = time.time()

print("Total time to run the loop is ", endtime - starttime )

# # End of main loop
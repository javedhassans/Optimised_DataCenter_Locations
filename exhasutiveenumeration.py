# %%
# # importing the dependecies
import pandas as pd
from numpy import int64
import math

# %%
# # creting the variable for later use
prb_ID_EU = pd.read_csv("m_AS_EU_hosting.csv")
prb_id_list = list(prb_ID_EU["prb_id"])

# %%

#  # Read all of the files processed and concatente in only one object --> df_rawLatency. THe numner of rows is equal to the total of rows of all files you processed
df_rawLatency_6 = pd.read_pickle("df_rawLatency_0.pkl")  # todo: everyone has to put their own files
df_rawLatency_7 = pd.read_pickle("df_rawLatency_1.pkl")  # todo: everyone has to put their own files
df_rawLatency_8 = pd.read_pickle("df_rawLatency_2.pkl")  # todo: everyone has to put their own files
df_rawLatency_9 = pd.read_pickle("df_rawLatency_3.pkl")  # todo: everyone has to put their own files
df_rawLatency_10 = pd.read_pickle("df_rawLatency_4.pkl")  # todo: everyone has to put their own files
df_rawLatency_11 = pd.read_pickle("df_rawLatency_5.pkl")  # todo: everyone has to put their own files

df_rawLatency = df_rawLatency_6.append(df_rawLatency_7)
df_rawLatency = df_rawLatency.append(df_rawLatency_8)
df_rawLatency = df_rawLatency.append(df_rawLatency_9)
df_rawLatency = df_rawLatency.append(df_rawLatency_10)
df_rawLatency = df_rawLatency.append(df_rawLatency_11)
df_rawLatency.shape
df_rawLatency.head()

# %%

# # Select from the entire file only average latency greater than zero
df_rawLatency = df_rawLatency[df_rawLatency["avg_latency"] > 0]

## renaming the coloumbs so we can convert the colum to init64 type later used for merging
df_rawLatency.columns = ["country_code_target", "prb_id_new", "avg_latency"]

# %%
# # casting the oloum type of prb_id to init64
df_rawLatency["prb_id"] = df_rawLatency["prb_id_new"].astype(int64)
# # droping the colum which is not required
df_rawLatency = df_rawLatency.drop('prb_id_new', axis=1)

# %%
# # Merging with the ASN file returned in Question  A
df_ASN_Country = prb_ID_EU.merge(df_rawLatency)


# %%
# # Calculate the mean for each ASN and Country combination and caluclating the mean
df_ASN_Country_Average = df_ASN_Country.groupby(['ASN', "Country"])['avg_latency'].mean().reset_index()


# %%
# # Reshaping the data frame in matrix n (countries) X m (ASN)
df_ASN_Country_Average = df_ASN_Country_Average.pivot(index='Country', columns='ASN', values='avg_latency')

# %%

#  Exhaustive enumeration for the entire matrix

import math

nASN = df_ASN_Country_Average.shape[1]

optimal_ASN = math.inf
for ii in range(nASN):
    for jj in range(ii + 1, nASN):
        for kk in range(jj + 1, nASN):
            for ll in range(kk + 1, nASN):
                latency_step = df_ASN_Country_Average.iloc[:, [ii, jj, kk, ll]].min(1).sum()
                if (latency_step < optimal_ASN):
                    optimal_ASN = latency_step
                    optimal_combination = [ii, jj, kk, ll]
print("Optimal ASN" + df_ASN_Country_Average.columns[optimal_combination])

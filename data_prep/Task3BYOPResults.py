#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 10:57:06 2023

@author: dfox
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import random
import pickle

pd.set_option("display.max_columns", 15)
random.seed(17)

K = 5

centroids_filename = "centroids_eii.csv"
clusters_filename = "clusters_eii.csv"

cab_columns = [ 'passenger_count', # 0
                'trip_distance', # 1
                'pickup_longitude', # 2
                'pickup_latitude', # 3
                'dropoff_longitude', # 4
                'dropoff_latitude', # 5
                'tolls_amount', # 6
                'total_amount', # 7
                'dayofweek', # 8
                'lighthour', # 9
                'duration', # 10
                'credit_card', # 11 
                'cash', # 12
                'tip_frac' # 13 
]

cab_cols_w_ids = cab_columns.copy() 
cab_cols_w_ids.insert(0, 'Centroid_Id')

print(cab_cols_w_ids)
df_cl = pd.read_csv(clusters_filename, index_col=False, names=cab_cols_w_ids)
print(df_cl)
df_ce_id = df_cl['Centroid_Id']
df_clx = df_cl[cab_columns]
print(df_clx.head())

with open("Task3BYOD_scaler", "rb") as f:
    scaler = pickle.load(f)
df_clx = pd.DataFrame(scaler.inverse_transform(df_clx), columns=cab_columns)
print(df_clx.head())

df_res = pd.concat([df_ce_id, df_clx], axis=1)
print(df_res.head())

df_clusters = [df_res[df_res.Centroid_Id==i] for i in range(K)]

for col in cab_columns:
    print("")
    print(col)
    for i in range(K):
        print(f"centroid {i} {df_clusters[i][col].median()}:")

df_res.to_csv("byod_results.csv", index=False, index_label=False )

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 21 13:28:31 2023

@author: dfox
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import random
import pickle

random.seed(17)

K = 5

input_filename = "yellow_tripdata_2016-03.csv"

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

df = pd.read_csv(input_filename, index_col=False)
df = df.dropna()
# dataset is too big for mapreduce to run timely
df = df.sample(frac=0.1)

df['pickup_dt'] = pd.to_datetime(df.tpep_pickup_datetime)
df['dropoff_dt'] = pd.to_datetime(df.tpep_dropoff_datetime)
df['duration'] = (df.dropoff_dt.sub(df.pickup_dt)).view('int64') // 10**9
df['dayofweek'] = df.pickup_dt.dt.dayofweek # Mon (0) - Sun (6)
df['hour'] = df.pickup_dt.dt.hour
df['lighthour'] = np.where(df.hour >= 7, df.hour - 7, df.hour + 17)
df['credit_card'] = (df.payment_type == 1)
df['credit_card'] = df['credit_card'].astype(int)
df['cash'] = (df.payment_type == 2)
df['cash'] = df['cash'].astype(int)
tips_df = df[df.credit_card == 1]
#tips_frac = (tips_df['tip_amount'].sum()
#             / tips_df[tips_df['tip_amount'] != 0].shape[0])
df['tip_frac'] = df['tip_amount'] / df['total_amount']

# Row management

df = df.drop(['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'RatecodeID',
              'VendorID', 'store_and_fwd_flag', 'mta_tax',
              'tip_amount', 'improvement_surcharge', 'extra', 'fare_amount',
              'pickup_dt', 'dropoff_dt', 'hour'], axis=1)

# # outlier detection

# outlier_high = {}
# outlier_low = {}

# for col in df:
#     outlier_high[col] = df[col].mean() + df[col].std()*3
#     outlier_low[col] = df[col].mean() - df[col].std()*3
#     print(col, outlier_high[col], outlier_low[col])

xdf = df[df.passenger_count != 0]
xdf = xdf[xdf.trip_distance > 0]
xdf = xdf[xdf.trip_distance < 50]
xdf = xdf[xdf.pickup_longitude > -75]
xdf = xdf[xdf.pickup_longitude < -73]
xdf = xdf[xdf.pickup_latitude < 41]
xdf = xdf[xdf.pickup_latitude > 40]
xdf = xdf[xdf.tolls_amount < 20]
xdf = xdf[xdf.tolls_amount >= 0.0]
xdf = xdf[xdf.total_amount < 300]
xdf = xdf[xdf.total_amount > 0.0]
xdf = xdf[xdf.duration < 20000]
xdf = xdf[xdf.duration > 0]
df = xdf.reset_index(drop=True)

# blank_df = pd.DataFrame(columns=cab_columns)

# blank_df.to_csv("Task2BYOD_cab_column_names.csv", index=False, index_label=False)

df = df[cab_columns]
print(df.head())

df.to_csv("Task3BYOD_cab_base_10.csv", header=True, index_label='ID')

scaler = MinMaxScaler()
df_norm = pd.DataFrame(scaler.fit_transform(df.to_numpy()), columns=cab_columns)

df_norm.to_csv("Task3BYOD_cab_normMM_10.csv", header=False, index=False, index_label=False)

centroids = pd.DataFrame(np.random.uniform(0, 1, size=(K, len(cab_columns))), 
                         columns=cab_columns)

centroids.to_csv("Task3BYOD_centroids.csv", header=False, index_label='ID')

with open("Task3BYOD_scaler", "wb") as f:
    pickle.dump(scaler, f)
scaler = None

with open("Task3BYOD_scaler", "rb") as f:
    scaler = pickle.load(f)
df2 = pd.DataFrame(scaler.inverse_transform(df_norm))
print(df2.head())


                                                           
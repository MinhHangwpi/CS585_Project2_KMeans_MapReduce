#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 21 13:28:31 2023

@author: dfox
"""
import pandas as pd
import numpy as np

input_filename = "yellow_tripdata_2016-03.csv"

df = pd.read_csv(input_filename, index_col=False)
df = df.dropna()

# Row management
xdf = df[df.pickup_longitude != 0]
xdf = xdf[xdf.pickup_latitude != 0]
xdf = xdf[xdf.dropoff_longitude != 0]
xdf = xdf[xdf.dropoff_latitude != 0]
xdf = xdf[xdf.total_amount != 0]
xdf = xdf[xdf.passenger_count != 0]
xdf = xdf[xdf.trip_distance != 0]
df = xdf.reset_index(drop=True)


# Column management
# drop VendorID
df['pickup_dt'] = pd.to_datetime(df.tpep_pickup_datetime)
df['dropoff_dt'] = pd.to_datetime(df.tpep_dropoff_datetime)
df['dayofweek'] = df.pickup_dt.dt.dayofweek # Mon (0) - Sun (6)
df['hour'] = df.pickup_dt.dt.hour
df['lighthour'] = np.where(df.hour >= 7, df.hour - 7, df.hour + 17)
df['duration'] = (df.dropoff_dt.sub(df.pickup_dt)).view('int64') // 10**9
# keep df.passenger_count
# drop df.passenger_count = 0
# keep df.trip_distance
# drop df.trip_distance=0
# keep pickup_longitude
# keep pickup_lattitude
# keep dropoff_longitude
# keep dropoff_lattitude
# drop pickup_longitude = 0
# drop pickup_lattitude = 0
# drop dropoff_longitude = 0
# drop dropoff_lattitude = 0
# drop total_amount = 0
# drop RateCodeID
# drop store_and_fwd_flag
df['credit_card'] = (df.payment_type == 1)
df['credit_card'] = df['credit_card'].astype(int)
df['cash'] = (df.payment_type == 2)
df['cash'] = df['cash'].astype(int)
tips_df = df[df.credit_card == 1]
tips_frac = (tips_df['tip_amount'].sum()
             / tips_df[tips_df['tip_amount'] != 0].shape[0])

# one-hot encode payment_type = Credit (1/0), Cash (1/0) (all else 0's)
# calculate tip fraction when credit=1 (tip_amount / total_amount), encode
#   as mean otherwise?
# keep tolls_amount
# drop mta_tax, tip_amount, improvement_surcharge, extra, fare_amount
# drop temp variables: pickup_dt, dropoff_dt

df = df.drop(['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'RatecodeID',
              'VendorID', 'store_and_fwd_flag', 'mta_tax',
              'tip_amount', 'improvement_surcharge', 'extra', 'fare_amount',
              'pickup_dt', 'dropoff_dt', 'hour'], axis=1)

df.to_csv("Task3BYOD_cab.csv", header=False, index=False, index_label=False)

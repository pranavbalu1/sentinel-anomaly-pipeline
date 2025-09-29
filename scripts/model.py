import pyarrow.dataset as ds
import pyarrow.fs as fs
import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
from config import BATCH_SIZE, CONTAMINATION, RANDOM_STATE, EPS, MIN_SAMPLES
from dotenv import load_dotenv
from datetime import datetime
import hdbscan

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="sklearn")

load_dotenv()

def read_parquet_in_batches(bucket_name, prefix, batch_size=BATCH_SIZE):
    s3_path = f"{bucket_name}/{prefix}"
    print(f"Reading dataset from: {s3_path}")

    s3 = fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_DEFAULT_REGION") or "us-east-2"
    )

    dataset = ds.dataset(s3_path, format="parquet", filesystem=s3)

    for batch in dataset.to_batches(batch_size=batch_size):
        df = batch.to_pandas()
        df = df[(df['B11'] != 0) & (df['B12'] != 0)]
        yield df


def preprocess(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    if df.empty:
        return df

    df['date_ordinal'] = df['date'].astype('int64') / 1e9 / 86400
    df['B_diff'] = df['B11'] - df['B12']
    features = ['B11', 'B12', 'B_diff', 'latitude', 'longitude', 'date_ordinal']

    scaler = StandardScaler()
    df[[f"{f}_scaled" for f in features]] = scaler.fit_transform(df[features])

    return df


def detect_anomalies(df): 
    features = ['B11_scaled', 'B12_scaled', 'B_diff_scaled',
                'latitude_scaled', 'longitude_scaled', 'date_ordinal_scaled']
    model = IsolationForest(contamination=CONTAMINATION, random_state=RANDOM_STATE)
    df['anomaly'] = model.fit_predict(df[features])
    anomalies = df[df['anomaly'] == -1].copy()
    print(f"⚠️ Detected {len(anomalies)} anomalies in this batch")
    return anomalies


def cluster_anomalies(anomalies):
    if anomalies.empty:
        return anomalies

    # Better to use scaled values for clustering
    X = anomalies[['latitude_scaled', 'longitude_scaled', 'date_ordinal_scaled']].to_numpy()

    # HDBSCAN does not use eps; instead you control it with min_cluster_size
    clusterer = hdbscan.HDBSCAN(min_cluster_size=MIN_SAMPLES, metric='euclidean')
    anomalies['plume_cluster'] = clusterer.fit_predict(X)

    summary = anomalies.groupby('plume_cluster').agg(
        count=('latitude', 'size'),
        min_latitude=('latitude', 'min'),
        max_latitude=('latitude', 'max'),
        min_longitude=('longitude', 'min'),
        max_longitude=('longitude', 'max'),
        min_date=('date', 'min'),
        max_date=('date', 'max')
    ).reset_index()
    print(f"🛢️ Identified {summary[summary['plume_cluster'] != -1].shape[0]} methane plumes")
    '''
    for _, row in summary.iterrows():
        cluster_id = row['plume_cluster']
        print(f"Cluster {cluster_id}: {row['count']} anomalies | "
              f"Lat: {row['min_latitude']:.4f}-{row['max_latitude']:.4f} | "
              f"Lon: {row['min_longitude']:.4f}-{row['max_longitude']:.4f} | "
              f"Dates: {row['min_date']} to {row['max_date']}")
'''
    return anomalies


def plot_anomalies(all_data_df, anomalies, date_str):
    timestamp = datetime.now().strftime("%m-%d-%H%M")  # MM-DD-HHMM
    param_text = (f"BATCH_SIZE={BATCH_SIZE}, CONTAMINATION={CONTAMINATION}, "
                  f"MIN_SAMPLES={MIN_SAMPLES}")

    print(f"Total data points: {len(all_data_df)}")
    print(f"Total anomalies: {len(anomalies)}")
    print(f"Generating plots with timestamp: {timestamp}")

    # ---------------------------
    # 1️⃣ Plot anomalies only
    # ---------------------------
    plt.figure(figsize=(10,6))
    clusters = anomalies['plume_cluster'].unique()
    cmap = plt.colormaps['tab10']
    colors = [cmap(i % 10) for i in range(len(clusters))]

    for i, cluster in enumerate(clusters):
        cluster_points = anomalies[anomalies['plume_cluster'] == cluster]
        if cluster == -1:
            plt.scatter(cluster_points['latitude'], cluster_points['longitude'], 
                        s=5, color='grey', label='Noise')
        else:
            plt.scatter(cluster_points['latitude'], cluster_points['longitude'], 
                        s=5, color=colors[i], label=f'Plume {cluster}')

    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(f'Methane Plume Anomalies ({date_str})')
    #plt.legend()
    plt.figtext(0.5, 0.01, param_text, ha="center", fontsize=8, wrap=True)  # ⬅️ Added here
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    anomalies_file = f"anomalies_only_{date_str}.png"
    plt.savefig(anomalies_file, dpi=300)
    plt.close()
    print(f"Saved anomalies-only plot as {anomalies_file}")

    return anomalies_file

    # ---------------------------
    # 2️⃣ Plot all datapoints
    # ---------------------------
    '''
    plt.figure(figsize=(10,6))
    plt.scatter(all_data_df['longitude'], all_data_df['latitude'], 
                s=1, alpha=0.3, label='All points', color='lightblue')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(f'All Data Points ({timestamp})')
    plt.legend()
    plt.figtext(0.5, 0.01, param_text, ha="center", fontsize=8, wrap=True)  # ⬅️ Added here too
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    all_data_file = f"all_data_points_{timestamp}.png"
    plt.savefig(all_data_file, dpi=300)
    plt.close()
    print(f"Saved all-data plot as {all_data_file}")
    '''


def run_model_for_date(bucket, prefix, date_str):
    all_anomalies = []
    df_batches = []

    # Target folder in S3 for this date
    date_prefix = f"{prefix}/{date_str}"

    for df_batch in read_parquet_in_batches(bucket, date_prefix, batch_size=BATCH_SIZE):
        if df_batch.empty:
            continue

        df_batch = preprocess(df_batch)
        df_batches.append(df_batch)
        anomalies_batch = detect_anomalies(df_batch)
        anomalies_batch = cluster_anomalies(anomalies_batch)
        all_anomalies.append(anomalies_batch)

    if all_anomalies:
        all_anomalies_df = pd.concat(all_anomalies, ignore_index=True)
        all_data_df = pd.concat(df_batches, ignore_index=True)
        return plot_anomalies(all_data_df, all_anomalies_df, date_str)
    else:
        print(f"No anomalies detected for {date_str}.")

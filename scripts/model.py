import pyarrow.dataset as ds
import pyarrow.fs as fs
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
from config import PREFIX, BATCH_SIZE, CONTAMINATION, RANDOM_STATE, EPS, MIN_SAMPLES


def read_parquet_in_batches(bucket_name, prefix, batch_size=BATCH_SIZE):
    s3_path = f"{bucket_name}/{prefix}"
    print(f"Reading dataset from: {s3_path}")

    s3 = fs.S3FileSystem()
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

    # Cluster based on latitude, longitude, and date
    X = anomalies[['latitude', 'longitude', 'date_ordinal']].to_numpy()
    db = DBSCAN(eps=EPS, min_samples=MIN_SAMPLES).fit(X)
    anomalies['plume_cluster'] = db.labels_

    # Summarize anomalies by cluster
    summary = anomalies.groupby('plume_cluster').agg(
        count=('latitude', 'size'),
        min_latitude=('latitude', 'min'),
        max_latitude=('latitude', 'max'),
        min_longitude=('longitude', 'min'),
        max_longitude=('longitude', 'max'),
        min_date=('date', 'min'),
        max_date=('date', 'max')
    ).reset_index()

    for _, row in summary.iterrows():
        cluster_id = row['plume_cluster']
        print(f"Cluster {cluster_id}: {row['count']} anomalies | "
              f"Lat: {row['min_latitude']:.4f}-{row['max_latitude']:.4f} | "
              f"Lon: {row['min_longitude']:.4f}-{row['max_longitude']:.4f} | "
              f"Dates: {row['min_date']} to {row['max_date']}")

    return anomalies



def plot_anomalies(df, anomalies):
    plt.figure(figsize=(10,6))
    plt.scatter(df['longitude'], df['latitude'], s=1, alpha=0.3, label='Normal')
    
    clusters = anomalies['plume_cluster'].unique()
    colors = plt.cm.get_cmap('tab10', len(clusters))
    
    for i, cluster in enumerate(clusters):
        cluster_points = anomalies[anomalies['plume_cluster'] == cluster]
        if cluster == -1:
            plt.scatter(cluster_points['longitude'], cluster_points['latitude'], s=5, color='grey', label='Noise')
        else:
            plt.scatter(cluster_points['longitude'], cluster_points['latitude'], s=5, color=colors(i), label=f'Plume {cluster}')
    
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Methane Plume Anomalies')
    plt.legend()
    plt.show()


if __name__ == "__main__":
    bucket = "ndvi-daily-data"
    prefix = PREFIX
    
    all_anomalies = []

    # Process S3 in batches
    for df_batch in read_parquet_in_batches(bucket, prefix, batch_size=BATCH_SIZE):
        # Skip empty batches
        if df_batch.empty:
            continue

        df_batch = preprocess(df_batch)
        anomalies_batch = detect_anomalies(df_batch)
        anomalies_batch = cluster_anomalies(anomalies_batch)
        all_anomalies.append(anomalies_batch)

    # Combine anomalies across all batches
    if all_anomalies:
        all_anomalies_df = pd.concat(all_anomalies, ignore_index=True)
        plot_anomalies(all_anomalies_df, all_anomalies_df)
    else:
        print("No anomalies detected.")

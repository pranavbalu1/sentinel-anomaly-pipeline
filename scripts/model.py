import pyarrow.dataset as ds
import pyarrow.fs as fs
import pandas as pd
import numpy as np
import os
import argparse
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN, OPTICS
from sklearn.mixture import GaussianMixture
import hdbscan
import matplotlib.pyplot as plt
from config import PREFIX, BATCH_SIZE, CONTAMINATION, RANDOM_STATE, EPS, MIN_SAMPLES, N_COMPONENTS, XI, MIN_CLUSTER_SIZE
from dotenv import load_dotenv
from datetime import datetime

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


def cluster_with_dbscan(X):
    db = DBSCAN(eps=EPS, min_samples=MIN_SAMPLES).fit(X)
    return db.labels_


def cluster_with_hdbscan(X):
    clusterer = hdbscan.HDBSCAN(min_cluster_size=MIN_SAMPLES)
    return clusterer.fit_predict(X)


def cluster_with_optics(X):
    clusterer = OPTICS(min_samples=MIN_SAMPLES, xi=XI, min_cluster_size=MIN_CLUSTER_SIZE)
    return clusterer.fit_predict(X)


def cluster_with_gmm(X, n_components=N_COMPONENTS):
    clusterer = GaussianMixture(n_components=n_components, random_state=RANDOM_STATE)
    return clusterer.fit_predict(X)


def cluster_anomalies(anomalies, methods):
    if anomalies.empty:
        return {m: anomalies for m in methods}

    X = anomalies[['latitude_scaled', 'longitude_scaled', 'date_ordinal_scaled']].to_numpy()
    results = {}

    for method in methods:
        if method == "dbscan":
            labels = cluster_with_dbscan(X)
        elif method == "hdbscan":
            labels = cluster_with_hdbscan(X)
        elif method == "optics":
            labels = cluster_with_optics(X)
        elif method == "gmm":
            labels = cluster_with_gmm(X)
        else:
            print(f"⚠️ Unknown method: {method}, skipping...")
            continue

        anomalies_copy = anomalies.copy()
        anomalies_copy[f'plume_cluster_{method}'] = labels
        results[method] = anomalies_copy

        summary = anomalies_copy.groupby(f'plume_cluster_{method}').agg(
            count=('latitude', 'size'),
            min_latitude=('latitude', 'min'),
            max_latitude=('latitude', 'max'),
            min_longitude=('longitude', 'min'),
            max_longitude=('longitude', 'max'),
            min_date=('date', 'min'),
            max_date=('date', 'max')
        ).reset_index()

        print(f"\n📊 {method.upper()} Clustering Results:")
        for _, row in summary.iterrows():
            cluster_id = row[f'plume_cluster_{method}']
            print(f"Cluster {cluster_id}: {row['count']} anomalies | "
                  f"Lat: {row['min_latitude']:.4f}-{row['max_latitude']:.4f} | "
                  f"Lon: {row['min_longitude']:.4f}-{row['max_longitude']:.4f} | "
                  f"Dates: {row['min_date']} to {row['max_date']}")

    return results


def plot_anomalies(all_data_df, results):
    timestamp = datetime.now().strftime("%m-%d-%H%M")
    param_text = (f"BATCH_SIZE={BATCH_SIZE}, CONTAMINATION={CONTAMINATION}, "
                  f"RANDOM_STATE={RANDOM_STATE}, EPS={EPS}, MIN_SAMPLES={MIN_SAMPLES}")

    for method, anomalies in results.items():
        plt.figure(figsize=(10,6))
        clusters = anomalies[f'plume_cluster_{method}'].unique()
        cmap = plt.colormaps['tab10']
        colors = [cmap(i % 10) for i in range(len(clusters))]

        for i, cluster in enumerate(clusters):
            cluster_points = anomalies[anomalies[f'plume_cluster_{method}'] == cluster]
            if cluster == -1:
                plt.scatter(cluster_points['longitude'], cluster_points['latitude'],
                            s=5, color='grey', label='Noise')
            else:
                plt.scatter(cluster_points['longitude'], cluster_points['latitude'],
                            s=5, color=colors[i], label=f'Plume {cluster}')

        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title(f'Methane Plume Anomalies\n({method.upper()} Clustering) - {timestamp}')
        
        # Footer text shows parameters + model used
        footer_text = f"Model: {method.upper()} | {param_text}"
        plt.figtext(0.5, 0.01, footer_text, ha="center", fontsize=8, wrap=True)

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        anomalies_file = f"anomalies_{method}_{timestamp}.png"
        plt.savefig(anomalies_file, dpi=300)
        plt.close()
        print(f"💾 Saved {method.upper()} anomalies plot as {anomalies_file}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Methane anomaly detection & clustering")
    parser.add_argument("--methods", nargs="+", default=["dbscan"],
                        help="Clustering methods: dbscan hdbscan optics gmm (choose one or more)")
    parser.add_argument("--gmm-components", type=int, default=5,
                        help="Number of components for GMM")
    args = parser.parse_args()

    bucket = "ndvi-daily-data"
    prefix = PREFIX

    all_anomalies = []
    df_batches = []

    for df_batch in read_parquet_in_batches(bucket, prefix, batch_size=BATCH_SIZE):
        if df_batch.empty:
            continue

        df_batch = preprocess(df_batch)
        df_batches.append(df_batch)
        anomalies_batch = detect_anomalies(df_batch)
        clustered_results = cluster_anomalies(anomalies_batch, args.methods)

        for method, df in clustered_results.items():
            all_anomalies.append(df)

    if all_anomalies:
        all_data_df = pd.concat(df_batches, ignore_index=True)
        all_anomalies_dict = {m: pd.concat([df for df in all_anomalies if f'plume_cluster_{m}' in df.columns],
                                           ignore_index=True) for m in args.methods}
        plot_anomalies(all_data_df, all_anomalies_dict)
    else:
        print("No anomalies detected.")

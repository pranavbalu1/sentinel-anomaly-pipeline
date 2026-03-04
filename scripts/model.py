"""Batch anomaly detection and plume clustering for transformed Sentinel parquet data."""

import os
import warnings
from datetime import datetime

import hdbscan
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as fs
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from config import BATCH_SIZE, CONTAMINATION, MIN_SAMPLES, RANDOM_STATE

warnings.filterwarnings("ignore", category=FutureWarning, module="sklearn")
load_dotenv()


def read_parquet_in_batches(bucket_name: str, prefix: str, batch_size: int = BATCH_SIZE):
    """Stream parquet rows from S3 as pandas DataFrames to keep memory bounded."""
    s3_path = f"{bucket_name}/{prefix}"
    print(f"Reading dataset from: {s3_path}")

    # Instantiate S3 filesystem for pyarrow dataset scanning.
    s3 = fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_DEFAULT_REGION") or "us-east-2",
    )

    dataset = ds.dataset(s3_path, format="parquet", filesystem=s3)
    for batch in dataset.to_batches(batch_size=batch_size):
        df = batch.to_pandas()

        # Exclude all-zero SWIR readings that are typically invalid/background pixels.
        yield df[(df["B11"] != 0) & (df["B12"] != 0)]


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare features: parse date, add B11-B12 delta, and standardize model inputs."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return df

    # Convert dates to numeric day scale and derive methane-sensitive SWIR difference.
    df["date_ordinal"] = df["date"].astype("int64") / 1e9 / 86400
    df["B_diff"] = df["B11"] - df["B12"]

    features = ["B11", "B12", "B_diff", "latitude", "longitude", "date_ordinal"]
    scaler = StandardScaler()
    df[[f"{feature}_scaled" for feature in features]] = scaler.fit_transform(df[features])
    return df


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Isolation Forest and return only outlier rows marked as anomalies."""
    features = [
        "B11_scaled",
        "B12_scaled",
        "B_diff_scaled",
        "latitude_scaled",
        "longitude_scaled",
        "date_ordinal_scaled",
    ]
    model = IsolationForest(contamination=CONTAMINATION, random_state=RANDOM_STATE)
    df["anomaly"] = model.fit_predict(df[features])

    anomalies = df[df["anomaly"] == -1].copy()
    print(f"⚠️ Detected {len(anomalies)} anomalies in this batch")
    return anomalies


def cluster_anomalies(anomalies: pd.DataFrame) -> pd.DataFrame:
    """Cluster anomaly points into plume groups using HDBSCAN."""
    if anomalies.empty:
        return anomalies

    # Use scaled geo-temporal coordinates for density-based clustering.
    points = anomalies[["latitude_scaled", "longitude_scaled", "date_ordinal_scaled"]].to_numpy()
    clusterer = hdbscan.HDBSCAN(min_cluster_size=MIN_SAMPLES, metric="euclidean")
    anomalies["plume_cluster"] = clusterer.fit_predict(points)

    summary = anomalies.groupby("plume_cluster").agg(
        count=("latitude", "size"),
        min_latitude=("latitude", "min"),
        max_latitude=("latitude", "max"),
        min_longitude=("longitude", "min"),
        max_longitude=("longitude", "max"),
        min_date=("date", "min"),
        max_date=("date", "max"),
    ).reset_index()
    print(f"🛢️ Identified {summary[summary['plume_cluster'] != -1].shape[0]} methane plumes")
    return anomalies


def plot_anomalies(all_data_df: pd.DataFrame, anomalies: pd.DataFrame, date_str: str) -> str:
    """Generate anomalies-only scatter plot and return saved PNG path."""
    timestamp = datetime.now().strftime("%m-%d-%H%M")
    param_text = (
        f"BATCH_SIZE={BATCH_SIZE}, CONTAMINATION={CONTAMINATION}, "
        f"MIN_SAMPLES={MIN_SAMPLES}"
    )

    print(f"Total data points: {len(all_data_df)}")
    print(f"Total anomalies: {len(anomalies)}")
    print(f"Generating plots with timestamp: {timestamp}")

    plt.figure(figsize=(10, 6))
    clusters = anomalies["plume_cluster"].unique()
    cmap = plt.colormaps["tab10"]
    colors = [cmap(i % 10) for i in range(len(clusters))]

    # Plot each cluster with a separate color; noise remains grey.
    for i, cluster in enumerate(clusters):
        cluster_points = anomalies[anomalies["plume_cluster"] == cluster]
        if cluster == -1:
            plt.scatter(cluster_points["latitude"], cluster_points["longitude"], s=5, color="grey", label="Noise")
        else:
            plt.scatter(
                cluster_points["latitude"],
                cluster_points["longitude"],
                s=5,
                color=colors[i],
                label=f"Plume {cluster}",
            )

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Methane Plume Anomalies ({date_str})")
    plt.figtext(0.5, 0.01, param_text, ha="center", fontsize=8, wrap=True)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    anomalies_file = f"anomalies_only_{date_str}.png"
    plt.savefig(anomalies_file, dpi=300)
    plt.close()
    print(f"Saved anomalies-only plot as {anomalies_file}")
    return anomalies_file


def run_model_for_date(bucket: str, prefix: str, date_str: str):
    """Run anomaly workflow for a single date-partition path and return plot path."""
    all_anomalies = []
    df_batches = []

    # Scope reads to one date partition under the shared prefix.
    date_prefix = f"{prefix}/{date_str}"

    for df_batch in read_parquet_in_batches(bucket, date_prefix, batch_size=BATCH_SIZE):
        if df_batch.empty:
            continue

        processed_batch = preprocess(df_batch)
        if processed_batch.empty:
            continue

        df_batches.append(processed_batch)
        anomalies_batch = detect_anomalies(processed_batch)
        anomalies_batch = cluster_anomalies(anomalies_batch)
        all_anomalies.append(anomalies_batch)

    if all_anomalies:
        all_anomalies_df = pd.concat(all_anomalies, ignore_index=True)
        all_data_df = pd.concat(df_batches, ignore_index=True)
        return plot_anomalies(all_data_df, all_anomalies_df, date_str)

    print(f"No anomalies detected for {date_str}.")
    return None

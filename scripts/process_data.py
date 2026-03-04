"""Raster-to-parquet preprocessing utilities for Sentinel B11/B12 imagery."""

import os
import re

import numpy as np
import rasterio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import FloatType, StringType, StructField, StructType

# Reuse one Spark session for all local transformations in this process.
spark = SparkSession.builder.appName("SatellitePreprocessing").getOrCreate()


def extract_geospatial_data(tiff_path: str):
    """Read TIFF bands and return row-wise tuples of lon/lat/B11/B12 values."""
    print(f"📂 Opening TIFF file: {tiff_path}")
    with rasterio.open(tiff_path) as src:
        # Read as (bands, height, width) then transpose to (height, width, bands).
        bands = src.read()
        bands = np.transpose(bands, (1, 2, 0))
        height, width, _ = bands.shape

        # Build coordinate grids and convert pixel indices to geographic coordinates.
        rows, cols = np.meshgrid(np.arange(height), np.arange(width), indexing="ij")
        xs, ys = rasterio.transform.xy(src.transform, rows, cols)

        # Flatten coordinates/pixels so each item represents one geolocated pixel.
        coords = list(zip(np.array(xs).flatten(), np.array(ys).flatten()))
        pixels = bands.reshape(-1, bands.shape[2])

        data = [
            (
                float(coords[i][0]),  # longitude
                float(coords[i][1]),  # latitude
                float(pixels[i][0]),  # B11
                float(pixels[i][1]),  # B12
            )
            for i in range(len(pixels))
        ]

    return data


def create_spark_df(data, date_str: str):
    """Create a typed Spark DataFrame from extracted pixel tuples."""
    schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("B11", FloatType(), True),
        StructField("B12", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)

    # Attach acquisition date for partitioning/model temporal context.
    return df.withColumn("date", lit(date_str).cast(StringType()))


def transform_red_edge_image(tiff_path: str, output_path: str) -> None:
    """Transform TIFF pixels into filtered parquet format, then remove source TIFF."""
    print(f"🛠️ Transforming image: {tiff_path}")

    # Pull YYYY-MM-DD from filename; fallback keeps pipeline resilient if filename changes.
    match = re.search(r"(\d{4}-\d{2}-\d{2})", tiff_path)
    date_str = match.group(1) if match else "unknown"
    print(f"📅 Data Date: {date_str}")

    # Convert raster pixels to a Spark DataFrame enriched with date metadata.
    data = extract_geospatial_data(tiff_path)
    df = create_spark_df(data, date_str)

    # Filter out very low/invalid signal pixels to reduce downstream noise.
    df_filtered = df.filter((col("B11") > 0.01) & (col("B12") > 0.01))
    print("📉 Filtered DataFrame:")
    df_filtered.show(5)

    # Persist transformed data for model consumption and S3 upload.
    df_filtered.write.mode("overwrite").parquet(output_path)
    print(f"✅ Processed data saved to {output_path}")

    # Delete local raw TIFF to avoid storage accumulation.
    os.remove(tiff_path)

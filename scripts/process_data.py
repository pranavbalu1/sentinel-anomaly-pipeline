import rasterio
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import col, lit
import os
import re

# Initialize Spark
spark = SparkSession.builder.appName("SatellitePreprocessing").getOrCreate()

def extract_geospatial_data(tiff_path):
    print(f"📂 Opening TIFF file: {tiff_path}")
    with rasterio.open(tiff_path) as src:
        bands = src.read()  # Shape: (bands, height, width)
        bands = np.transpose(bands, (1, 2, 0))  # (height, width, bands)
        height, width, _ = bands.shape

        # Generate pixel coordinates mapped to geographic coordinates
        rows, cols = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')
        xs, ys = rasterio.transform.xy(src.transform, rows, cols)

        # Flatten arrays for Spark dataframe creation
        coords = list(zip(np.array(xs).flatten(), np.array(ys).flatten()))
        pixels = bands.reshape(-1, bands.shape[2])

        # Combine geospatial coordinates with pixel band data
        data = [
            (
                float(coords[i][0]),  # latitude
                float(coords[i][1]),  # longitude
                float(pixels[i][0]),  # band1 (B11)
                float(pixels[i][1])   # band2 (B12)
            )
            for i in range(len(pixels))
        ]

        return data

def create_spark_df(data, date_str):
    schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("B11", FloatType(), True),
        StructField("B12", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)
    # Add date column
    df = df.withColumn("date", lit(date_str).cast(StringType()))
    return df

def transform_red_edge_image(tiff_path, output_path):
    print(f"🛠️ Transforming image: {tiff_path}")

    # Extract date from filename using regex
    match = re.search(r"(\d{4}-\d{2}-\d{2})", tiff_path)
    date_str = match.group(1) if match else "unknown"
    print(f"📅 Data Date: {date_str}")

    # Extract pixel data
    data = extract_geospatial_data(tiff_path)

    # Create Spark DataFrame with date
    df = create_spark_df(data, date_str)

    # Filter low-value pixels to reduce noise
    df_filtered = df.filter(
        (col("B11") > 0.01) &
        (col("B12") > 0.01)
    )
    print("📉 Filtered DataFrame:")
    df_filtered.show(5)

    # Save filtered data as Parquet for downstream use
    df_filtered.write.mode("overwrite").parquet(output_path)
    print(f"✅ Processed data saved to {output_path}")

    # Cleanup
    os.remove(tiff_path)

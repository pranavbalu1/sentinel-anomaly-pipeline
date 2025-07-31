import rasterio
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql.functions import col
import os

# Initialize Spark
spark = SparkSession.builder.appName("SatellitePreprocessing").getOrCreate()

def extract_geospatial_data(tiff_path):
    print(f"📂 Opening TIFF file: {tiff_path}")
    with rasterio.open(tiff_path) as src:
        bands = src.read()  # Shape: (bands, height, width)
        print(f"ℹ️ Image shape (bands, height, width): {bands.shape}")
        bands = np.transpose(bands, (1, 2, 0))  # (height, width, bands)
        height, width, _ = bands.shape

        # Generate pixel coordinates mapped to geographic coordinates
        rows, cols = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')
        xs, ys = rasterio.transform.xy(src.transform, rows, cols)

        # Flatten the arrays for Spark dataframe creation
        coords = list(zip(np.array(xs).flatten(), np.array(ys).flatten()))
        pixels = bands.reshape(-1, bands.shape[2])

        # Combine geospatial coordinates with pixel band data
        data = [
            (
                float(coords[i][0]),  # latitude
                float(coords[i][1]),  # longitude
                float(pixels[i][0]),  # red_edge1
                float(pixels[i][1]),  # red_edge2
                float(pixels[i][2])   # red_edge3
            )
            for i in range(len(pixels))
        ]

        return data

def create_spark_df(data):
    schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("red_edge1", FloatType(), True),
        StructField("red_edge2", FloatType(), True),
        StructField("red_edge3", FloatType(), True)
    ])
    df = spark.createDataFrame(data, schema=schema)
    print("✅ Spark DataFrame created with coordinates.")
    df.show(5)
    return df

def transform_red_edge_image(tiff_path, output_path):
    print(f"🛠️ Transforming image: {tiff_path}")
    data = extract_geospatial_data(tiff_path)
    df = create_spark_df(data)

    # Filter low-value pixels to reduce noise
    df_filtered = df.filter(
        (col("red_edge1") > 0.01) &
        (col("red_edge2") > 0.01) &
        (col("red_edge3") > 0.01)
    )
    print("📉 Filtered DataFrame:")
    df_filtered.show(5)

    # Save filtered data as Parquet for downstream use
    df_filtered.write.mode("overwrite").parquet(output_path)
    print(f"✅ Processed data saved to {output_path}")

    #Cleanup
    os.remove(tiff_path)

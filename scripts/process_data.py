import rasterio
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql.functions import col

# Initialize Spark
spark = SparkSession.builder.appName("SatellitePreprocessing").getOrCreate()

def extract_pixel_data(tiff_path):
    print(f"📂 Opening TIFF file: {tiff_path}")
    with rasterio.open(tiff_path) as src:
        img = src.read()  # Shape: (bands, height, width)
        print(f"ℹ️ Image shape (bands, height, width): {img.shape}")
        img = np.transpose(img, (1, 2, 0))  # Shape: (height, width, bands)

        # Flatten the image
        height, width, bands = img.shape
        pixels = img.reshape((height * width, bands))

        # Show some pixel data
        print(f"🔍 Sample raw pixels (first 5):\n{pixels[:5]}")
        return pixels

def create_spark_df(pixels):
    schema = StructType([
        StructField("red_edge1", FloatType(), True),
        StructField("red_edge2", FloatType(), True),
        StructField("red_edge3", FloatType(), True)
    ])
    df = spark.createDataFrame(pixels.tolist(), schema=schema)
    print("✅ Spark DataFrame created.")
    print("📊 Sample DataFrame rows (first 5):")
    df.show(5)
    return df

def transform_red_edge_image(tiff_path, output_path):
    print(f"🛠️  Transforming image: {tiff_path}")
    pixels = extract_pixel_data(tiff_path)
    df = create_spark_df(pixels)

    # Filter low-value pixels to remove noise (threshold can be adjusted)
    df_filtered = df.filter("red_edge1 > 0.01 AND red_edge2 > 0.01 AND red_edge3 > 0.01")
    print("📉 Filtered DataFrame (red_edge1 > 0.01, red_edge2 > 0.01, red_edge3 > 0.01):")
    df_filtered.show(5)


    # Save the processed dataset with features as Parquet
    df_filtered.write.mode("overwrite").parquet(output_path)
    print(f"✅ Transformed data with vegetation indices written to {output_path}")

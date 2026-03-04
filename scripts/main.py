"""Scratchpad snippet showing the key Spark/raster transformation operations.

This file appears to be a development notebook-style reference rather than an
entrypoint. The runnable pipeline uses `flow.py` + module functions.
"""

# type: ignore

# Example: read raster bands from a TIFF.
with rasterio.open(tiff_path) as src:
    bands = src.read()  # Shape: (bands, height, width)

# Example: flatten coordinates and pixel matrices for tabular conversion.
coords = list(zip(np.array(xs).flatten(), np.array(ys).flatten()))
pixels = bands.reshape(-1, bands.shape[2])

# Example: create Spark DataFrame and annotate with acquisition date.
df = spark.createDataFrame(data, schema=schema)
df = df.withColumn("date", lit(date_str))

# Example: filter low signal values and write parquet output.
df_filtered = df.filter((col("B11") > 0.01) & (col("B12") > 0.01))
df_filtered.write.mode("overwrite").parquet(output_path)

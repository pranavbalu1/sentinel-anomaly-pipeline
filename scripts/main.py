# type: ignore

with rasterio.open(tiff_path) as src: 
    bands = src.read()   # Shape: (bands, height, width)

coords = list(zip(np.array(xs).flatten(), np.array(ys).flatten()))
pixels = bands.reshape(-1, bands.shape[2])

df = spark.createDataFrame(data, schema=schema)
df = df.withColumn("date", lit(date_str))

df_filtered = df.filter((col("B11") > 0.01) & (col("B12") > 0.01))
df_filtered.write.mode("overwrite").parquet(output_path)



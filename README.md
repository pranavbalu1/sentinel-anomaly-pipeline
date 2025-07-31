# 🌍 Sentinel-5P Satellite Data Pipeline

This project ingests, processes, and uploads Earth observation data from the **Sentinel-5P** satellite with a focus on **red-edge bands** used in vegetation analysis. It is designed as a reproducible, daily pipeline that automates downloading, transforming, and uploading satellite imagery for scalable environmental monitoring and analysis.

---

## 🛰 About Sentinel-5P

The **Sentinel-5P (Precursor)** satellite, part of the Copernicus Earth observation program run by the European Space Agency (ESA), is equipped with the **TROPOMI** instrument to monitor atmospheric trace gases and aerosols. It captures high-resolution data critical for environmental studies, especially:

* **Air quality monitoring**
* **Greenhouse gas emissions**
* **Vegetation health via spectral bands (e.g., red-edge)**

---

## 🚀 What This Pipeline Does

1. **Download** raw Sentinel-5P imagery for the current day
2. **Process** the imagery to extract relevant red-edge spectral bands
3. **Transform** the image data into tabular format for analytical use
4. **Filter** out invalid/irrelevant pixels based on a red-edge threshold
5. **Save** the transformed dataset as a `.parquet` file
6. **Upload** the transformed data to an **Amazon S3** bucket in a date-partitioned format

---

## 📊 Data Lifecycle

### 🔹 Step 1: Raw Data (TIFF)

* **Format:** GeoTIFF (`.tiff`)
* **Source:** Sentinel-5P via Copernicus Open Access Hub or Sentinel Hub API
* **Structure:** Multiband image array (3 bands: `red_edge1`, `red_edge2`, `red_edge3`)
* **Example shape:** `(3, 512, 512)`

### 🔹 Step 2: Intermediate Data (NumPy array ➝ Spark DataFrame)

* **Parsed into rows:** Each row = one pixel with 3 red-edge values
* **Initial filtering:** Drop any pixel where any red-edge value < 0.01

### 🔹 Step 3: Transformed Data (Parquet)

* **Output:** Cleaned Spark DataFrame serialized to `.parquet`
* **Columns:** `red_edge1`, `red_edge2`, `red_edge3`
* **Sample Output:**

  ```text
  +----------+----------+----------+
  |red_edge1 |red_edge2 |red_edge3 |
  +----------+----------+----------+
  | 0.5343   | 0.5538   | 0.5650   |
  | 0.5845   | 0.6057   | 0.6173   |
  | 0.2983   | 0.3377   | 0.3530   |
  ```

### 🔹 Step 4: Storage (Amazon S3)

* **Path Format:**

  ```
  s3://ndvi-daily-data/sentinel-data/date=YYYY-MM-DD/
  ```
* **Partitioned by:** Date (for time-series querying)

---

## 📂 File-by-File Explanation

### `main.py`

* **Role:** Entry point of the pipeline
* **Function:** Coordinates downloading, processing, and uploading
* **Key Actions:**

  * Gets today’s date
  * Calls download and processing modules
  * Uploads `.parquet` to S3

### `sentinel_download.py`

* **Role:** Download helper
* **Function:** Authenticates with Sentinel API and downloads a `.tiff` image file
* **Details:** Uses an access token to make a GET request for today’s image

### `process_data.py`

* **Role:** Transformation logic
* **Function:** Converts `.tiff` to Spark DataFrame
* **Steps:**

  * Opens the TIFF file
  * Reshapes bands to tabular form
  * Filters based on red-edge thresholds
  * Saves to Parquet

### `s3_uploader.py`

* **Role:** S3 client wrapper
* **Function:** Uploads local files to AWS S3 with Boto3
* **Smart features:**

  * Verifies destination path
  * Logs success/failure

---

## ✅ Expected Output (When Pipeline Runs Smoothly)

When everything works properly, the script outputs the following:

```bash
📥 Downloading Sentinel image for 2025-07-30...
✅ Access Token retrieved.
Access Token: eyJhbGciOi...
✅ Download complete: raw_sentinel_2025-07-30.tiff

⚙️  Processing image and saving to transformed_data_2025-07-30.parquet...
🛠️  Transforming image: raw_sentinel_2025-07-30.tiff
📂 Opening TIFF file: raw_sentinel_2025-07-30.tiff
ℹ️ Image shape (bands, height, width): (3, 512, 512)
🔍 Sample raw pixels (first 5):
[[0.5343 0.5538 0.565 ]
 [0.5845 0.6057 0.6173]
 ...
✅ Spark DataFrame created.
📊 Sample DataFrame rows (first 5):
+---------+---------+---------+
|red_edge1|red_edge2|red_edge3|
+---------+---------+---------+
|   0.5343|   0.5538|    0.565|
|   0.5845|   0.6057|   0.6173|
...

📉 Filtered DataFrame (red_edge1 > 0.01, red_edge2 > 0.01, red_edge3 > 0.01):
+---------+---------+---------+
...

✅ Transformed data with vegetation indices written to transformed_data_2025-07-30.parquet

☁️ Uploading to S3...
✅ Uploaded to s3://ndvi-daily-data/sentinel-data/date=2025-07-30/...
```

---

## 💡 Use Cases for Transformed Data

The final `.parquet` dataset enables:

* 🌱 **Vegetation Health Monitoring** (NDVI, Red Edge Position Index)
* 🛰 **Time-Series Analysis** of Earth surface changes
* 🌾 **Agriculture Monitoring** (crop stress, soil condition)
* 🌳 **Deforestation and Land Use Tracking**
* 🧠 **Machine Learning Input** for anomaly detection or classification
* 📈 **Visualization Dashboards** with tools like Apache Superset or Grafana

---

## 🛠 Requirements

```bash
pip install -r requirements.txt
```

Includes:

* `boto3`
* `pyspark`
* `rasterio`
* `numpy`
* `requests`

---

## 📌 Notes

* Ensure you have AWS credentials configured to use `boto3`.
* The TIFF files should contain red-edge bands; other bands are currently ignored.
* This system can be extended to compute vegetation indices (NDVI, RECI, etc.).

---

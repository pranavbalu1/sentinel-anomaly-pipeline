
# Sentinel Methane Detection Pipeline

## Overview

This project implements a daily pipeline for downloading, processing, and analyzing Sentinel-2 satellite imagery to detect methane plumes. The pipeline is built using **Python**, **Prefect**, **PySpark**, **PyArrow**, and **AWS S3** for data storage.

The main functionality includes:

1. **Download Sentinel-2 imagery** (bands B11 and B12 sensitive to methane absorption).
2. **Process and transform the imagery** into Parquet format with geospatial coordinates.
3. **Upload processed data to AWS S3** for storage.
4. **Detect anomalies** using machine learning models (**Isolation Forest** and **DBSCAN**) to identify potential methane plumes.
5. **Visualize anomalies** on a scatter plot with clusters representing detected plumes.



## Installation

1. **Create a virtual environment and activate it:**

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows
```

2. **Install dependencies:**

```bash
pip install -r requirements.txt
```

3. **Set environment variables** (in `.env` file in the root directory):

```env
SENTINEL_CLIENT_ID=<your-client-id>
SENTINEL_CLIENT_SECRET=<your-client-secret>
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_DEFAULT_REGION=us-east-2
```

---

## Configuration

Edit `config.py` to adjust:

* `BBOX` – bounding box for your region of interest.
* `WIDTH` and `HEIGHT` – resolution of the downloaded image.
* `PREFIX` – S3 prefix for storing data.
* Batch size and ML model parameters for anomaly detection (`BATCH_SIZE`, `CONTAMINATION`, `EPS`, `MIN_SAMPLES`).

---

## Running the Data Pipeline

* **Download** Sentinel images for a specific date.
* **Process** the raw image into a Parquet file (red edge transformation).
* **Upload** the transformed data to an S3 bucket.
* Supports **single-date** or **date-range** batch processing.



## File Overview

* `flow.py`: Main Prefect flow that orchestrates download, process, and upload tasks.
* `sentinel_download.py`: Module for downloading Sentinel images.
* `process_data.py`: Module for transforming raw Sentinel images into Parquet format.
* `s3_uploader.py`: Module to upload Parquet data to S3.
* `config.py`: Contains configuration variables like `S3_BUCKET`.

## Usage

Run the pipeline via command line:

### 1. Process **today’s data** (default):

```bash
python flow.py
```

### 2. Process a **single date**:

```bash
python flow.py --date YYYY-MM-DD
```

**Example:**

```bash
python flow.py --date 2025-09-26
```

### 3. Process a **range of dates**:

```bash
python flow.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

**Example:**

```bash
python flow.py --start-date 2025-09-20 --end-date 2025-09-26
```

## Anomaly Detection

1. Reads processed Parquet files from S3 in batches.
2. Preprocesses features (`B11`, `B12`, `B_diff`, latitude, longitude, date).
3. Uses **Isolation Forest** to detect anomalous pixels.
4. Clusters anomalies with **DBSCAN** to identify plumes.
5. Visualizes results using matplotlib.

---

## Notes

* Requires **AWS credentials** configured via environment variables or AWS CLI.
* Spark is used to efficiently process large satellite images.
* TIFF images are deleted locally after transformation to save space.
* Ensure Sentinel Hub credentials are valid.

---

* Plume anomaly visualization:

  * Grey points: noise
  * Colored points: clustered methane plumes
  * Interactive scatter plot showing latitude/longitude of anomalies

````


Perfect! I can create a `README.md` and a `requirements.txt` for your project based on your code and structure. I’ll organize the README to be clear, professional, and user-friendly.

Here’s a draft:

---

### `README.md`

```markdown
# Sentinel Methane Detection Pipeline

## Overview

This project implements a daily pipeline for downloading, processing, and analyzing Sentinel-2 satellite imagery to detect methane plumes. The pipeline is built using **Python**, **Prefect**, **PySpark**, **PyArrow**, and **AWS S3** for data storage.

The main functionality includes:

1. **Download Sentinel-2 imagery** (bands B11 and B12 sensitive to methane absorption).
2. **Process and transform the imagery** into Parquet format with geospatial coordinates.
3. **Upload processed data to AWS S3** for storage.
4. **Detect anomalies** using machine learning models (**Isolation Forest** and **DBSCAN**) to identify potential methane plumes.
5. **Visualize anomalies** on a scatter plot with clusters representing detected plumes.

---

## Project Structure

```

project-root/
│
├─ sentinel_pipeline.py       # Prefect pipeline for daily processing
├─ monthly_pipeline.py        # Script to process all dates from the previous month
├─ model.py                   # Anomaly detection and clustering script
├─ process_data.py            # Image transformation & Spark DataFrame creation
├─ s3_uploader.py             # Upload transformed Parquet files to S3
├─ sentinel_download.py       # Download Sentinel-2 images via Sentinel Hub API
├─ sentinel_auth.py           # Sentinel Hub OAuth token management
├─ config.py                  # Configuration constants (BBOX, S3 prefix, etc.)
├─ requirements.txt           # Python dependencies
└─ README.md

````

---

## Installation

1. **Clone the repository:**

```bash
git clone <your-repo-url>
cd project-root
````

2. **Create a virtual environment and activate it:**

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows
```

3. **Install dependencies:**

```bash
pip install -r requirements.txt
```

4. **Set environment variables** (in `.env` file in the root directory):

```env
SENTINEL_CLIENT_ID=<your-client-id>
SENTINEL_CLIENT_SECRET=<your-client-secret>
S3_BUCKET=<your-s3-bucket>
```

---

## Configuration

Edit `config.py` to adjust:

* `BBOX` – bounding box for your region of interest.
* `WIDTH` and `HEIGHT` – resolution of the downloaded image.
* `PREFIX` – S3 prefix for storing data.
* Batch size and ML model parameters for anomaly detection (`BATCH_SIZE`, `CONTAMINATION`, `EPS`, `MIN_SAMPLES`).

---

## Running the Pipeline

### 1. Daily Pipeline

```bash
python sentinel_pipeline.py --date 2025-09-25
```

* `--date` : single date in `YYYY-MM-DD` format.
* If no date is provided, defaults to today.
* Can also process a date range with `--start-date` and `--end-date`.

### 2. Monthly Pipeline

```bash
python monthly_pipeline.py
```

* Automatically processes all dates from the previous month.

---

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

## Example Output

* Parquet files stored in S3:

```
s3://<bucket>/<prefix>/part-*.parquet
```

* Plume anomaly visualization:

  * Grey points: noise
  * Colored points: clustered methane plumes
  * Interactive scatter plot showing latitude/longitude of anomalies

````

---

### `requirements.txt`

```text
prefect==2.12.2
pandas==2.1.0
numpy==1.27.0
pyarrow==12.0.1
boto3==2.13.0
botocore==2.13.0
scikit-learn==1.3.0
matplotlib==3.8.0
rasterio==1.3.9
pyspark==3.5.0
requests==2.32.0
python-dotenv==1.0.1
````

---

I can also draft a **diagram of the pipeline workflow** for your README to make it very clear how data flows from Sentinel Hub → processing → S3 → anomaly detection.

Do you want me to include that diagram?

"""Central configuration values for the Sentinel methane pipeline."""

# Geographic bounding box [min_lon, max_lat, max_lon, min_lat] for image requests.
BBOX = [-117.85, 34.35, -117.35, 33.85]

# Pixel dimensions used when requesting TIFF output from Sentinel Hub.
WIDTH = 1600
HEIGHT = 1200

# AWS S3 destination bucket for transformed parquet outputs.
S3_BUCKET = "ndvi-daily-data"

# Prefix under the bucket where date-partitioned parquet files are stored.
PREFIX = "sentinel-data"

# ---------------------
# Modeling configuration
# ---------------------

# Number of rows to process at a time when streaming parquet batches from S3.
BATCH_SIZE = 100000

# Isolation Forest contamination ratio (expected anomaly proportion).
CONTAMINATION = 0.005

# Random seed for reproducible anomaly detection.
RANDOM_STATE = 42

# Preserved for compatibility with earlier clustering experiments.
EPS = 0.01

# Minimum cluster size parameter passed to HDBSCAN.
MIN_SAMPLES = 2

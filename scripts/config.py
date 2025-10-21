# Bounding Box for area of interest
BBOX = [-117.85, 34.35, -117.35, 33.85]
WIDTH = 1600
HEIGHT = 1200

# AWS S3
S3_BUCKET = "ndvi-daily-data"

PREFIX = "sentinel-data"


#Model parameters
BATCH_SIZE = 100000

CONTAMINATION = 0.005 # Proportion of outliers in the data
RANDOM_STATE = 42
EPS = 0.01  
MIN_SAMPLES = 2  # DBSCAN min_samples parameter

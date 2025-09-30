# Bounding Box for area of interest
BBOX = [-117.8, 33.9, -117.4, 34.3] # Example: California region
WIDTH = 2500  # Image width
HEIGHT = 2500  # Image height

# AWS S3
S3_BUCKET = "ndvi-daily-data"

PREFIX = "sentinel-data-zoomed"


#Model parameters
BATCH_SIZE = 100000

CONTAMINATION = 0.003 # Proportion of outliers in the data
RANDOM_STATE = 42
EPS = 0.01  
MIN_SAMPLES = 10  # DBSCAN min_samples parameter

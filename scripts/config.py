# Bounding Box for area of interest
BBOX = [-120.5, 35.0, -118.0, 37.0] # Example: California region
WIDTH = 1200  # Image width
HEIGHT = 1200  # Image height

# AWS S3
S3_BUCKET = "ndvi-daily-data"

#PREFIX = "sentinel-data" # 1 year of data
#PREFIX = "test" # 1 month of data
PREFIX = "test_2" # 1 day of data


#Model parameters
BATCH_SIZE = 100000

CONTAMINATION = 0.01
RANDOM_STATE = 42
EPS = 0.01  # DBSCAN eps parameter
MIN_SAMPLES = 5  # DBSCAN min_samples parameter

# Bounding Box for area of interest
BBOX = [-118.56, 33.56, -117.5, 34.245] # Example: California region
WIDTH = 2000  # Image width
HEIGHT = 1560  # Image height

# AWS S3
S3_BUCKET = "ndvi-daily-data"

#PREFIX = "sentinel-data" # 1 year of data
#PREFIX = "test" # 1 month of data
#PREFIX = "test_2" # 1 day of data
PREFIX = "known_sources" # Known methane sources


#Model parameters
BATCH_SIZE = 100000

CONTAMINATION = 0.01 # Proportion of outliers in the data
RANDOM_STATE = 42
EPS = 0.01  
MIN_SAMPLES = 5  # DBSCAN min_samples parameter

import os
import datetime
from sentinel_download import download_sentinel_image
from process_data import transform_red_edge_image
from s3_uploader import upload_parquet_directory_to_s3
import glob


def main():
    #Setup
    date = datetime.date.today().strftime("%Y-%m-%d")
    raw_file = f"raw_sentinel_{date}.tiff"
    transformed_file = f"transformed_data_{date}.parquet"

    #Download Raw Data
    print(f"📥 Downloading Sentinel image for {date}...")
    download_sentinel_image(date, raw_file)

    #Transform and Save as Parquet
    print(f"⚙️  Processing image and saving to {transformed_file}...")
    transform_red_edge_image(raw_file, transformed_file)

    #Upload to S3
    print(f"☁️ Uploading to S3...")

    bucket_name = "ndvi-daily-data"
    local_base_dir = os.path.dirname(__file__)  # points to /scripts
    upload_parquet_directory_to_s3(date, local_base_dir, transformed_file, bucket_name)

    #cleanup, leave it be for now since 7/30 data isn't empty
    #os.remove(raw_file)

if __name__ == "__main__":
    main()
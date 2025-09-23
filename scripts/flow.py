from prefect import flow, task
from datetime import date
import os
import argparse
from sentinel_download import download_sentinel_image
from process_data import transform_red_edge_image
from s3_uploader import upload_parquet_directory_to_s3

@task
def download_task(date_str, raw_file):
    download_sentinel_image(date_str, raw_file)

@task
def process_task(raw_file, transformed_file):
    transform_red_edge_image(raw_file, transformed_file)

@task
def upload_task(date_str, base_dir, transformed_file, bucket):
    upload_parquet_directory_to_s3(date_str, base_dir, transformed_file, bucket)

@flow(name="Sentinel Daily Pipeline")
def sentinel_pipeline(date_str: str):
    # File paths
    raw_file = f"raw_sentinel_{date_str}.tiff"
    transformed_file = f"transformed_data_{date_str}.parquet"
    local_base_dir = os.path.dirname(__file__)
    bucket_name = "ndvi-daily-data"

    # Tasks
    download_task(date_str, raw_file)
    process_task(raw_file, transformed_file)
    upload_task(date_str, local_base_dir, transformed_file, bucket_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Sentinel daily pipeline")
    parser.add_argument(
        "--date",
        type=str,
        help="Date for the pipeline in YYYY-MM-DD format (defaults to today)"
    )
    args = parser.parse_args()

    run_date = args.date or date.today().strftime("%Y-%m-%d")
    sentinel_pipeline(run_date)

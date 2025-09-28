from prefect import flow, task
from datetime import date, datetime, timedelta
import os
import argparse
from sentinel_download import download_sentinel_image
from process_data import transform_red_edge_image
from s3_uploader import upload_parquet_directory_to_s3
from model import run_model_for_date
from config import S3_BUCKET, PREFIX


@task
def download_task(date_str, raw_file):
    download_sentinel_image(date_str, raw_file)


@task
def process_task(raw_file, transformed_file):
    transform_red_edge_image(raw_file, transformed_file)


@task
def upload_task(date_str, base_dir, transformed_file, bucket):
    # Upload parquet files into date-based folder
    upload_parquet_directory_to_s3(date_str, base_dir, transformed_file, bucket)


@task
def analyze_task(bucket, prefix, date_str):
    graph_file = run_model_for_date(bucket, prefix, date_str)
    #if graph_file:
        #from s3_uploader import upload_graph_to_s3
        #upload_graph_to_s3(graph_file, bucket, date_str)



@flow(name="Sentinel Daily Pipeline")
def sentinel_pipeline(date_str: str):
    # File paths
    raw_file = f"raw_sentinel_{date_str}.tiff"
    transformed_file = f"transformed_data_{date_str}.parquet"
    local_base_dir = os.path.dirname(__file__)
    bucket_name = S3_BUCKET
    prefix = PREFIX

    # Tasks
    download_task(date_str, raw_file)
    process_task(raw_file, transformed_file)
    upload_task(date_str, local_base_dir, transformed_file, bucket_name)
    analyze_task(bucket_name, prefix, date_str)


def daterange(start_date, end_date):
    """Yield all dates between start_date and end_date (inclusive)."""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Sentinel daily pipeline")
    parser.add_argument("--date", type=str, help="Single date (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for range (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.date:
        sentinel_pipeline(args.date)
    elif args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        for d in daterange(start, end):
            sentinel_pipeline(d.strftime("%Y-%m-%d"))
    else:
        today = date.today().strftime("%Y-%m-%d")
        sentinel_pipeline(today)

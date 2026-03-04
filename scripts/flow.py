"""Prefect orchestration flow for daily Sentinel download, transform, upload, and analysis."""

import argparse
import os
from datetime import date, datetime, timedelta

from prefect import flow, task

from config import PREFIX, S3_BUCKET
from model import run_model_for_date
from process_data import transform_red_edge_image
from s3_uploader import upload_parquet_directory_to_s3
from sentinel_download import download_sentinel_image


@task
def download_task(date_str: str, raw_file: str) -> None:
    """Download a Sentinel TIFF for a specific date into a local file."""
    download_sentinel_image(date_str, raw_file)


@task
def process_task(raw_file: str, transformed_file: str) -> None:
    """Convert raw TIFF pixels to filtered parquet records."""
    transform_red_edge_image(raw_file, transformed_file)


@task
def upload_task(date_str: str, base_dir: str, transformed_file: str, bucket: str) -> None:
    """Upload partitioned parquet outputs for a date to S3."""
    upload_parquet_directory_to_s3(date_str, base_dir, transformed_file, bucket)


@task
def analyze_task(bucket: str, prefix: str, date_str: str) -> None:
    """Run anomaly detection on the uploaded date partition and produce a chart."""
    graph_file = run_model_for_date(bucket, prefix, date_str)

    # Optional future enhancement: upload graph artifacts to S3.
    # if graph_file:
    #     from s3_uploader import upload_graph_to_s3
    #     upload_graph_to_s3(graph_file, bucket, date_str)


@flow(name="Sentinel Daily Pipeline")
def sentinel_pipeline(date_str: str) -> None:
    """Execute the complete ETL + anomaly flow for one date string (YYYY-MM-DD)."""
    # Build deterministic file names so each run is tied to a specific acquisition date.
    raw_file = f"raw_sentinel_{date_str}.tiff"
    transformed_file = f"transformed_data_{date_str}.parquet"

    # Resolve runtime configuration from local context and shared config values.
    local_base_dir = os.path.dirname(__file__)
    bucket_name = S3_BUCKET
    prefix = PREFIX

    # Run download -> process -> upload -> model in sequence.
    download_task(date_str, raw_file)
    process_task(raw_file, transformed_file)
    upload_task(date_str, local_base_dir, transformed_file, bucket_name)
    analyze_task(bucket_name, prefix, date_str)


def daterange(start_date: date, end_date: date):
    """Yield each date from start_date through end_date inclusive."""
    for day_offset in range((end_date - start_date).days + 1):
        yield start_date + timedelta(day_offset)


if __name__ == "__main__":
    # CLI supports one day, a date range, or defaults to today.
    parser = argparse.ArgumentParser(description="Run Sentinel daily pipeline")
    parser.add_argument("--date", type=str, help="Single date (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for range (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.date:
        # One-off run for a single acquisition day.
        sentinel_pipeline(args.date)
    elif args.start_date and args.end_date:
        # Backfill run for a contiguous date range.
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        for current_day in daterange(start, end):
            sentinel_pipeline(current_day.strftime("%Y-%m-%d"))
    else:
        # Default behavior: process current day.
        sentinel_pipeline(date.today().strftime("%Y-%m-%d"))

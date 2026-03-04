"""S3 upload helpers for parquet partitions and model plot artifacts."""

import glob
import os
import shutil

import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

from config import PREFIX

# Load AWS credentials from environment/.env for boto3 client creation.
load_dotenv()


def upload_to_s3(file_path: str, bucket_name: str, object_key: str) -> None:
    """Upload a single file to S3 and print a status message."""
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_path, bucket_name, object_key)
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{object_key}")
    except FileNotFoundError:
        print("❌ The file was not found.")
    except NoCredentialsError:
        print("❌ AWS credentials not available.")


def upload_parquet_directory_to_s3(
    date: str,
    local_base_dir: str,
    transformed_file: str,
    bucket_name: str,
) -> None:
    """Upload Spark parquet part files for one date partition and remove local output."""
    parquet_dir = os.path.join(local_base_dir, transformed_file)
    part_files = glob.glob(os.path.join(parquet_dir, "part-*.parquet"))

    if not part_files:
        raise FileNotFoundError(f"No part-*.parquet files found in {parquet_dir}")

    # Upload each Spark output shard into an S3 date partition.
    for part_file in part_files:
        object_key = f"{PREFIX}/{date}/{os.path.basename(part_file)}"
        print(f"Uploading {part_file} to s3://{bucket_name}/{object_key}...")
        upload_to_s3(part_file, bucket_name, object_key)

    # Local parquet directory is transient; clean up after successful upload loop.
    shutil.rmtree(parquet_dir)
    print("✅ Cleaned up local files")


def upload_graph_to_s3(file_path: str, bucket_name: str, date: str) -> None:
    """Upload generated analysis graph artifacts to a dedicated S3 folder."""
    if not os.path.exists(file_path):
        print(f"❌ Graph file {file_path} not found.")
        return

    object_key = f"graphs/{os.path.basename(file_path)}"
    upload_to_s3(file_path, bucket_name, object_key)
    print(f"📊 Uploaded graph {file_path} to s3://{bucket_name}/{object_key}")

import os
import glob
import boto3
from botocore.exceptions import NoCredentialsError


def upload_to_s3(file_path, bucket_name, object_key):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_path, bucket_name, object_key)
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{object_key}")
    except FileNotFoundError:
        print("❌ The file was not found.")
    except NoCredentialsError:
        print("❌ AWS credentials not available.")


def upload_parquet_directory_to_s3(date, local_base_dir, transformed_file, bucket_name):
    parquet_dir = os.path.join(local_base_dir, transformed_file)
    part_files = glob.glob(os.path.join(parquet_dir, "part-*.parquet"))

    if not part_files:
        raise FileNotFoundError(f"No part-*.parquet files found in {parquet_dir}")

    s3_prefix = f"sentinel-data/date={date}"

    for part_file in part_files:
        object_key = f"{s3_prefix}/{os.path.basename(part_file)}"
        print(f"Uploading {part_file} to s3://{bucket_name}/{object_key}...")
        upload_to_s3(part_file, bucket_name, object_key)

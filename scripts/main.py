import os
import datetime
import shutil
from sentinel_download import download_sentinel_image
from process_data import transform_red_edge_image
from s3_uploader import upload_parquet_directory_to_s3


def get_last_month_dates():
    today = datetime.date.today()
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - datetime.timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    # Generate all dates from the first to last day of previous month
    num_days = (last_day_last_month - first_day_last_month).days + 1
    return [first_day_last_month + datetime.timedelta(days=i) for i in range(num_days)]


def process_date(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    raw_file = f"raw_sentinel_{date_str}.tiff"
    transformed_file = f"transformed_data_{date_str}.parquet"

    print(f"\n📥 Downloading Sentinel image for {date_str}...")
    download_sentinel_image(date_str, raw_file)

    print(f"⚙️  Processing image and saving to {transformed_file}...")
    transform_red_edge_image(raw_file, transformed_file)

    print(f"☁️ Uploading to S3...")
    bucket_name = "ndvi-daily-data"
    local_base_dir = os.path.dirname(__file__)
    upload_parquet_directory_to_s3(date_str, local_base_dir, transformed_file, bucket_name)

    print(f"✅ Done for {date_str}!")


def main():
    dates = get_last_month_dates()
    for date_obj in dates:
        try:
            process_date(date_obj)
        except Exception as e:
            print(f"❌ Failed for {date_obj.strftime('%Y-%m-%d')}: {e}")


if __name__ == "__main__":
    main()

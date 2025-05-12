# ETL pipeline: download, clean, upload to S3
import argparse
import pandas as pd
from utils import download_csv, upload_to_s3
from config import get_cloud_config
import os

def main():
    parser = argparse.ArgumentParser(description="Cloud ETL Pipeline")
    parser.add_argument('--bucket', required=True, help='Target S3 bucket')
    parser.add_argument('--key', required=True, help='S3 key for upload')
    parser.add_argument('--region', default=None, help='AWS region')
    parser.add_argument('--endpoint_url', default=None, help='S3 endpoint URL (optional)')
    parser.add_argument('--aws_access_key_id', default=None, help='AWS access key (optional)')
    parser.add_argument('--aws_secret_access_key', default=None, help='AWS secret key (optional)')
    parser.add_argument('--source', default='https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv', help='Source CSV URL')
    parser.add_argument('--local', default='iris.csv', help='Local filename for download')
    args = parser.parse_args()

    print(f"Downloading {args.source} ...")
    download_csv(args.source, args.local)
    print(f"Downloaded to {args.local}")

    # Simple cleaning: drop NA, ensure columns
    df = pd.read_csv(args.local)
    df = df.dropna()
    print(f"Data shape after cleaning: {df.shape}")
    cleaned_file = f"cleaned_{args.local}"
    df.to_csv(cleaned_file, index=False)
    print(f"Saved cleaned data to {cleaned_file}")

    # Get cloud config (from args or env)
    cloud_cfg = get_cloud_config(
        bucket=args.bucket,
        key=args.key,
        region=args.region,
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    # Upload to S3 with dynamic config
    upload_to_s3(
        cleaned_file,
        cloud_cfg['bucket'],
        cloud_cfg['key'],
        region=cloud_cfg['region'],
        endpoint_url=cloud_cfg['endpoint_url'],
        aws_access_key_id=cloud_cfg['aws_access_key_id'],
        aws_secret_access_key=cloud_cfg['aws_secret_access_key']
    )
    print(f"Uploaded {cleaned_file} to s3://{cloud_cfg['bucket']}/{cloud_cfg['key']}")

if __name__ == "__main__":
    main()

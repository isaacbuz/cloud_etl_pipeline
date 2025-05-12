# Helper functions for ETL pipeline
import requests
import boto3

def download_csv(url, filename):
    r = requests.get(url)
    r.raise_for_status()
    with open(filename, 'wb') as f:
        f.write(r.content)

def upload_to_s3(filename, bucket, key, region=None, endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None):
    s3 = boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    with open(filename, 'rb') as f:
        s3.upload_fileobj(f, bucket, key)

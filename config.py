import os

def get_cloud_config(
    bucket=None, key=None, region=None, endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None
):
    return {
        "bucket": bucket or os.getenv("AWS_S3_BUCKET", "your-s3-bucket"),
        "key": key or os.getenv("AWS_S3_KEY", "cleaned_iris.csv"),
        "region": region or os.getenv("AWS_REGION", "us-east-1"),
        "endpoint_url": endpoint_url or os.getenv("AWS_S3_ENDPOINT", None),
        "aws_access_key_id": aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID", None),
        "aws_secret_access_key": aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY", None),
    }

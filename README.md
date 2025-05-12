# Cloud-Integrated Data Pipeline

This project demonstrates a Python-based ETL pipeline that downloads, cleans, and uploads data to AWS S3.

## Features
- Download a public dataset (configurable)
- Clean and preprocess data with pandas
- Upload processed data to AWS S3 using boto3
- (Optional) Automate with shell script or Lambda

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure AWS credentials (see below)
3. Run the ETL pipeline:
   ```bash
   python etl.py --bucket <your-s3-bucket> --key <s3-key> [--source <url>] [--local <filename>]
   ```

## AWS Credentials
- Set up your AWS credentials using `aws configure` or environment variables.
- The script uses boto3 to upload to S3.

## Extending
- Swap out the data source or add more transformations.
- Automate with cron, shell, or AWS Lambda.

## License
MIT License

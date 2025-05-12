import streamlit as st
import pandas as pd
from utils import download_csv, upload_to_s3
from config import get_cloud_config
import os

st.set_page_config(page_title="Cloud ETL Pipeline", layout="centered")
st.title("☁️ Cloud ETL Pipeline Panel")

# Default values
default_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
default_local = "iris.csv"
default_cleaned = "cleaned_iris.csv"

def cloud_config_inputs():
    st.sidebar.header("ETL Settings")
    source_url = st.sidebar.text_input("Source CSV URL", value=default_url)
    local_filename = st.sidebar.text_input("Local Filename", value=default_local)
    cleaned_filename = st.sidebar.text_input("Cleaned Filename", value=default_cleaned)
    s3_bucket = st.sidebar.text_input("AWS S3 Bucket", value="your-s3-bucket")
    s3_key = st.sidebar.text_input("S3 Key (path in bucket)", value="cleaned_iris.csv")
    region = st.sidebar.text_input("AWS Region", value="us-east-1")
    endpoint_url = st.sidebar.text_input("S3 Endpoint URL (optional)", value="")
    aws_access_key_id = st.sidebar.text_input("AWS Access Key ID (optional)", value="")
    aws_secret_access_key = st.sidebar.text_input("AWS Secret Access Key (optional)", value="", type="password")
    run_etl = st.sidebar.button("Run ETL Pipeline")
    return dict(
        source_url=source_url,
        local_filename=local_filename,
        cleaned_filename=cleaned_filename,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        region=region or None,
        endpoint_url=endpoint_url or None,
        aws_access_key_id=aws_access_key_id or None,
        aws_secret_access_key=aws_secret_access_key or None,
        run_etl=run_etl
    )

cfg = cloud_config_inputs()

if cfg['run_etl']:
    try:
        st.info(f"Downloading {cfg['source_url']} ...")
        download_csv(cfg['source_url'], cfg['local_filename'])
        st.success(f"Downloaded to {cfg['local_filename']}")
        df = pd.read_csv(cfg['local_filename'])
        df = df.dropna()
        df.to_csv(cfg['cleaned_filename'], index=False)
        st.success(f"Cleaned and saved to {cfg['cleaned_filename']}")
        cloud_cfg = get_cloud_config(
            bucket=cfg['s3_bucket'],
            key=cfg['s3_key'],
            region=cfg['region'],
            endpoint_url=cfg['endpoint_url'],
            aws_access_key_id=cfg['aws_access_key_id'],
            aws_secret_access_key=cfg['aws_secret_access_key']
        )
        upload_to_s3(
            cfg['cleaned_filename'],
            cloud_cfg['bucket'],
            cloud_cfg['key'],
            region=cloud_cfg['region'],
            endpoint_url=cloud_cfg['endpoint_url'],
            aws_access_key_id=cloud_cfg['aws_access_key_id'],
            aws_secret_access_key=cloud_cfg['aws_secret_access_key']
        )
        st.success(f"Uploaded {cfg['cleaned_filename']} to s3://{cloud_cfg['bucket']}/{cloud_cfg['key']}")
        st.subheader("Preview of Cleaned Data")
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"ETL failed: {e}")

st.markdown("---")
st.markdown("**Instructions:**\n1. Enter the source CSV URL, local filename, S3 bucket, key, region, and credentials.\n2. Click 'Run ETL Pipeline' to download, clean, and upload the data.\n3. AWS credentials can also be set up via environment variables or AWS CLI.")

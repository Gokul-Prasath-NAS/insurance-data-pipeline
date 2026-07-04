"""
ingest_raw_data.py
------------------
Ingestion layer of the Insurance Data Pipeline.

Downloads the public US insurance dataset and lands it in MinIO (S3 equivalent) 
under the raw bucket, with a timestamped filename so every run is traceable.

AWS equivalent: AWS Glue ingestion job triggered by EventBridge
"""

import os
import sys
import logging
from datetime import datetime, timezone

import requests
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configure logging (CloudWatch Logs equivalent)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ingest_raw_data")

def get_minio_client():
    """
    Creates a boto3 S3 client pointed at our local MinIO instance. 
    This is the SAME boto3 client code used to talk to real AWS S3 - only the endpoint_url is different. 
    In production, just remove endpoint_url and this code talks to actual Amazon S3 unchanged.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def download_dataset(url: str) -> bytes:
    """Downloads the raw CSV file from the public source."""
    logger.info(f"Downloading dataset from: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # raises an error if the download failed
    logger.info(f"Downloaded {len(response.content)} bytes")
    return response.content

def upload_to_minio(client, bucket: str, key: str, data: bytes):
    """Uploads raw bytes to a MinIO bucket - identical to an S3 put_object call."""
    client.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info(f"Uploaded to s3://{bucket}/{key}")

def main():
    raw_data_url = os.getenv("RAW_DATA_URL")
    bucket_raw = os.getenv("MINIO_BUCKET_RAW", "insurance-raw")

    if not raw_data_url:
        logger.error("RAW_DATA_URL not set in env file")
        sys.exit(1)

    # Timestamped filename e.g. insurance_20260624_143000.csv
    # Keeps every run's data separate and traceable, never overwritten
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    object_key = f"insurance_{timestamp}.csv"

    try:
        data = download_dataset(raw_data_url)
        client = get_minio_client()
        upload_to_minio(client, bucket_raw, object_key, data)
        logger.info("Ingestion completed successfully")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download dataset: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
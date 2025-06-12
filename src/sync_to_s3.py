"""
sync_to_s3.py

This script synchronizes local directories containing ZTF alert and
image data to an S3 bucket.
It uploads only new or changed files by comparing local MD5 hashes
with S3 ETags, and logs all actions.
Intended for use in automated workflows such as Airflow DAGs.
"""

import os
import boto3
import hashlib
import logging
from botocore.exceptions import NoCredentialsError, ClientError
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


load_dotenv('/opt/airflow/.env')


# === CONFIG ===
BUCKET_NAME = 'ztf-pipeline-data'
LOCAL_DIRECTORIES = [
    ('/opt/airflow/data/alerts_partitioned', 'alerts_partitioned'),
    ('/opt/airflow/images/by_date', 'images/by_date')
]
LOG_FILE = '/opt/airflow/custom_logs/sync_to_s3.log'

# === SETUP LOGGING ===
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)

s3 = boto3.client('s3')

def md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def s3_etag_matches(local_path, bucket, key):
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        etag = response['ETag'].strip('"')
        return etag == md5(local_path)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def upload_directory(local_dir, s3_prefix):
    local_dir = Path(local_dir)
    for file_path in local_dir.rglob('*'):
        if file_path.is_file():
            s3_key = os.path.join(s3_prefix, file_path.relative_to(local_dir).as_posix())
            if s3_etag_matches(file_path, BUCKET_NAME, s3_key):
                #logging.info(f"⏭️ Skipped (unchanged): {file_path}")
                continue
            try:
                s3.upload_file(str(file_path), BUCKET_NAME, s3_key)
                logging.info(f"✅ Uploaded: {file_path} -> s3://{BUCKET_NAME}/{s3_key}")
            except NoCredentialsError:
                logging.error("❌ AWS credentials not found.")
                return
            except Exception as e:
                logging.error(f"❌ Failed to upload {file_path}: {e}")

def main():
    logging.info("=" * 60)
    logging.info("🕒 Starting sync cycle")
    for local_path, s3_prefix in LOCAL_DIRECTORIES:
        if os.path.exists(local_path):
            logging.info(f"🔄 Syncing directory: {local_path}")
            upload_directory(local_path, s3_prefix)
        else:
            logging.warning(f"⚠️ Skipped missing path: {local_path}")
    logging.info("✅ Sync cycle complete")
    logging.info("=" * 60)
    logging.info("\n")
if __name__ == "__main__":
    main()

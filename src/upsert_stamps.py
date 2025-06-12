import os
import re
import boto3
import pandas as pd
from datetime import datetime
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

def run_upsert():
    load_dotenv()

    # === CONFIG ===
    bucket = "ztf-pipeline-data"
    local_base_dir = Path("images/by_date")
    s3_prefix = "manifests/stamps"
    manifest_name = f"ztf_stamps_manifest.csv"
    s3_key = f"{s3_prefix}/{manifest_name}"

    snowflake_config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE")
    }

    # === STAMP EXTRACTION ===
    stamp_re = re.compile(r"([A-Z0-9]+)_(science|template|difference)\.png", re.IGNORECASE)

    rows = []
    for date_dir in local_base_dir.iterdir():
        if not date_dir.is_dir():
            continue
        alert_date = date_dir.name
        for file_path in date_dir.glob("*.png"):
            match = stamp_re.match(file_path.name)
            if match:
                object_id, stamp_type = match.groups()
                s3_path = f"s3://{bucket}/images/by_date/{alert_date}/{file_path.name}"
                rows.append((object_id, stamp_type, alert_date, s3_path))

    
    df = pd.DataFrame(rows, columns=["object_id", "stamp_type", "alert_date", "s3_path"])
    df["alert_date"] = pd.to_datetime(df["alert_date"])
    df.to_csv(manifest_name, index=False)

    print(f"[INFO] Found {len(df)} stamps. Manifest saved as: {manifest_name}")

    # === UPLOAD TO S3 ===
    s3 = boto3.client("s3")
    s3.upload_file(manifest_name, bucket, s3_key)
    print(f"[INFO] Uploaded manifest to s3://{bucket}/{s3_key}")

    # === LOAD INTO SNOWFLAKE STAGING TABLE ===
    conn = snowflake.connector.connect(**snowflake_config)
    cs = conn.cursor()
    cs.execute("USE DATABASE ztf_data;")
    cs.execute("USE SCHEMA public;")

    copy_sql = f"""
    COPY INTO ztf_stamps_stage (object_id, stamp_type, alert_date, s3_path)
    FROM @ztf_stage/manifests/stamps/{manifest_name}
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);
    """

    print("[INFO] Copying data into ztf_stamps_stage...")
    cs.execute(copy_sql)

    # === MERGE INTO FINAL TABLE ===
    print("[INFO] Merging data into ztf_stamps...")
    merge_sql = """
    MERGE INTO ztf_stamps AS target
    USING ztf_stamps_stage AS source
    ON target.object_id = source.object_id
    AND target.stamp_type = source.stamp_type
    AND target.alert_date = source.alert_date
    WHEN MATCHED THEN UPDATE SET target.s3_path = source.s3_path
    WHEN NOT MATCHED THEN INSERT (object_id, stamp_type, alert_date, s3_path)
    VALUES (source.object_id, source.stamp_type, source.alert_date, source.s3_path);
    """
    cs.execute(merge_sql)

    print("[INFO] Upsert complete.")

    cs.close()
    conn.close()

if __name__ == "__main__":
    run_upsert()

import boto3
import re
import pandas as pd
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()


# CONFIG
bucket = "ztf-pipeline-data"
prefix = "images/by_date/"
snowflake_config = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE")
}


# SETUP
s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")
stamp_re = re.compile(r"by_date/(\d{4}-\d{2}-\d{2})/([A-Z0-9]+)_(science|template|difference)\.png")

rows = []
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        match = stamp_re.search(key)
        if match:
            alert_date, object_id, stamp_type = match.groups()
            s3_path = f"s3://{bucket}/{key}"
            rows.append((object_id, stamp_type, alert_date, s3_path))

df = pd.DataFrame(rows, columns=["object_id", "stamp_type", "alert_date", "s3_path"])
df["alert_date"] = pd.to_datetime(df["alert_date"])

# UPSERT INTO SNOWFLAKE
conn = snowflake.connector.connect(**snowflake_config)
cs = conn.cursor()
cs.execute("USE DATABASE ztf_data;")
cs.execute("USE SCHEMA public;")

for row in df.itertuples(index=False):
    cs.execute("""
        MERGE INTO ztf_stamps AS target
        USING (SELECT %s AS object_id, %s AS stamp_type, %s AS alert_date, %s AS s3_path) AS source
        ON target.object_id = source.object_id AND target.stamp_type = source.stamp_type AND target.alert_date = source.alert_date
        WHEN MATCHED THEN UPDATE SET s3_path = source.s3_path
        WHEN NOT MATCHED THEN INSERT (object_id, stamp_type, alert_date, s3_path)
        VALUES (source.object_id, source.stamp_type, source.alert_date, source.s3_path)
    """, row)

cs.close()
conn.close()

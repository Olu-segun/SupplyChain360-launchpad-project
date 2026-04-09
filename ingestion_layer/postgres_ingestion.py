import json
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

from utils.credentials import get_db_engine, get_destination_s3_client

# Configuration

BUCKET = "supplychain360-data-lake"
TARGET_PREFIX = "raw/store_sales_transactions/"
STATE_FILE_KEY = "metadata/_processed_pg_sales.json"


# Logger

logger = LoggingMixin().log


# s3 Client

s3 = get_destination_s3_client()


# Retry Wrapper Logic


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def s3_get_object(bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def s3_put_object(bucket, key, body):
    return s3.put_object(Bucket=bucket, Key=key, Body=body)


# State Management


def load_processed_tables():
    try:
        response = s3_get_object(BUCKET, STATE_FILE_KEY)
        data = json.loads(response["Body"].read())
        return set(data)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No state file found, starting fresh.")
            return set()
        else:
            logger.warning(f"Error loading state: {e}")
            return set()
    except Exception as e:
        logger.warning(f"Unexpected error loading state: {e}")
        return set()


def save_processed_tables(processed):
    try:
        body = json.dumps(list(processed))
        s3_put_object(BUCKET, STATE_FILE_KEY, body)
    except Exception as e:
        logger.error(f"Error saving state: {e}")


# Extract, Transform, Load Functions
# ----------------------------
def extract_table_to_s3(table_name, engine):
    logger.info(f"Extracting {table_name}...")

    query = f'SELECT * FROM public."{table_name}"'

    # Use context manager to ensure connection closes after query execution
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    # Convert UUID/object columns to string safely to avoid Parquet issues
    for col in df.select_dtypes(include=["object", "string"]).columns:
        if not df[col].apply(lambda x: isinstance(x, (str, bytes)) or pd.isna(x)).all():
            df[col] = df[col].astype(str)

    # Add ingestion timestamp for partitioning and auditing
    df["ingestion_timestamp"] = datetime.now(timezone.utc)

    # Write to Parquet in memory
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    target_key = f"{TARGET_PREFIX}{table_name}.parquet"
    s3_put_object(BUCKET, target_key, buffer.getvalue())

    logger.info(f"Saved {table_name} to s3://{BUCKET}/{target_key}")


# Main Pipeline Function


def postgres_ingestion_pipeline():
    logger.info("Start ingesting data from Postgres database to AWS s3 bucket...")

    processed = load_processed_tables()

    # Daily tables (incremental load)
    tables = [
        "sales_2026_03_10",
        "sales_2026_03_11",
        "sales_2026_03_12",
        "sales_2026_03_13",
        "sales_2026_03_14",
        "sales_2026_03_15",
        "sales_2026_03_16",
    ]

    new_tables = [t for t in tables if t not in processed]
    logger.info(f"{len(new_tables)} new tables to process")

    # Create engine once, but close connections per query
    engine = get_db_engine()

    for table in new_tables:
        try:
            extract_table_to_s3(table, engine)
            processed.add(table)
        except Exception as e:
            logger.error(f"Failed processing {table}: {e}")

    save_processed_tables(processed)
    logger.info("Pipeline completed successfully.")

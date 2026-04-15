import gc
import json
import time
from io import BytesIO
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.credentials import get_destination_s3_client, get_source_s3_client


# Configuration
SOURCE_BUCKET = "supplychain360-data"
TARGET_BUCKET = "supplychain360-data-lake"

MAX_WORKERS = 4
SLEEP_TIME = 0.1

FOLDER_MAPPING = {
    "raw/inventory/": "raw/warehouse_inventory/",
    "raw/products/": "raw/product_catalog_master/",
    "raw/shipments/": "raw/shipment_delivery_logs/",
    "raw/suppliers/": "raw/supplier_registry_data/",
    "raw/warehouses/": "raw/warehouse_master_data/",
}

STATE_FILE_KEY = "metadata/_processed_files.json"


# Logger

logger = LoggingMixin().log


# AWS Clients

source_s3 = get_source_s3_client()
destination_s3 = get_destination_s3_client()


# State Management


def load_processed_files():
    try:
        response = destination_s3.get_object(Bucket=TARGET_BUCKET, Key=STATE_FILE_KEY)
        return set(json.loads(response["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No state file found. Starting fresh.")
            return set()
        raise


def save_processed_files(processed_files):
    body = json.dumps(list(processed_files))
    destination_s3.put_object(Bucket=TARGET_BUCKET, Key=STATE_FILE_KEY, Body=body)


# List files in S3 with pagination and filter for CSV/JSON


def list_files(prefix):
    paginator = source_s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix)

    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv") or key.endswith(".json"):
                files.append(key)
    return files


# Memory logging for debugging purposes


def log_memory():
    try:
        import os

        import psutil

        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / 1024**2
        logger.info(f"Memory usage: {mem:.2f} MB")
    except:
        pass


# Process single file: read, transform, write to S3


def process_file(key, target_prefix):
    logger.info(f"[START] Processing {key}")
    response = source_s3.get_object(Bucket=SOURCE_BUCKET, Key=key)

    file_name = key.split("/")[-1]
    file_name = file_name.replace(".csv", ".parquet").replace(".json", ".parquet")
    target_key = f"{target_prefix}{file_name}"

    try:

        # Read file into DataFrame based on extension

        if key.endswith(".csv"):
            df = pd.read_csv(response["Body"])
        elif key.endswith(".json"):
            df = pd.read_json(response["Body"])
        else:
            raise ValueError("Unsupported format")

        # Add metadata columns for traceability and debugging

        df["ingestion_timestamp"] = datetime.now(timezone.utc)
        df["source_file"] = key

        # Convert to Arrow Table for efficient Parquet writing and to handle complex data types better
        table = pa.Table.from_pandas(df)

        # Write to buffer in Parquet format using PyArrow for better performance and compatibility with complex data types
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)

        # Upload to S3 as Parquet file
        destination_s3.put_object(
            Bucket=TARGET_BUCKET, Key=target_key, Body=buf.getvalue().to_pybytes()
        )

        logger.info(f"[SUCCESS] Saved → {target_key}")
        log_memory()

    except Exception as e:
        logger.error(f"[FAILED] {key}: {e}")
        raise
    finally:
        gc.collect()


# Main Pipeline Function
def s3_ingestion_pipeline():
    logger.info("Starting S3 ingestion pipeline...")

    processed_files = load_processed_files()

    for source_prefix, target_prefix in FOLDER_MAPPING.items():
        logger.info(f"Scanning {source_prefix}")

        all_files = list_files(source_prefix)
        new_files = [f for f in all_files if f not in processed_files]

        logger.info(f"{len(new_files)} new files")

        if not new_files:
            continue

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_file, key, target_prefix): key
                for key in new_files
            }

            for future in as_completed(futures):
                key = futures[future]
                try:
                    future.result()
                    processed_files.add(key)
                except Exception as e:
                    logger.error(f"Failed: {key} -> {e}")
                time.sleep(SLEEP_TIME)

    save_processed_files(processed_files)
    logger.info("Pipeline completed successfully.")
    
s3_ingestion_pipeline()

import json
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timezone
from googleapiclient.discovery import build
from airflow.utils.log.logging_mixin import LoggingMixin
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.credentials import get_google_service_account_credentials


# Configuration
S3_BUCKET = "supplychain360-data-lake"
TARGET_PREFIX = "raw/retail_store_locations/"
STATE_FILE_KEY = "metadata/retail_store_locations_state.json"

SPREADSHEET_ID = "1r5VtrdRiW-5AmX-_GG-IXuVLzptwTTWb9_RC4A_dhgg"
RANGE_NAME = "Sheet1!A:F"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


# Logger
logger = LoggingMixin().log


# AWS Client
s3 = boto3.client("s3")


# Retry Wrapper Logic
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def s3_get_object(bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def s3_put_object(bucket, key, body):
    return s3.put_object(Bucket=bucket, Key=key, Body=body)


# State Management
def load_state():
    try:
        response = s3_get_object(S3_BUCKET, STATE_FILE_KEY)
        return json.loads(response["Body"].read())
    except Exception:
        logger.info("No existing state found. Starting fresh data ingestion.")
        return {"last_processed_date": None}


def save_state(state):
    s3_put_object(S3_BUCKET, STATE_FILE_KEY, json.dumps(state))


# Google Sheet Data Extraction


def fetch_google_sheet_data(creds):
    logger.info("Fetching data from Google Sheets")

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    result = (
        sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    )

    values = result.get("values", [])
    if not values:
        logger.warning("No data found in Google Sheets")
        return pd.DataFrame()

    # First row = header -> columns, rest = data
    return pd.DataFrame(values[1:], columns=values[0])


# Transformation and Incremental Load


def transform_data(df, last_processed_date):
    if df.empty:
        return df

    logger.info("Transforming data")
    df["store_open_date"] = pd.to_datetime(
        df["store_open_date"], dayfirst=True, errors="coerce"
    )

    if last_processed_date:
        last_date = pd.to_datetime(last_processed_date)
        df = df[df["store_open_date"] > last_date]
        logger.info(f"Filtered incremental records: {len(df)} rows")

    return df


# Write to s3 Bucket in Parquet format
def write_to_s3(df):
    if df.empty:
        logger.info("No new data to write")
        return None

    ingestion_time = datetime.now(timezone.utc)
    file_name = f"retail_store_locations_{ingestion_time.strftime('%Y%m%d')}.parquet"
    s3_key = f"{TARGET_PREFIX}{file_name}"

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3_put_object(S3_BUCKET, s3_key, buffer.getvalue())
    logger.info(f"Data written to {s3_key}")

    return df["store_open_date"].max().strftime("%Y-%m-%d")


# Main Pipeline Function
def google_sheet_ingestion_pipeline():
    logger.info("Starting Google Sheets ingestion pipeline")

    # Load state for incremental processing
    state = load_state()
    last_processed_date = state.get("last_processed_date")

    # Fetch credentials once and reuse for all API calls
    creds = get_google_service_account_credentials(scopes=SCOPES)

    # Extract data from Google Sheets
    df = fetch_google_sheet_data(creds)

    # Transform (incremental) and add metadata
    df = transform_data(df, last_processed_date)

    if not df.empty:
        df["ingestion_timestamp"] = datetime.now(timezone.utc)

    # Load to S3 and get new max date for state update
    new_max_date = write_to_s3(df)

    # Update state (idempotency) only if we have a new max date to avoid overwriting with same state
    if new_max_date:
        state["last_processed_date"] = new_max_date
        save_state(state)
        logger.info(f"Updated state: {state}")

    logger.info("Pipeline completed successfully")

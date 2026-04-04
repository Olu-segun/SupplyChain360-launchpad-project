import boto3
import os
import json
import logging
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials

REGION = "eu-west-2"


# Logger setup

logger = logging.getLogger(__name__)


# Singletons for AWS Clients and Google Credentials to optimize resource usage

_session = None
_ssm_client = None
_source_s3 = None
_destination_s3 = None
_logged_credentials = False

def get_boto3_session(region=REGION):
    global _session, _logged_credentials

    if _session is not None:
        return _session

    try:
        profile = os.getenv("AWS_PROFILE")

        if profile:
            if not _logged_credentials:
                logger.info(f"Using AWS profile: {profile}")
                _logged_credentials = True

            _session = boto3.Session(profile_name=profile, region_name=region)
        else:
            if not _logged_credentials:
                logger.info("Using default AWS credential.")
                _logged_credentials = True

            _session = boto3.Session(region_name=region)

        return _session

    except Exception as e:
        logger.error(f"Failed to create boto3 session: {e}")
        raise



# SSM Client Singleton 

def get_ssm_client(region=REGION):
    global _ssm_client

    if _ssm_client is None:
        _ssm_client = get_boto3_session(region).client("ssm")

    return _ssm_client



# Source S3 Bucket Credentials From AWS SSM

def get_source_s3_client(region=REGION):
    global _source_s3

    if _source_s3 is not None:
        return _source_s3

    ssm = get_ssm_client(region)

    access_key = ssm.get_parameter(Name="/source/aws/access_key")["Parameter"]["Value"]
    secret_key = ssm.get_parameter(Name="/source/aws/secret_key")["Parameter"]["Value"]

    _source_s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

    return _source_s3


# Destination S3 Bucket Credentials From AWS SSM

def get_destination_s3_client(region=REGION):
    global _destination_s3

    if _destination_s3 is not None:
        return _destination_s3

    ssm = get_ssm_client(region)

    access_key = ssm.get_parameter(Name="/destination/aws/access_key")["Parameter"]["Value"]
    secret_key = ssm.get_parameter(Name="/destination/aws/secret_key")["Parameter"]["Value"]

    _destination_s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

    return _destination_s3



# Copy Object between S3 Buckets.

def copy_object(source_bucket, source_key, dest_bucket, dest_key, region=REGION):
    source_s3 = get_source_s3_client(region)
    destination_s3 = get_destination_s3_client(region)

    obj = source_s3.get_object(Bucket=source_bucket, Key=source_key)
    destination_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=obj["Body"].read())



# Postgress Database Credentials From AWS SSM and SQLAlchemy Engine Creation

def get_db_engine(region=REGION, connect_args=None):
    ssm = get_ssm_client(region)

    host = ssm.get_parameter(Name="/supplychain360/db/host")["Parameter"]["Value"].strip()
    port = ssm.get_parameter(Name="/supplychain360/db/port")["Parameter"]["Value"].strip()
    user = ssm.get_parameter(Name="/supplychain360/db/user")["Parameter"]["Value"].strip()
    password = ssm.get_parameter(Name="/supplychain360/db/password")["Parameter"]["Value"].strip()
    database = ssm.get_parameter(Name="/supplychain360/db/dbname")["Parameter"]["Value"].strip()

    default_connect_args = {
        "connect_timeout": 30,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=0",
    }

    if connect_args:
        default_connect_args.update(connect_args)

    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require",
        connect_args=default_connect_args,
        pool_pre_ping=True,
        pool_recycle=1800,
    )



# Google Credentials From AWS SSM for Google Sheets API Access

def get_google_service_account_credentials(param_name="google_sheet_api", scopes=None, region=REGION):
    ssm = get_ssm_client(region)

    logger.info(f"Fetching Google service account JSON from SSM: {param_name}")

    response = ssm.get_parameter(Name=param_name)
    service_account_json = response["Parameter"]["Value"]

    info = json.loads(service_account_json)

    return Credentials.from_service_account_info(info, scopes=scopes)
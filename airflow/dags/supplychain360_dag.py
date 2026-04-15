import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from tasks.dbt_tasks import run_dbt
from tasks.ingestion_tasks import create_ingestion_group
from tasks.snowflake_tasks import snowflake_copy_tasks
from tasks.success_mail_alert import dag_success_alert
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Create Airflow DAG for SupplyChain360 Data Pipeline with Ingestion Tasks, Snowflake Copy Tasks, and DBT Transformations.

default_args = {
    "owner": "olukayode_olusegun",
    "email": ["olukayodeoluseguno@gmail.com"],
    "email_on_failure": True,
    "on_success_callback": dag_success_alert,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}
with DAG(
    dag_id="supplychain360_dag",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 3, 24),
    catchup=False,
    tags=["s3", "postgres", "google_sheet"],
) as dag:

    ingestion_group = create_ingestion_group(dag)
    snowflake_copy = snowflake_copy_tasks(dag)
    dbt_transform = run_dbt

    ingestion_group >> snowflake_copy >> dbt_transform

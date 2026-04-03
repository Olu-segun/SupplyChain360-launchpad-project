import sys
import os
from airflow import DAG
from datetime import datetime, timedelta
from tasks.ingestion_tasks import create_ingestion_group
from tasks.snowflake_tasks import snowflake_copy_tasks
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

default_args = {
    "owner": "olukayode_olusegun",
    "email": ["olukayodeoluseguno@gmail.com"],
    "email_on_failure": False,
    "email_on_success": False,
    "email_on_retry": False,
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

    
    
    ingestion_group >> snowflake_copy
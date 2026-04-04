from airflow import DAG
from datetime import datetime, timedelta
from tasks.ingestion_tasks import create_ingestion_group
from tasks.snowflake_tasks import snowflake_copy_tasks

# Create Airflow DAG for SupplyChain360 Data Pipeline with Ingestion and Snowflake Copy Tasks.

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
    
    """Create Ingestion Tasks for S3, Postgres, and Google Sheets. 
     Each task will call the respective ingestion pipeline function 
    defined in the ingestion_layer modules."""

    ingestion_group = create_ingestion_group(dag)
    snowflake_copy = snowflake_copy_tasks(dag)

    
    ingestion_group >> snowflake_copy
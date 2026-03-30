from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

default_args = {
    "owner": "olukayode_olusegun",
    "email": ["olukayodeoluseguno@gmail.com"],
    "email_on_failure": True,
    "email_on_success": True,
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
    
# -----------------------------
# Lazy wrapper functions
# -----------------------------
    def run_s3_pipeline():
        from ingestion_layer.s3_ingestion import s3_ingestion_pipeline
        s3_ingestion_pipeline()


    def run_postgres_pipeline():
        from ingestion_layer.postgres_ingestion import postgres_ingestion_pipeline
        postgres_ingestion_pipeline()
        
    def run_sheet_pipeline():
        from ingestion_layer.google_sheet_ingestion import google_sheet_ingestion_pipeline
        google_sheet_ingestion_pipeline()



    s3_pipeline_task = PythonOperator(
        task_id="run_s3_ingestion_pipeline",
        python_callable=run_s3_pipeline,
    )

    postgres_pipeline_task = PythonOperator(
        task_id="run_postgres_ingestion_pipeline",
        python_callable=run_postgres_pipeline,
    )

    sheet_pipeline_task = PythonOperator(
        task_id="run_sheet_ingestion_pipeline",
        python_callable=run_sheet_pipeline,
    )
    
    s3_pipeline_task >> postgres_pipeline_task >> sheet_pipeline_task
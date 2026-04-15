import sys
import os
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

""" 
Create Ingestion Tasks for S3, Postgres, and Google Sheets. 
Each task will call the respective ingestion pipeline function 
defined in the ingestion_layer modules.
                                        """


def run_s3_pipeline():
    from ingestion_layer.s3_ingestion import s3_ingestion_pipeline
    s3_ingestion_pipeline()


def run_postgres_pipeline():
    from ingestion_layer.postgres_ingestion import postgres_ingestion_pipeline
    postgres_ingestion_pipeline()


def run_sheet_pipeline():
    from ingestion_layer.google_sheet_ingestion import google_sheet_ingestion_pipeline
    google_sheet_ingestion_pipeline()


def create_ingestion_group(dag):
    with TaskGroup("ingestion_tasks", dag=dag) as group:

        s3_ingestion_task = PythonOperator(
            task_id="run_s3_ingestion_pipeline",
            python_callable=run_s3_pipeline,
        )

        postgres_ingestion_task = PythonOperator(
            task_id="run_postgres_ingestion_pipeline",
            python_callable=run_postgres_pipeline,
        )

        sheet_ingestion_task = PythonOperator(
            task_id="run_sheet_ingestion_pipeline",
            python_callable=run_sheet_pipeline,
        )

        s3_ingestion_task >> postgres_ingestion_task >> sheet_ingestion_task

    return group

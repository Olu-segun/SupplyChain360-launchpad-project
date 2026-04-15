from airflow.utils.email import send_email

def dag_success_alert(context):
    send_email(
        to=["olukayodeoluseguno@gmail.com"],
        subject="SupplyChain360 DAG Success",
        html_content=f"DAG {context['dag'].dag_id} completed successfully on {context['execution_date']}.",
    )

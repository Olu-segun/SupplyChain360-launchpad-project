from airflow.providers.standard.operators.bash import BashOperator

run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command="cd /opt/airflow/dbt && dbt build",
)
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator

tables = {
    "store_locations": "retail_store_locations/",
    "products": "product_catalog_master/",
    "shipments": "shipment_delivery_logs/",
    "sales": "store_sales_transactions/",
    "suppliers": "supplier_registry_data/",
    "inventory": "warehouse_inventory/",
    "warehouses": "warehouse_master_data/"
}

def snowflake_copy_tasks(dag):
    tasks = []

    for table, path in tables.items():
        task = SQLExecuteQueryOperator(
            task_id=f"copy_{table}",
            sql=f"""
            COPY INTO supplychain360_db.raw.{table}
            FROM @supplychain360_db.raw.supplychain360_s3_stage/{path}
            FILE_FORMAT = (TYPE = PARQUET);
            """,
            conn_id="snowflake_conn",
            dag=dag
        )
        tasks.append(task)

    return tasks
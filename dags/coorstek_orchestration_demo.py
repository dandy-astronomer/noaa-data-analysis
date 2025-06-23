from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from fivetran_provider_async.operators import FivetranOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.providers.http.operators.http import HttpOperator 

# Replace with your actual values
FIVETRAN_CONNECTOR_ID = "gloater_outer"
DATABRICKS_JOB_ID = 1123532341565253

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="orchestrate_fivetran_databricks_adf",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Demo DAG for orchestrating Fivetran, Databricks, and ADF",
    tags=["demo", "astronomer"],
) as dag:

    start = EmptyOperator(task_id="start")

    fivetran_sync = FivetranOperator(
        task_id="fivetran_sync",
        connector_id=FIVETRAN_CONNECTOR_ID,
        fivetran_conn_id="fivetran_conn"
    )

    databricks_task = DatabricksSqlOperator(
        task_id="run_databricks_sql",
        databricks_conn_id="databricks_default",
        #you just need to know the task ID and conn ID and then you can perform SQL as usual
        sql="SELECT current_timestamp() AS airflow_triggered_time;"
    )

    trigger_adf = HttpOperator(
        task_id="simulate_adf_pipeline",
        http_conn_id="http_default",
        endpoint="get",
        method="GET",
        log_response=True,
    )

    notify_success = EmptyOperator(
        task_id="notify_success",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    notify_failure = EmptyOperator(
        task_id="notify_failure",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # DAG structure
    start >> fivetran_sync >> databricks_task >> trigger_adf
    [fivetran_sync, databricks_task, trigger_adf] >> notify_success
    [fivetran_sync, databricks_task, trigger_adf] >> notify_failure
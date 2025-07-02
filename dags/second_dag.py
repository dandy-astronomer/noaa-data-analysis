from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow import Dataset
from datetime import datetime

dataset1 = Dataset("s3://dataset1/output_1.txt")
dataset2 = Dataset("s3://dataset1/output_2.txt")

@dag(
    dag_id="second_dag",
    start_date=datetime(2025, 1, 1),
    schedule=[Dataset("s3://dataset1/output_1.txt")],
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
)
def second_dag():
 
    task_1 = BashOperator(
        task_id="Extract",
        bash_command="echo 'Extracting'",
        retries=1,
    )

    task_2 = BashOperator(
        task_id="Transform",
        bash_command="echo 'Transforming'",
        retries=1,
    )

    task_3 = BashOperator(
        task_id="Load",
        bash_command="echo 'Updating dataset'",
        outlets=[dataset2],  
        retries=1,
    )

    task_1 >> task_2 >> task_3

second_dag()
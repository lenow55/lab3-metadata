# dags/test_debug.py
from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator


def debug_task():
    print("Start dag")
    x = 2 + 2
    print(f"x={x}")


with DAG(
    "debug_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    _ = PythonOperator(
        task_id="debug_me",
        python_callable=debug_task,
    )

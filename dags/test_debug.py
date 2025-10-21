from datetime import UTC, datetime
from pprint import pprint
from typing import Any

from airflow.sdk import dag, task  # pyright: ignore[reportUnknownVariableType]


@task(task_id="print_the_context")
def debug_task(ds: Any = None, **kwargs) -> str:
    print("Start dag")
    x = 2 + 2
    print(f"x={x}")
    print(ds)
    pprint(kwargs)
    return "Whatever you return gets printed in the logs"


@dag(
    dag_id="debug_example",
    schedule=None,
    start_date=datetime(2021, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["debug"],
)
def example_debug_dag():
    run_this = debug_task()

    run_this


debug_dag = example_debug_dag()

from datetime import UTC, datetime
from pprint import pprint
from typing import Any

from airflow.sdk import dag, task  # pyright: ignore[reportUnknownVariableType]


@task(task_id="task_1")
def debug_task(ds: Any = None, **kwargs) -> str:
    print("Start task")
    x = 2 + 2
    print(f"x={x}")
    print(ds)
    pprint(kwargs)
    return "Whatever you return gets printed in the logs"


@task(task_id="task_2")
def debug_task2(ds: Any = None, **kwargs) -> str:
    print("Start task2")
    x = 2 + 2
    print(f"x={x}")
    print(ds)
    pprint(kwargs)
    return "Whatever you return gets printed in the logs 2"


@dag(
    dag_id="debug_example",
    schedule=None,
    start_date=datetime(2021, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["debug"],
)
def example_debug_dag():
    run_this = debug_task()
    run_2_this = debug_task2()

    run_this >> run_2_this


debug_dag = example_debug_dag()

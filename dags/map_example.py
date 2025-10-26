from datetime import UTC, datetime

from airflow.sdk import DAG, task


@task
def add(x: int, y: int):
    return x + y


with DAG(dag_id="map-with-partial", start_date=datetime(2021, 1, 1, tzinfo=UTC)) as dag:
    _ = add.partial(y=10).expand(x=[1, 2, 3])

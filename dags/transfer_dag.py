from datetime import UTC, datetime
import logging

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task  # pyright: ignore[reportUnknownVariableType]
from pydantic import TypeAdapter, ValidationError

from dags.schemas import Target2Src, metadata_table

logger = logging.getLogger(__name__)


@task(task_id="load_transfer_maping")
def load_transfer_maping() -> list[Target2Src]:
    logger.info("Start task get transfer columns")
    hook = PostgresHook(postgres_conn_id="metadata_conn_id")
    engine = hook.get_sqlalchemy_engine()
    ta = TypeAdapter(list[Target2Src])

    with engine.begin() as con:
        try:
            result = con.execute(metadata_table.select())
            mapings_rows = result.mappings().all()
            mapings_list = ta.validate_python(mapings_rows)

        except ValidationError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e

    if not len(mapings_list):
        logger.warning("Empty Transfer Table")
        raise AirflowNotFoundException("No columns to transfer")

    result = ta.dump_python(mapings_list, mode="json")
    return result


@dag(
    dag_id="debug_psql",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["debug"],
)
def debug_psql_dag():
    run_this = load_transfer_maping()


debug_psql = debug_psql_dag()

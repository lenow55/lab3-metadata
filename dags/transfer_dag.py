import logging

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task

from dags.schemas import metadata_table

logger = logging.getLogger(__name__)


@task(task_id="load_transfer_maping")
def load_transfer_maping():
    logger.info("Start task get transfer columns")
    hook = PostgresHook(postgres_conn_id="metadata_conn_id")
    engine = hook.get_sqlalchemy_engine()
    smth = metadata_table.select().compile(bind=engine)
    map_df = hook.get_pandas_df(smth.string)
    if map_df.empty:
        logger.warning("Empty Transfer Table")
        raise AirflowNotFoundException("No columns to transfer")

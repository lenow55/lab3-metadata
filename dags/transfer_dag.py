import logging
from datetime import UTC, datetime
from typing import Any

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task  # pyright: ignore[reportUnknownVariableType]
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from pydantic import TypeAdapter, ValidationError

from dags.schemas import Target2Src, metadata_table
from plugins.transfer_manager import TransferDateTimeManager

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


# default_args = (
#     {
#         "metadata_transfer_table": "transfer_max_dates",
#         "init_date": "1970-01-01 00:00:00",
#     },
# )


@task(task_id="get_last_update", multiple_outputs=True)
def get_last_update():
    init_date = Variable.get(key="init_date", default="1970-01-01 00:00:00")
    transfer_table = Variable.get(
        key="metadata_transfer_table", default="transfer_max_dates"
    )
    manager = TransferDateTimeManager(transfer_table, init_date=init_date)
    starttime = manager.get_max_date()
    endtime = datetime.now().isoformat(sep=" ")
    return {"starttime": starttime, "endtime": endtime}


@task(task_id="transfer_data")
def transfer_data(starttime: Any, endtime: Any, src2target_list: Any):
    logger.info(starttime)
    logger.info(endtime)
    logger.info(src2target_list)
    pass


@dag(
    dag_id="debug_psql",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["debug"],
)
def debug_psql_dag():
    src2target = load_transfer_maping()
    work_dates = get_last_update()
    if not isinstance(work_dates, PlainXComArg):
        raise RuntimeError("Bad arg type")
    transfer_res = transfer_data(
        starttime=work_dates["starttime"],
        endtime=work_dates["endtime"],
        src2target_list=src2target,
    )
    transfer_res.set_upstream([src2target, work_dates])


# TODO: получается дальше надо извлечь одну дату и её передать в функцию,
# которая уже будет перемещать колонки
#
# мне нужен код, который будет перемещать одну колонку в другую.
# В целом можно сделать это одним запросом в базу данных.
# можно, кстати, запускать последовательно несколько тасков для перемещения
# но тогда есть опасность, что одна отработает, а другая нет, тогда повторный запуск
# не сработает
# или всю пачку перезапускать придётся

# да зачем тут сложности?

debug_psql = debug_psql_dag()

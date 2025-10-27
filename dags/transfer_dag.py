import logging

from airflow.sdk import dag, task  # pyright: ignore[reportUnknownVariableType]

from .schemas import metadata_backend, metadata_md
from .transfer_manager import TransferDateTimeManager

logger = logging.getLogger(__name__)

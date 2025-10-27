from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

# Объект для подвязки схем базы данных
metadata_backend = MetaData()

source = Table(
    "source",
    metadata_backend,
    Column("id", Integer, primary_key=True),
    Column("ts", DateTime),
    Column("column1", String(length=50), nullable=True),
    Column("i_column2", String(length=50), nullable=True),
    Column("column3", String(length=50), nullable=True),
    Column("i_column4", String(length=50), nullable=True),
    Column("column5", String(length=50), nullable=True),
    Column("i_column6", String(length=50), nullable=True),
)

target = Table(
    "target",
    metadata_backend,
    Column("id", Integer, primary_key=True),
    Column("ts", DateTime),
    Column("column7", String(length=50), nullable=True),
    Column("i_column8", String(length=50), nullable=True),
    Column("column9", String(length=50), nullable=True),
    Column("i_column10", String(length=50), nullable=True),
    Column("column11", String(length=50), nullable=True),
    Column("i_column12", String(length=50), nullable=True),
)

# Объект для подвязки схем базы данных
metadata_md = MetaData()

transfer_table = Table(
    "transfer_max_dates",
    metadata_md,
    Column("table_name", Text, primary_key=True),
    Column("max_date", DateTime),
    Column("updated_at", DateTime),
)

metadata_table = Table(
    "transfer_metadata",
    metadata_md,
    Column("id", Integer, primary_key=True),
    Column("source_name", String(length=50)),
    Column("target_name", String(length=50)),
)

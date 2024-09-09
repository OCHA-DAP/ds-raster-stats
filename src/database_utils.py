import datetime

from sqlalchemy import (
    CHAR,
    Column,
    Date,
    Double,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects.postgresql import insert

from config import DATABASES


def db_engine(mode):
    """
    Create a SQLAlchemy engine for connecting to the specified database.
    Must be defined in config.DATABASES.

    Parameters
    ----------
    mode : str
        The mode in which the database is being accessed (e.g., 'local', 'dev').

    Returns
    -------
    sqlalchemy.engine.Engine
        A SQLAlchemy engine object for the specified database mode.
    """
    engine_url = DATABASES[mode]["engine_url"]
    return create_engine(engine_url)


def create_dataset_table(dataset, engine, incl_leadtime=False):
    """
    Create a table for storing dataset statistics in the database.

    Parameters
    ----------
    dataset : str
        The name of the dataset for which the table is being created.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object used to connect to the database.
    incl_leadtime : Bool
        Whether or not to include a 'leadtime' column in the table.
        Will only apply for datasets that are forecasts.

    Returns
    -------
    None
    """
    metadata = MetaData()
    columns = [
        Column("min", Double),
        Column("max", Double),
        Column("mean", Double),
        Column("count", Integer),
        Column("sum", Double),
        Column("std", Double),
        Column("percentile_10", Double),
        Column("percentile_20", Double),
        Column("percentile_30", Double),
        Column("percentile_40", Double),
        Column("percentile_50", Double),
        Column("percentile_60", Double),
        Column("percentile_70", Double),
        Column("percentile_80", Double),
        Column("percentile_90", Double),
        Column("valid_date", Date),
        Column("pcode", String),
        Column("adm_level", Integer),
        Column("iso3", CHAR(3)),
    ]

    unique_constraint_columns = ["valid_date", "pcode"]
    if incl_leadtime:
        columns.append(Column("leadtime", String))
        unique_constraint_columns.append("leadtime")

    Table(
        dataset,
        metadata,
        *columns,
        UniqueConstraint(
            *unique_constraint_columns,
            name=f"{dataset}_valid_date_leadtime_pcode_key",
            postgresql_nulls_not_distinct=True,
        ),
    )

    metadata.create_all(engine)
    return


def create_qa_table(engine):
    """
    Create a 'qa' table in the database for logging errors during processing.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object used to connect to the database.

    Returns
    -------
    None
    """
    metadata = MetaData()
    Table(
        "qa",
        metadata,
        Column("date", String),
        Column("iso3", CHAR(3)),
        Column("adm_level", Integer),
        Column("dataset", String),
        Column("error", String),
        Column("stack_trace", String),
    )
    metadata.create_all(engine)
    return


def insert_qa_table(iso3, adm_level, dataset, error, stack_trace, engine):
    """
    Insert an error record into the 'qa' table in the database.

    Parameters
    ----------
    iso3 : str
        The ISO3 code for the country where the error occurred.
    adm_level : int, optional
        The administrative level associated with the error (default is None).
    dataset : str
        The dataset name related to the error.
    error : Exception
        The error encountered during processing.
    stack_trace : str
        The stack trace of the error.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object used to connect to the database.

    Returns
    -------
    None
    """
    metadata = MetaData()
    qa = Table("qa", metadata, autoload_with=engine)
    cur_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stmt = insert(qa).values(
        date=cur_date,
        iso3=iso3,
        adm_level=adm_level,
        dataset=dataset,
        error=str(error),
        stack_trace=stack_trace.strip(),
    )
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
    return


# https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
def postgres_upsert(table, conn, keys, data_iter, constraint=None):
    """
    Perform an upsert (insert or update) operation on a PostgreSQL table.

    Parameters
    ----------
    table : sqlalchemy.sql.schema.Table
        The SQLAlchemy Table object where the data will be inserted or updated.
    conn : sqlalchemy.engine.Connection
        The SQLAlchemy connection object used to execute the upsert operation.
    keys : list of str
        The list of column names used as keys for the upsert operation.
    data_iter : iterable
        An iterable of tuples or lists containing the data to be inserted or updated.
    constraint_name : str
        Name of the uniqueness constraint

    Returns
    -------
    None
    """
    if not constraint:
        constraint = f"{table.table.name}_valid_date_leadtime_pcode_key"
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=constraint,
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
    return

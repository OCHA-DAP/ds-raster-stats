import datetime

from sqlalchemy import (
    CHAR,
    REAL,
    Boolean,
    Column,
    Date,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import insert

from src.config.settings import DATABASES


def db_engine_url(mode):
    """
    Create a SQLAlchemy engine url for connecting to the specified database.
    Must be defined in config.DATABASES.

    Parameters
    ----------
    mode : str
        The mode in which the database is being accessed (e.g., 'local', 'dev').

    Returns
    -------
    str
        The engine url for the appropriate mode
    """
    return DATABASES[mode]


def create_dataset_table(dataset, engine, is_forecast=False, extra_dims=[]):
    """
    Create a table for storing dataset statistics in the database.

    Parameters
    ----------
    dataset : str
        The name of the dataset for which the table is being created.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object used to connect to the database.
    is_forecast : Bool
        Whether or not the dataset is a forecast. Will include `leadtime` and
        `issued_date` columns if so.

    Returns
    -------
    None
    """
    metadata = MetaData()
    columns = [
        Column("iso3", CHAR(3)),
        Column("pcode", String),
        Column("valid_date", Date),
        Column("adm_level", Integer),
        Column("mean", REAL),
        Column("median", REAL),
        Column("min", REAL),
        Column("max", REAL),
        Column("count", Integer),
        Column("sum", REAL),
        Column("std", REAL),
    ]

    unique_constraint_columns = ["valid_date", "pcode"]
    if is_forecast:
        columns.insert(3, Column("issued_date", Date))
    for idx, dim in enumerate(extra_dims):
        # TODO: Support non-integer columns
        columns.insert(idx + 4, Column(dim, Integer))
        unique_constraint_columns.append(dim)

    Table(
        f"{dataset}",
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


def create_iso3_table(engine):
    metadata = MetaData()
    Table(
        "iso3",
        metadata,
        Column("iso3", CHAR(3)),
        Column("has_active_hrp", Boolean),
        Column("max_adm_level", Integer),
        Column("stats_last_updated", Date),
        Column("shp_url", String),
    )
    metadata.create_all(engine)


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


def postgres_upsert(table, conn, keys, data_iter, constraint=None):
    """
    Perform an upsert (insert or update) operation on a PostgreSQL table. Adapted from:
    https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update

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

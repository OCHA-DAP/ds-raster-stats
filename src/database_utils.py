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
    insert,
)

from config import DATABASES


def db_engine(mode):
    engine_url = DATABASES[mode]["engine_url"]
    return create_engine(engine_url)


def create_error_table(engine):
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


def write_error_table(iso3, adm_level, dataset, error, stack_trace, engine):
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


def create_dataset_table(dataset, engine):
    metadata = MetaData()
    Table(
        dataset,
        metadata,
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
        Column("leadtime", String),
        Column("pcode", String),
        Column("adm_level", Integer),
        Column("iso3", CHAR(3)),
        UniqueConstraint(
            "valid_date",
            "leadtime",
            "pcode",
            name=f"{dataset}_valid_date_leadtime_pcode_key",
            postgresql_nulls_not_distinct=True,
        ),
    )

    metadata.create_all(engine)
    return


# https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_valid_date_leadtime_pcode_key",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

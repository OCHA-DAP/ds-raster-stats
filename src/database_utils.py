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

from config import DATABASES


def db_engine(mode):
    engine_url = DATABASES[mode]["engine_url"]
    return create_engine(engine_url)


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

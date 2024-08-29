import sqlite3

from config import DATABASES, DATASETS


def init_db(mode):
    conn = db_connection(mode)

    # TODO: Can this be less hard-coded?
    table_schema = """
    CREATE TABLE IF NOT EXISTS {dataset} (
        "mean" REAL,
        "std" REAL,
        "min" REAL,
        "max" REAL,
        "sum" REAL,
        "count" INTEGER,
        "percentile_10" REAL,
        "percentile_20" REAL,
        "percentile_30" REAL,
        "percentile_40" REAL,
        "percentile_50" REAL,
        "percentile_60" REAL,
        "percentile_70" REAL,
        "percentile_80" REAL,
        "percentile_90" REAL,
        "valid_date" TEXT,
        "leadtime" INTEGER,
        "pcode" TEXT,
        "adm_level" INTEGER,
        "iso3" TEXT
        );
    """
    cursor = conn.cursor()
    for dataset in DATASETS.keys():
        cursor.execute(table_schema.format(dataset=dataset))

    conn.commit()
    conn.close()

    return


def db_connection(mode):
    db = DATABASES[mode]
    if mode == "local":
        return sqlite3.connect(db["name"])
    else:
        # TODO
        return None

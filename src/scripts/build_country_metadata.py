from datetime import datetime

import pandas as pd
from sqlalchemy import CHAR, Boolean, Column, Date, Integer, MetaData, String, Table

from src.utils.cod_utils import get_metadata
from src.utils.database_utils import db_engine


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


def determine_max_adm_level(row):
    if row["has_active_hrp"]:
        return min(2, row["src_lvl"])
    else:
        return min(1, row["src_lvl"])


def create_iso3_df():
    # Get all countries with CODs
    df_all = get_metadata()
    df = df_all[["iso_3", "src_lvl", "src_update", "o_shp"]]
    df = df.drop_duplicates(["iso_3"], keep="first")

    # Get all countries with active HRPs
    # Download from https://data.humdata.org/dataset/humanitarian-response-plans?
    # and save in local project `data/` directory
    df_hrp = pd.read_csv("data/humanitarian-response-plans.csv").loc[1:]
    df_hrp["endDate"] = pd.to_datetime(df_hrp["endDate"])
    current_date = datetime.now()
    df_active_hrp = df_hrp[
        (
            df_hrp["categories"].str.contains(
                "Humanitarian response plan", case=False, na=False
            )
        )
        & (df_hrp["endDate"] >= current_date)  # noqa
    ]
    iso3_codes = set()
    for locations in df_active_hrp["locations"]:
        iso3_codes.update(locations.split("|"))
    iso3_codes = {code.strip() for code in iso3_codes if code.strip()}

    df["has_active_hrp"] = df["iso_3"].isin(iso3_codes)
    df["max_adm_level"] = df.apply(determine_max_adm_level, axis=1)
    df["stats_last_updated"] = None
    return df


def main(mode):
    engine = db_engine(mode)
    create_iso3_table(engine)
    df = create_iso3_df()
    df.to_sql(
        "iso3",
        con=engine,
        if_exists="replace",
        index=False,
    )

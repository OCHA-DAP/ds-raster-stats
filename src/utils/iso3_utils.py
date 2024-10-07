import os
import zipfile
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from sqlalchemy import text

from src.utils.cloud_utils import get_container_client
from src.utils.database_utils import create_iso3_table


def get_metadata():
    """
    Retrieve metadata on COD boundaries downloadable from fieldmaps.io.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the metadata sorted by the "iso_3" column
        in descending order.
    """
    url = "https://data.fieldmaps.io/cod.csv"
    response = requests.get(url)
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data).sort_values(by="iso_3", ascending=True)

    # Some ISO3s are duplicated, with separate entries used to identify
    # certain offshore territories or distinct geographic regions that might be labelled
    # separately on a global map. However the source data in the shp originals is the
    # same so we can just drop the duplicates.
    df = df.drop_duplicates(subset="iso_3", keep="first")
    return df


def load_shp(shp_url, shp_dir, iso3):
    """
    Download and extract a zipped shapefile from a given Fieldmaps URL.

    Parameters
    ----------
    shp_url : str
        The URL of the zipped shapefile to be downloaded.
    shp_dir : str
        The directory where the zipped shapefile will be saved and extracted.
    iso3 : str
        A three-letter ISO code used to name the temporary zip file.

    Returns
    -------
    None
    """
    response = requests.get(shp_url)
    temp_path = os.path.join(shp_dir, f"{iso3}_shapefile.zip")

    with open(temp_path, "wb") as f:
        f.write(response.content)

    with zipfile.ZipFile(temp_path, "r") as zip_ref:
        zip_ref.extractall(shp_dir)


def load_shp_from_azure(iso3, shp_dir, mode):
    """
    Download and extract a zipped shapefile from Azure Blob Storage.

    Parameters
    ----------
    iso3 : str
        A three-letter ISO code used to identify the shapefile.
    shp_dir : str
        The directory where the zipped shapefile will be saved and extracted.
    mode : str
        The current mode, determining which Azure storage container to point to (dev or prod)

    Returns
    -------
    None
    """
    blob_name = f"{iso3.lower()}_shp.zip"
    container_client = get_container_client(mode, "polygon")
    blob_client = container_client.get_blob_client(blob_name)

    if not blob_client.exists():
        print(f"No shapefile zip found for: {blob_name}")
        return

    temp_path = os.path.join(shp_dir, f"{iso3}_shapefile.zip")
    with open(temp_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    with zipfile.ZipFile(temp_path, "r") as zip_ref:
        zip_ref.extractall(shp_dir)

    print(f"Shapefile for {iso3} downloaded and extracted to: {shp_dir}")


def get_iso3_data(iso3_codes, engine):
    """
    Retrieve ISO3 data from a database for given ISO3 code(s).

    Parameters
    ----------
    iso3_codes : list of str
        A list containing one or more three-letter ISO country codes.
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for database connection.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the ISO3 data for the specified country code(s).

    """
    if iso3_codes:
        if len(iso3_codes) == 1:
            query = text("SELECT * FROM public.iso3 WHERE iso_3 = :code")
            params = {"code": iso3_codes[0]}
        else:
            query = text("SELECT * FROM public.iso3 WHERE iso_3 = ANY(:codes)")
            params = {"codes": iso3_codes}
        df = pd.read_sql_query(query, engine.connect(), params=params)
    else:
        query = text("SELECT * FROM public.iso3")
        df = pd.read_sql_query(query, engine.connect())

    return df


def determine_max_adm_level(row):
    """
    Determine the maximum administrative level to calculate stats to,
    based on HRP status and data availability.

    Parameters
    ----------
    row : pandas.Series
        A row from a DataFrame containing 'has_active_hrp' and 'src_lvl' columns.

    Returns
    -------
    int
        The determined maximum administrative level.
    """
    if row["has_active_hrp"]:
        return min(2, row["src_lvl"])
    else:
        return min(1, row["src_lvl"])


def create_iso3_df(engine):
    """
    Create and populate an ISO3 table in the database with country information.
    NOTE: Needs to be run locally with an appropriate CSV in the `data/` directory.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for database connection.

    Returns
    -------
    None
    """
    create_iso3_table(engine)
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

    df.to_sql(
        "iso3",
        con=engine,
        if_exists="replace",
        index=False,
    )

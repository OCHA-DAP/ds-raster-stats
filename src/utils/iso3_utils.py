import os
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import requests
from sqlalchemy import bindparam, text

from src.config.settings import load_pipeline_config
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


def load_shp_cached(iso3, mode, cache_dir=None):
    """
    Download and extract a country's zipped shapefile from Azure Blob
    Storage into a cache directory, reusing it if already present. The
    extraction is atomic (extract to a temp dir, then rename), so
    concurrent worker processes can share the cache safely.

    The cache directory is keyed on the blob's ETag, so a hand-refreshed
    COD in blob is picked up on the next run rather than being masked by
    a stale copy on a long-lived host. Without that, the weight cache
    (which keys on the geometry it is given) would stay perfectly
    consistent with boundaries that are silently out of date.

    Parameters
    ----------
    iso3 : str
        A three-letter ISO code used to identify the shapefile.
    mode : str
        The current mode, determining which Azure storage container to
        point to (dev or prod)
    cache_dir : str, optional
        Root of the boundary cache. Defaults to $BOUNDARY_CACHE_DIR or
        `raster-stats-cache/boundaries` under the system temp dir
        (relative dot-dirs fail on Databricks' workspace filesystem).

    Returns
    -------
    str
        Directory containing the extracted shapefiles.
    """
    if cache_dir is None:
        cache_dir = os.getenv(
            "BOUNDARY_CACHE_DIR",
            os.path.join(
                tempfile.gettempdir(), "raster-stats-cache", "boundaries"
            ),
        )
    blob_mode = "dev" if mode == "local" else mode
    # Deliberately not caught: an unreachable blob must fail the country
    # loudly (process_chunk logs it to the qa table like any other
    # per-country error) rather than silently reusing a cached copy of
    # unbounded age -- which is the drift this ETag key exists to stop.
    props = (
        get_container_client(blob_mode, "polygon")
        .get_blob_client(f"{iso3.lower()}_shp.zip")
        .get_blob_properties()
    )
    tag = re.sub(r"[^A-Za-z0-9]", "", str(props.etag))[:16]
    target_dir = os.path.join(cache_dir, mode, f"{iso3.lower()}_{tag}")
    if os.path.isdir(target_dir):
        return target_dir

    os.makedirs(os.path.dirname(target_dir), exist_ok=True)
    tmp_dir = f"{target_dir}.tmp{os.getpid()}"
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        load_shp_from_azure(iso3, tmp_dir, mode)
        os.rename(tmp_dir, target_dir)
    except OSError:
        # Another worker finished extracting first
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if not os.path.isdir(target_dir):
            raise
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return target_dir


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
    # No data stored locally, so read from dev (as in stack_cogs)
    if mode == "local":
        mode = "dev"
    blob_name = f"{iso3.lower()}_shp.zip"
    container_client = get_container_client(mode, "polygon")
    blob_client = container_client.get_blob_client(blob_name)

    temp_path = os.path.join(shp_dir, f"{iso3}_shapefile.zip")
    with open(temp_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    with zipfile.ZipFile(temp_path, "r") as zip_ref:
        zip_ref.extractall(shp_dir)


def _iso3_table(engine):
    """Qualify with the schema on postgres; sqlite has no schemas.

    Leaving it unqualified on postgres would resolve through
    `search_path`, so a same-named table in another schema on the
    search path would silently be read instead.
    """
    return "public.iso3" if engine.dialect.name == "postgresql" else "iso3"


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
    if iso3_codes and len(iso3_codes) > 0:
        # IN with an expanding bindparam works on both postgres and
        # local sqlite (unlike ANY)
        table = _iso3_table(engine)
        query = text(f"SELECT * FROM {table} WHERE iso3 IN :codes").bindparams(
            bindparam("codes", expanding=True)
        )
        params = {"codes": list(iso3_codes)}
        df = pd.read_sql_query(query, engine.connect(), params=params)
    else:
        query = text(f"SELECT * FROM {_iso3_table(engine)}")
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


def load_coverage():
    pipelines = ["seas5", "era5", "imerg", "floodscan"]
    coverage = {}

    for dataset in pipelines:
        config = load_pipeline_config(dataset)
        if "coverage" in config:
            dataset_coverage = config["coverage"]
            coverage[dataset] = dataset_coverage

    return coverage


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
    dataset_coverage = load_coverage()

    iso3_codes = set()
    for locations in df_active_hrp["locations"]:
        iso3_codes.update(locations.split("|"))
    iso3_codes = {code.strip() for code in iso3_codes if code.strip()}

    df["has_active_hrp"] = df["iso_3"].isin(iso3_codes)
    df["max_adm_level"] = df.apply(determine_max_adm_level, axis=1)
    df["stats_last_updated"] = None

    for dataset in dataset_coverage:
        df[dataset] = df["iso_3"].isin(dataset_coverage[dataset])

    # TODO: This list seems to have some inconsistencies when compared against the
    # contents of all polygons
    # Also need global p-codes list from https://fieldmaps.io/data/cod
    # We want to get the total number of pcodes per iso3, across each admin level
    df_pcodes = pd.read_csv("data/global-pcodes.csv", low_memory=False)
    df_pcodes.drop(df_pcodes.index[0], inplace=True)
    df_counts = (
        df_pcodes.groupby(["Location", "Admin Level"])["P-Code"]
        .count()
        .reset_index(name="P-Code Count")
    )
    df_counts["Admin Level"] = df_counts["Admin Level"].astype(int)
    df_counts = df_counts[df_counts["Admin Level"].isin([0, 1, 2])]

    df_wide = df_counts.pivot(
        index="Location", columns="Admin Level", values="P-Code Count"
    )
    df_wide.columns = [f"adm{level}-pcode-count" for level in df_wide.columns]
    df_wide = df_wide.reset_index()
    df_wide["adm0-pcode-count"] = 1

    df_merged = df.merge(df_wide, left_on="iso_3", right_on="Location")
    df_merged["adm2-pcode-count"] = np.where(
        df_merged["max_adm_level"] == 2, df_merged["adm2-pcode-count"], np.nan
    )
    df_merged["total-pcodes"] = (
        df_merged[["adm1-pcode-count", "adm2-pcode-count", "adm0-pcode-count"]]
        .fillna(0)
        .sum(axis=1)
    )
    df_merged = df_merged.drop(columns=["src_lvl", "Location"])
    df_merged.rename(columns={"iso_3": "iso3"}, inplace=True)

    df_merged.to_sql(
        "iso3",
        con=engine,
        if_exists="replace",
        index=False,
    )

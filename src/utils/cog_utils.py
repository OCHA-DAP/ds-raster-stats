import logging
import os
import re
from datetime import date, datetime, timedelta

import coloredlogs
import dask
import rioxarray as rxr
import tqdm
import xarray as xr

from helpers.download_file import download_file
from src.config.settings import LOG_LEVEL, load_pipeline_config
from src.utils.cloud_utils import get_cog_url, get_container_client
from src.utils.general_utils import parse_date

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def process_imerg(cog_name, mode):
    """
    Processes an IMERG Cloud Optimized GeoTIFF (COG) file
    and prepares it to be stacked.

    Parameters
    ----------
    cog_name : str
        The name of the IMERG COG file
    mode : str
        Storage mode from where to access the data. local/dev/prod

    Returns
    -------
    xarray.DataArray
        A data array with the contents of the IMERG COG file, with an additional 'date' dimension
        based on the filename. The data array is persisted in memory for efficient access.
    """
    da_in = get_cog_da(cog_name, mode)

    year_valid = da_in.attrs["year_valid"]
    month_valid = str(da_in.attrs["month_valid"]).zfill(2)
    date_valid = str(da_in.attrs["date_valid"]).zfill(2)
    date_in = f"{year_valid}-{month_valid}-{date_valid}"

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in = da_in.expand_dims(["date"])

    da_in = da_in.persist()
    return da_in


def process_era5(cog_name, mode):
    """
    Processes an ERA5 Cloud Optimized GeoTIFF (COG) file
    and prepares it to be stacked.

    Parameters
    ----------
    cog_name : str
        The name of the ERA5 COG file
    mode : str
        Storage mode from where to access the data. local/dev/prod

    Returns
    -------
    xarray.DataArray
        A data array with the contents of the ERA5 COG file, with an additional 'date' dimension
        based on the filename. The data array is persisted in memory for efficient access.
    """
    da_in = get_cog_da(cog_name, mode)

    year_valid = da_in.attrs["year_valid"]
    month_valid = str(da_in.attrs["month_valid"]).zfill(2)
    date_in = f"{year_valid}-{month_valid}-01"

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in = da_in.expand_dims(["date"])

    da_in = da_in.persist()
    return da_in


def process_seas5(cog_name, mode):
    """
    Processes a SEAS5 Cloud Optimized GeoTIFF (COG) file
    and prepares it to be stacked.

    Parameters
    ----------
    cog_name : str
        The name of the SEAS5 COG file
    mode : str
        Storage mode from where to access the data. local/dev/prod

    Returns
    -------
    xarray.DataArray
        A data array with the contents of the SEAS5 COG file, with an additional 'date' dimension
        based on the filename. The data array is persisted in memory for efficient access.
    """
    da_in = get_cog_da(cog_name, mode)

    year_valid = da_in.attrs["year_valid"]
    month_valid = str(da_in.attrs["month_valid"]).zfill(2)
    date_in = f"{year_valid}-{month_valid}-01"

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in["leadtime"] = da_in.attrs["leadtime"]
    da_in = da_in.expand_dims(["date", "leadtime"])
    return da_in


def get_cog_da(cog_name, mode):
    cog_url = get_cog_url(mode, cog_name)
    da_in = rxr.open_rasterio(cog_url, chunks="auto")

    """
    if gdf is not None:
        minx, miny, maxx, maxy = gdf.total_bounds
        da_in = da_in.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
    else:
        da_in = rxr.open_rasterio(cog_url, chunks="auto")
    """
    return da_in


def process_floodscan(cog_name, mode):
    da_in = get_cog_da(cog_name, mode)

    year_valid = da_in.attrs["year_valid"]
    month_valid = str(da_in.attrs["month_valid"]).zfill(2)
    date_valid = str(da_in.attrs["date_valid"]).zfill(2)
    date_in = f"{year_valid}-{month_valid}-{date_valid}"

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in = da_in.expand_dims(["date"])

    da_in = da_in.persist()
    return da_in


def extract_date_and_leadtime_from(filepath):
    match_base_date = re.search(r"(\d{4})/(\d{2})/(\d{2})/", filepath)
    match_forecast_date_str = re.search(r"(\d{4}\.\d{2}\.\d{2})", filepath)[0]
    base_date = date(*map(int, match_base_date.groups()))
    valid_date = datetime.strptime(match_forecast_date_str, "%Y.%m.%d").date()
    leadtime = (valid_date - base_date).days

    return valid_date, leadtime


def process_chirps(cog_name, mode):
    if cog_name.startswith("ftp://"):
        cog_name = download_cog_from_ftp(cog_name)
    da_in = get_cog_da(cog_name, mode)
    cog_date, leadtime = extract_date_and_leadtime_from(
        da_in.attrs["TIFFTAG_DOCUMENTNAME"]
    )
    year_valid = cog_date.year
    month_valid = cog_date.month
    day_valid = cog_date.day
    date_in = (
        f"{year_valid}-{str(month_valid).zfill(2)}-{str(day_valid).zfill(2)}"
    )

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in["leadtime"] = leadtime
    da_in = da_in.expand_dims(["date", "leadtime"])
    return da_in


CHIRPS_LEADTIMES = range(16)


def get_chirps_cog_urls(base_url, issued_date, leadtimes=CHIRPS_LEADTIMES):
    """
    Generate the expected CHIRPS-GEFS COG URLs for one issue date.

    Parameters
    ----------
    base_url : str
        The root of the CHIRPS-GEFS daily archive (config `source_url`).
    issued_date : datetime.date
        The forecast issue date.
    leadtimes : iterable of int
        Leadtimes in days to generate URLs for.

    Returns
    -------
    list of str
        One URL per leadtime, in leadtime order.
    """
    date_url = f"{base_url.rstrip('/')}/{issued_date:%Y/%m/%d}/"
    return [
        f"{date_url}c3g_{issued_date + timedelta(days=lt):%Y.%m.%d}.tif"
        for lt in leadtimes
    ]


def download_cog_from_ftp(cog_url, output_dir="downloads"):
    """
    Download one CHIRPS-GEFS COG into a folder named after its issue date.

    The issue date is taken from the URL's YYYY/MM/DD/ folder, so
    .../2001/01/01/c3g_2001.01.16.tif is saved as
    `{output_dir}/2001/01/01/c3g_2001.01.16.tif`. Valid-date filenames repeat
    across issue dates, so this keeps each issue date's COGs apart. A COG
    that's already been downloaded is reused.

    Parameters
    ----------
    cog_url : str
        The FTP URL of the COG.
    output_dir : str
        The root directory for the issue-date folders.

    Returns
    -------
    str
        The local path of the COG.

    Raises
    ------
    ValueError
        If the URL has no YYYY/MM/DD/ issue-date folder.
    FileNotFoundError
        If the COG isn't on the server.
    """
    match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/[^/]+$", cog_url)
    if not match:
        raise ValueError(f"No YYYY/MM/DD/ issue-date folder in {cog_url}")
    issued_dir = os.path.join(output_dir, *match.groups())
    logger.info(f"Downloading COG {cog_url} to {issued_dir}...")
    return download_file(cog_url, issued_dir)


def get_cog_url_for_dates(dataset, base_url, dates):
    cogs_list = []

    for cog_date in dates:
        if dataset == "chirps":
            date_urls = get_chirps_cog_urls(base_url, cog_date)
            cogs_list.extend(date_urls)
        else:
            raise NotImplementedError(f"Not implemented for dataset {dataset}")

    return cogs_list


def stack_cogs(cogs, dataset, mode="dev"):
    """
    Stack Cloud Optimized GeoTIFFs (COGs) for a specified date range into an xarray Dataset.

    This function retrieves and stacks COGs from a cloud storage container for a given dataset and
    list of dates, and returns the stacked data as an `xarray.Dataset`. The data is accessed remotely
    and processed into a single `Dataset` with the dimension `date` as the stacking dimension.

    Parameters
    ----------
    cogs : list
        The list of cogs for which we want to stack
    dataset : str, optional
        The name of the dataset to retrieve COGs from. Options are "floodscan", "era5", "imerg", "seas5" and "chirps".
    mode : str, optional
        The environment mode to use when accessing the cloud storage container. May be "dev", "prod", or "local".

    Returns
    -------
    xarray.Dataset
        A Dataset containing the stacked COG data, with time as the stacking dimension.
    """

    das = []
    for cog in cogs:
        if dataset == "era5":
            da_in = process_era5(cog, mode)
        elif dataset == "seas5":
            da_in = process_seas5(cog, mode)
        elif dataset == "imerg":
            da_in = process_imerg(cog, mode)
        elif dataset == "floodscan":
            da_in = process_floodscan(cog, mode)
        elif dataset == "chirps":
            try:
                da_in = process_chirps(cog, mode)
            except FileNotFoundError as e:
                # The URLs are generated, so a leadtime may be missing
                logger.warning(f"Skipping {cog}: {e}")
                continue
        das.append(da_in)

    # Note that we're dropping all attributes here
    with dask.config.set({"array.slicing.split_large_chunks": True}):
        ds = xr.combine_by_coords(das, combine_attrs="drop")
    return ds


def get_cogs_list(dataset, dates, mode):
    if mode == "local":
        logger.info(
            "Retrieving data from `dev` Azure blob when running in `local` mode."
        )
        mode = "dev"

    container_client = get_container_client(mode, "raster")
    config = load_pipeline_config(dataset)
    cogs_list = []

    try:
        prefix = config["blob_prefix"] if "blob_prefix" in config else None
        source = config["source_url"] if "source_url" in config else None

        if prefix:
            cogs_list = [
                x.name
                for x in container_client.list_blobs(name_starts_with=prefix)
                if (parse_date(x.name) in (dates))
            ]
        elif source:
            source = config["source_url"]
            cogs_list = get_cog_url_for_dates(dataset, source, dates)
    except Exception:
        logger.error(
            "Input `dataset` must be one of `floodscan`, `era5`, `seas5`, `imerg` or `chirps`."
        )

    # TODO readd?logger.debug(f"Processing {len(cogs_list)} cog(s):")
    # for cog in cogs_list:
    #    logger.debug(f" - {cog}")

    # TODO Check with Hannah what the new behavior should be here
    # if len(cogs_list) != len(dates):
    #    logger.warning("Not all COGs available, given input dates")
    if len(cogs_list) == 0:
        raise Exception(f"No COGs found to process for dates: {dates}")

    # Only show progress bar if running in interactive mode (ie. running locally)
    cogs_list = tqdm.tqdm(cogs_list) if mode == "local" else cogs_list

    return cogs_list


# TODO: Might not scale well as we get more files in the blob
def get_most_recent_date(mode, name_prefix, dataset):
    """
    Find files with the most recent date in their filename from Azure blob storage.

    This function searches through Azure blob storage for files that start with the
    given prefix and match the date pattern for the specified dataset. It returns
    all files that match the most recent date found.

    Parameters
    ----------
    mode : str
        The mode in which the database is being accessed (e.g., 'local', 'dev').
    name_prefix : str
        The prefix of the filename before the date portion.
        For example, 'seas5/monthly/processed/precip_em_i'.
    dataset : str
        Type of dataset. Must be one of: 'imerg', 'era5', 'seas5'.
        This determines how the date is extracted from the filename.

    Returns
    -------
    list of str
        Names of all files that match the most recent date. Empty list if no
        matching files are found.
    """
    container_client = get_container_client(mode, "raster")
    blobs = container_client.list_blobs(name_starts_with=name_prefix)
    file_dates = {}

    for blob in blobs:
        try:
            date = parse_date(blob.name)
            file_dates[blob.name] = date
        except (ValueError, IndexError) as e:
            print(f"Skipping {blob.name}: {str(e)}")
            continue

    if not file_dates:
        return []

    most_recent_date = max(file_dates.values())

    return most_recent_date

import logging
import os
import re
from datetime import date, datetime, timedelta

import coloredlogs
import dask
import rioxarray as rxr
import tqdm
import xarray as xr

from helpers.download_file import download_ftp_files
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

    return base_date, leadtime


def process_chirps(cog_name, mode):
    da_in = get_cog_da(cog_name, mode)
    da_in = da_in.load()
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


def download_chirps_cogs(cog_urls, output_dir="downloads", connections=20):
    """
    Download CHIRPS-GEFS COGs before processing, over `connections` FTP
    connections at once. Only the COGs in `cog_urls` are downloaded. Each is
    saved under a folder named after its issue date
    (`{output_dir}/YYYY/MM/DD/`), since valid-date filenames repeat across
    issue dates.

    Downloading them up front means processing tasks only read local files,
    instead of each opening its own connections to the rate-limited server.
    COGs that are already downloaded are reused.

    Parameters
    ----------
    cog_urls : list of str
        FTP URLs of the COGs, as returned by `get_chirps_cog_urls`.
    output_dir : str
        The root directory for the issue-date folders.
    connections : int
        Number of FTP connections to download over at once.

    Returns
    -------
    list of str
        Local paths of the COGs, in the order of `cog_urls`. COGs that aren't
        on the server (the URLs are generated, so a leadtime may be missing)
        are skipped with a warning.

    Raises
    ------
    ValueError
        If a URL has no YYYY/MM/DD/ issue-date folder.
    """
    local_paths = {}
    for url in cog_urls:
        match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/([^/]+)$", url)
        if not match:
            raise ValueError(f"No YYYY/MM/DD/ issue-date folder in {url}")
        local_paths[url] = os.path.join(output_dir, *match.groups())

    to_download = [
        (url, path)
        for url, path in local_paths.items()
        if not os.path.isfile(path)
    ]
    if to_download:
        missing = download_ftp_files(to_download, connections=connections)
        for url in missing:
            logger.warning(f"Skipping {url}: not on the server")
    else:
        logger.info(f"Already downloaded all {len(cog_urls)} cog(s)")

    return [
        local_paths[url]
        for url in cog_urls
        if os.path.isfile(local_paths[url])
    ]


def download_cogs(dataset, cogs, output_dir, connections=20):
    """
    Download a date chunk's COGs before processing, so tasks read local files
    instead of each opening its own connections to the source.

    Parameters
    ----------
    dataset : str
        The dataset the COGs belong to.
    cogs : list of str
        The COGs, as returned by `get_cogs_list`: FTP URLs for CHIRPS
    output_dir : str
        The root directory to download into.
    connections : int
        Number of connections to download over at once.

    Returns
    -------
    list of str
        Local paths of the COGs. For CHIRPS, COGs that aren't on the server
        are skipped.
    """
    if dataset == "chirps":
        return download_chirps_cogs(cogs, output_dir, connections)
    raise NotImplementedError(f"Not implemented for dataset {dataset}")


def delete_downloads(paths, output_dir):
    """
    Delete downloaded COGs once they've been processed, then remove any
    issue-date folders under `output_dir` that are left empty.

    Parameters
    ----------
    paths : list of str
        Local paths of the COGs
    output_dir : str
        The root directory the COGs were downloaded into.
    """
    output_dir = os.path.abspath(output_dir)
    folders = set()
    for path in paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        folders.add(os.path.dirname(os.path.abspath(path)))

    # Walk up from each internal folder and its parents while they're empty
    for folder in sorted(folders, key=len, reverse=True):
        while folder.startswith(output_dir + os.sep):
            try:
                os.rmdir(folder)
            except OSError:
                break
            folder = os.path.dirname(folder)
    logger.info(f"Deleted {len(paths)} downloaded cog(s)")


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
            da_in = process_chirps(cog, mode)
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

    if len(cogs_list) != len(dates) and dataset != "chirps":
        logger.warning("Not all COGs available, given input dates")
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

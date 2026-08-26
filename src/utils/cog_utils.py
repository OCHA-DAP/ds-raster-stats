import logging
import re
from datetime import date, datetime
from urllib.parse import urljoin

import coloredlogs
import requests
import rioxarray as rxr
import tqdm
import xarray as xr
from bs4 import BeautifulSoup

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


def get_cog_da(cog_name, mode, test_path=None):
    cog_url = get_cog_url(mode, cog_name, test_path)
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
    da_in = get_cog_da(
        cog_name, mode, test_path="test_outputs/chirps/v3/15_day/global/data/"
    )
    cog_date, leadtime = extract_date_and_leadtime_from(
        da_in.attrs["TIFFTAG_DOCUMENTNAME"]
    )
    year_valid = cog_date.year
    month_valid = cog_date.month
    day_valid = cog_date.day
    date_in = f"{year_valid}-{month_valid}-{day_valid}"  # todo change this
    date_in = (
        f"{year_valid}-{str(month_valid).zfill(2)}-{str(day_valid).zfill(2)}"
    )

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in["leadtime"] = leadtime
    da_in = da_in.expand_dims(["date", "leadtime"])
    return da_in


def get_cog_url_for_dates(dataset, base_url, dates):
    cogs_list = []

    for cog_date in dates:
        logger.info(f"Processing date {cog_date}:")
        if dataset == "chirps":
            date_url = base_url + (
                f"/{cog_date.year}/"
                f"{str(cog_date.month).zfill(2)}/"
                f"{str(cog_date.day).zfill(2)}/"
            )
            date_urls = get_cog_list_from_url(date_url, date_url)
            cogs_list.extend(date_urls)
        else:
            raise NotImplementedError("")

    return cogs_list


def get_cog_list_from_url(
    base_url, current_url, visited=None, files_found=None
):
    files_found = [] if files_found is None else files_found
    visited = set() if visited is None else visited

    if current_url in visited:
        return files_found
    visited.add(current_url)

    try:
        response = requests.get(current_url)
        if response.status_code != 200:
            return
    except requests.RequestException:
        return

    soup = BeautifulSoup(response.text, "html.parser")

    for node in soup.find_all("a"):
        href = node.get("href")
        if not href or href.startswith(("?", "#", "mailto:")):
            continue

        full_url = urljoin(current_url, href)

        # Keep inside the base domain and path to avoid escaping the server
        if not full_url.startswith(base_url):
            continue

        # Check if it is a directory (ends with '/') or a file
        if href.endswith("/"):
            get_cog_list_from_url(
                current_url=full_url,
                base_url=base_url,
                visited=visited,
                files_found=files_found,
            )
        else:
            if full_url not in files_found:
                files_found.append(full_url)

    return files_found


def stack_cogs(dates, dataset, mode="dev"):
    """
    Stack Cloud Optimized GeoTIFFs (COGs) for a specified date range into an xarray Dataset.

    This function retrieves and stacks COGs from a cloud storage container for a given dataset and
    list of dates, and returns the stacked data as an `xarray.Dataset`. The data is accessed remotely
    and processed into a single `Dataset` with the dimension `date` as the stacking dimension.

    Parameters
    ----------
    dates : list
        The list of dates for which we want to load in COGs
    dataset : str, optional
        The name of the dataset to retrieve COGs from. Options include "floodscan", "era5", "imerg", and "seas5".
    mode : str, optional
        The environment mode to use when accessing the cloud storage container. May be "dev", "prod", or "local".

    Returns
    -------
    xarray.Dataset
        A Dataset containing the stacked COG data, with time as the stacking dimension.
    """
    # We don't have data stored locally, so will read from dev
    if mode == "local":
        logger.info(
            "Retrieving data from `dev` Azure blob when running in `local` mode."
        )
        mode = "dev"

    container_client = get_container_client(mode, "raster")
    config = load_pipeline_config(dataset)
    cogs_list = None

    try:
        prefix = config["blob_prefix"]
        cogs_list = [
            x.name
            for x in container_client.list_blobs(name_starts_with=prefix)
            if (parse_date(x.name) in (dates))
        ]
    except KeyError:
        source = config["source_url"]
        cogs_list = get_cog_url_for_dates(dataset, source, dates)
    except Exception:
        logger.error(
            "Input `dataset` must be one of `floodscan`, `era5`, `seas5`, `imerg` or `chirps`."
        )

    logger.debug(f"Processing {len(cogs_list)} cog(s):")
    for cog in cogs_list:
        logger.debug(f" - {cog}")

    # TODO fix below
    # if len(cogs_list) != len(dates):
    #    logger.warning("Not all COGs available, given input dates")
    if len(cogs_list) == 0:
        raise Exception(f"No COGs found to process for dates: {dates}")

    das = []

    # Only show progress bar if running in interactive mode (ie. running locally)
    cogs_list = tqdm.tqdm(cogs_list) if mode == "local" else cogs_list

    for cog in cogs_list:
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
    ds = xr.combine_by_coords(das, combine_attrs="drop")
    return ds


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

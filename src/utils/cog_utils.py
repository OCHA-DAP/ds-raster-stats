import logging

import coloredlogs
import pandas as pd
import rioxarray as rxr
import tqdm
import xarray as xr

from src.config.settings import load_pipeline_config
from src.utils.cloud_utils import get_cog_url, get_container_client

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)


def parse_date(filename, dataset):
    """
    Parses the date based on a COG filename.
    """
    if (dataset == "era5") or (dataset == "imerg"):
        date = pd.to_datetime(filename[-14:-4])
    elif dataset == "seas5":
        date = pd.to_datetime(filename[-18:-8])
    elif dataset == "floodscan":
        date = pd.to_datetime(filename[-21:-11])
    else:
        raise Exception("Input `dataset` must be one of: floodscan, imerg, era5, seas5")
    return pd.to_datetime(date)


# TODO: Update now that IMERG data has the right .attrs metadata
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
    cog_url = get_cog_url(mode, cog_name)
    da_in = rxr.open_rasterio(cog_url, chunks="auto")

    da_in = da_in.squeeze(drop=True)
    date_in = cog_name[-14:-4]
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
    cog_url = get_cog_url(mode, cog_name)
    da_in = rxr.open_rasterio(cog_url, chunks="auto")

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
    cog_url = get_cog_url(mode, cog_name)
    da_in = rxr.open_rasterio(cog_url, chunks="auto")

    year_valid = da_in.attrs["year_valid"]
    month_valid = str(da_in.attrs["month_valid"]).zfill(2)
    date_in = f"{year_valid}-{month_valid}-01"

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in["leadtime"] = da_in.attrs["leadtime"]
    da_in = da_in.expand_dims(["date", "leadtime"])
    return da_in


def process_floodscan(cog_name, mode):
    cog_url = get_cog_url(mode, cog_name)
    da_in = rxr.open_rasterio(cog_url, chunks="auto")

    year_valid = da_in.attrs["year_valid"]
    month_valid = str(da_in.attrs["month_valid"]).zfill(2)
    date_valid = cog_name[-13:-11]  # TODO: Change once attr is updated correctly
    date_in = f"{year_valid}-{month_valid}-{date_valid}"

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in = da_in.expand_dims(["date"])

    da_in = da_in.persist()
    return da_in


def stack_cogs(start_date, end_date, dataset="era5", mode="dev"):
    """
    Stack Cloud Optimized GeoTIFFs (COGs) for a specified date range into an xarray Dataset.

    This function retrieves and stacks COGs from a cloud storage container for a given dataset and
    date range, and returns the stacked data as an `xarray.Dataset`. The data is accessed remotely
    and processed into a single `Dataset` with the dimension `date` as the stacking dimension.

    Parameters
    ----------
    start_date : str or datetime-like
        The start date of the date range for stacking the COGs. This can be a string or a datetime object.
    end_date : str or datetime-like
        The end date of the date range for stacking the COGs. This can be a string or a datetime object.
    dataset : str, optional
        The name of the dataset to retrieve COGs from. Options include "floodscan", "era5", "imerg", and "seas5".
        Default is "era5".
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

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    container_client = get_container_client(mode, "raster")

    try:
        config = load_pipeline_config(dataset)
        prefix = config["blob_prefix"]
    except Exception:
        logger.error(
            "Input `dataset` must be one of `floodscan`, `era5`, `seas5`, or `imerg`."
        )

    cogs_list = [
        x.name
        for x in container_client.list_blobs(name_starts_with=prefix)
        if (parse_date(x.name, dataset) >= start_date)
        & (parse_date(x.name, dataset) <= end_date)  # noqa
    ]

    if len(cogs_list) == 0:
        raise Exception("No COGs found to process")

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
        das.append(da_in)

    # Note that we're dropping all attributes here
    ds = xr.combine_by_coords(das, combine_attrs="drop")
    return ds

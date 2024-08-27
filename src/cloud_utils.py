import os
from io import BytesIO

import pandas as pd
import rioxarray as rxr
import tqdm
import xarray as xr
from azure.storage.blob import ContainerClient


def get_container_client(mode, container_name):
    """
    Get a client for accessing an Azure Blob Storage container.

    This function generates a URL for an Azure Blob Storage container based on the specified mode
    and container name. It then creates and returns a `ContainerClient` object for interacting with
    the container.

    Parameters
    ----------
    mode : str
        The environment mode ("dev" or "prod"), used to determine the
        appropriate SAS token and Blob Storage URL.
    container_name : str
        The name of the container to access within the Blob Storage account.

    Returns
    -------
    azure.storage.blob.ContainerClient
        A `ContainerClient` object that can be used to interact with the specified Azure Blob Storage container

    """
    blob_sas = os.getenv(f"DSCI_AZ_SAS_{mode.upper()}")
    blob_url = (
        f"https://imb0chd0{mode}.blob.core.windows.net/"
        + container_name  # noqa
        + "?"  # noqa
        + blob_sas  # noqa
    )
    return ContainerClient.from_container_url(blob_url)


def get_cog_url(mode, cog_name):
    """
    Generate the URL for a Cloud Optimized GeoTIFF (COG) stored in Azure Blob Storage (or locally).
    This function constructs the URL for accessing a specific COG based on the provided mode and COG name.

    Parameters
    ----------
    mode : str
        The environment mode, ("dev" or "prod"), used to determine
        the appropriate URL and SAS token for the Blob Storage container.
    cog_name : str
        The name of the COG file within the Blob Storage container.

    Returns
    -------
    str
        The URL for accessing the specified COG.
    """
    if mode == "local":
        return "test_outputs/" + cog_name
    blob_sas = os.getenv(f"DSCI_AZ_SAS_{mode.upper()}")
    return f"https://imb0chd0{mode}.blob.core.windows.net/raster/{cog_name}?{blob_sas}"


def parse_date(filename):
    """
    Parses the date from the filename from an ERA5 COG.
    """
    # TODO: Works for ERA5
    date = pd.to_datetime(filename[-14:-4])
    return pd.to_datetime(date)


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
        The name of the dataset to retrieve COGs from. Options include "era5", "imerg", and "seas5".
        Default is "era5".
    mode : str, optional
        The environment mode to use when accessing the cloud storage container. May be "dev", "prod", or "local".

    Returns
    -------
    xarray.Dataset
        A Dataset containing the stacked COG data, with time as the stacking dimension.
    """

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    container_client = get_container_client(mode, "raster")

    # Get the start of each filename location on the blob
    if dataset == "era5":
        prefix = "era5/monthly/processed/daily_precip_reanalysis_v"
    elif dataset == "imerg":
        prefix = "imerg/v7/processed/imerg-daily-late-"
    elif dataset == "seas5":
        prefix = "seas5/mars/daily_precip_em_i"
    else:
        prefix = None

    cogs_list = []

    cogs_list = [
        x.name
        for x in container_client.list_blobs(name_starts_with=prefix)
        if (parse_date(x.name) >= start_date) & (parse_date(x.name) <= end_date)
    ]

    das = []
    for cog in tqdm.tqdm(cogs_list):
        cog_url = get_cog_url(mode, cog)
        da_in = rxr.open_rasterio(cog_url, chunks="auto")
        year_valid = da_in.attrs["year_valid"]
        month_valid = str(da_in.attrs["month_valid"]).zfill(2)
        date_in = f"{year_valid}-{month_valid}-01"
        da_in = da_in.squeeze(drop=True)
        da_in["date"] = date_in
        da_in = da_in.persist()
        das.append(da_in)

    ds = xr.concat(das, dim="date")
    return ds


def write_output_stats(df, fname, mode="dev"):
    """
    Write a DataFrame to a Parquet file either locally or to Azure Blob Storage.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing the data to be saved.
    fname : str
        The filename or blob name for the output Parquet file.
    mode : str, optional
        The mode of operation. If set to "local", the DataFrame is saved to a
        local Parquet file. Otherwise, the DataFrame is uploaded as a Parquet
        file to Azure Blob Storage. Default is "dev".

    Returns
    -------
    None
    """
    if mode == "local":
        df.to_parquet(fname, engine="pyarrow", index=False)
    else:
        # Convert the DataFrame to a Parquet file in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)  # Rewind the buffer
        container_client = get_container_client(mode, "tabular")
        data = parquet_buffer.getvalue()
        container_client.upload_blob(name=fname, data=data, overwrite=True)
    return

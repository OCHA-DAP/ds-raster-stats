import os
from io import BytesIO

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

import os
import zipfile
from io import StringIO

import pandas as pd
import requests


def get_metadata():
    """
    Retrieve and metadata on COD boundaries downloadable from fieldmaps.io.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the metadata sorted by the "src_update" column
        in descending order.
    """
    # TODO: Look into duplicated entries (by iso_3)
    url = "https://data.fieldmaps.io/cod.csv"
    response = requests.get(url)
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data).sort_values(by="src_update", ascending=False)
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

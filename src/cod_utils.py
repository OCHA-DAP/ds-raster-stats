import os
import zipfile
from io import StringIO

import pandas as pd
import requests


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

"""
This is a temporary helper script to load CODAB data from Fieldmaps to
a private Azure Storage Container. This is done to avoid overloading the
Fieldmaps server during large historical runs, and to integrate some basic
data cleaning that needs to be done on select ISO3 datasets.

This script will likely be quickly deprecated, so has not been written to
full production standards.

Usage: Run LOCALLY from root-level project directory `python helpers/load_polygons.py`
"""


import os
import zipfile
from pathlib import Path

import geopandas as gpd
import requests

from src.utils.cloud_utils import get_container_client
from src.utils.iso3_utils import get_metadata, load_shp

df = get_metadata()
# TODO: Swap out "dev"/"prod" depending on which container
# you're writing to
container_client = get_container_client("dev", "polygon")
data_dir = Path("data/tmp")


def download_zip(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to download: {url}")
        return None


if __name__ == "__main__":
    # TODO: Right now only set up to run locally
    for _, row in df.iterrows():
        shp_link = row["o_shp"]
        iso3 = row["iso_3"]
        print(f"Processing data for {iso3}...")
        zip_data = download_zip(shp_link)

        # Specific ISO3s that need to be dissolved at adm0 level
        # Temporary workaround before it's fixed in Fieldmaps
        if iso3 in ["NGA", "TCD", "BDI"]:
            outpath = "data/tmp/"
            load_shp(shp_link, outpath, iso3)
            adm0 = gpd.read_file(f"{outpath}{iso3}_adm0.shp")
            adm0 = adm0.dissolve()
            adm0.to_file(f"{outpath}{iso3}_adm0.shp")

            zip_name = f"{data_dir}/{iso3.lower()}_shp.zip"

            with zipfile.ZipFile(zip_name, "w") as zipf:
                for adm_level in range(3):  # 0 to 2
                    base_name = f"{iso3.lower()}_adm{adm_level}"
                    for ext in [".shp", ".dbf", ".prj", ".shx", ".cpg"]:
                        file_path = os.path.join(data_dir, base_name + ext)
                        if os.path.exists(file_path):
                            zipf.write(file_path, os.path.basename(file_path))

            with open(zip_name, "rb") as zip_data:
                blob_name = f"{iso3.lower()}_shp.zip"
                container_client.upload_blob(
                    name=blob_name, data=zip_data, overwrite=True
                )

        elif zip_data:
            blob_name = f"{iso3.lower()}_shp.zip"
            container_client.upload_blob(
                name=blob_name, data=zip_data, overwrite=True
            )
        else:
            print(f"Skipping {iso3} due to download failure")

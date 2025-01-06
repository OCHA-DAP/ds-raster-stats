---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.3
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Creating an admin lookup table

This notebook processes our saved Fieldmaps CODs to create a lookup table matching pcodes and places names across all relevant admin levels for which we have raster stats data. We select the **first available** column in the original CODs with the place name. In most cases this is the English, French, or Spanish name.

We want an output table with the following columns:

```python
DEFAULT_COLS = ["ISO3", "ADM0_PCODE", "ADM0_NAME",  "ADM1_PCODE", "ADM1_NAME", "ADM2_PCODE", "ADM2_NAME", "NAME_LANGUAGE", "ADM_LEVEL"]
```

```python
import pandas as pd
from sqlalchemy import create_engine
import tempfile
import geopandas as gpd
import os
from typing import Literal

from src.utils.database_utils import db_engine_url
from src.utils.iso3_utils import get_iso3_data, load_shp_from_azure
from src.utils.metadata_utils import select_name_column
from azure.storage.blob import ContainerClient, ContentSettings
from dotenv import load_dotenv

load_dotenv()

PROD_BLOB_SAS = os.getenv("DSCI_AZ_SAS_PROD")
DEV_BLOB_SAS = os.getenv("DSCI_AZ_SAS_DEV")

PROJECT_PREFIX = "polygon"

MODE = "dev"
engine = create_engine(db_engine_url(MODE))
df_iso3s = get_iso3_data(iso3_codes=None, engine=engine)
```

```python
dfs = []

with tempfile.TemporaryDirectory() as temp_dir:
    for _, row in df_iso3s.iterrows():
        iso3 = row["iso3"]
        max_adm_level = row["max_adm_level"]
        load_shp_from_azure(iso3, temp_dir, MODE)

        name_columns = []
        for admin_level in range(max_adm_level + 1):
            gdf = gpd.read_file(f"{temp_dir}/{iso3.lower()}_adm{admin_level}.shp")

            # Get name column and its language code
            name_column = select_name_column(gdf, admin_level)
            language_code = name_column[-2:]
            name_columns.append(name_column)

            # Standardize column names and add language info
            new_columns = [x.replace(f"_{language_code}", "_NAME") for x in name_columns]
            gdf = gdf.rename(columns=dict(zip(name_columns, new_columns)))
            gdf["NAME_LANGUAGE"] = language_code
            gdf["ISO3"] = iso3
            gdf["ADM_LEVEL"] = admin_level

            # Keep only relevant columns
            matching_cols = [col for col in gdf.columns if col in DEFAULT_COLS]
            dfs.append(gdf[matching_cols])

df_all = pd.concat(dfs, ignore_index=True)
```

Now writing this to Azure...

```python
def get_container_client(
    container_name: str = "projects", stage: Literal["prod", "dev"] = "dev"
):
    sas = DEV_BLOB_SAS if stage == "dev" else PROD_BLOB_SAS
    container_url = (
        f"https://imb0chd0{stage}.blob.core.windows.net/"
        f"{container_name}?{sas}"
    )
    return ContainerClient.from_container_url(container_url)


def upload_parquet_to_blob(
    blob_name,
    df,
    stage: Literal["prod", "dev"] = "dev",
    container_name: str = "projects",
    **kwargs,
):
    upload_blob_data(
        blob_name,
        df.to_parquet(**kwargs),
        stage=stage,
        container_name=container_name,
    )


def upload_blob_data(
    blob_name,
    data,
    stage: Literal["prod", "dev"] = "dev",
    container_name: str = "projects",
    content_type: str = None,
):
    container_client = get_container_client(
        stage=stage, container_name=container_name
    )

    if content_type is None:
        content_settings = ContentSettings(
            content_type="application/octet-stream"
        )
    else:
        content_settings = ContentSettings(content_type=content_type)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(
        data, overwrite=True, content_settings=content_settings
    )
```

```python
upload_parquet_to_blob("admin_lookup.parquet", df_all, MODE, PROJECT_PREFIX)
```

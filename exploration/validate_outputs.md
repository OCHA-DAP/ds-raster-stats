---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.3
  kernelspec:
    display_name: venv
    language: python
    name: python3
---

## Validate output stats

In this notebook we're taking a look at our output raster stats and comparing against the outputs from other raster stats calculation functions. We would expect our outputs to be the same as the `zonal_stats` outputs, and see some moderate differences with the `exactextract` outputs.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from exactextract import exact_extract
from rasterstats import zonal_stats
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import pandas as pd
import geopandas as gpd
import rioxarray as rxr
import numpy as np

# os.chdir("..")
from src.utils.cloud_utils import get_cog_url
from src.utils.raster_utils import upsample_raster, prep_raster

load_dotenv()
```

```python
AZURE_DB_PW_PROD = os.getenv("AZURE_DB_PW_PROD")
MODE = "prod"

adm_level = 2
iso3 = "AFG"
date = "2024-01-01"
stats = ["mean", "median", "min", "max"]
engine = create_engine(
    f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_PROD}@chd-rasterstats-prod.postgres.database.azure.com/postgres"
)
gdf = gpd.read_file(f"data/{iso3.lower()}/{iso3.lower()}_adm{adm_level}.shp")
gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.001, preserve_topology=True)
```

## 1. ERA5


Read in the ERA5 COG

```python
# Read in the ERA5 COG
cog_name = f"era5/monthly/processed/precip_reanalysis_v{date}.tif"
cog_url = get_cog_url(MODE, cog_name)
da_era5 = rxr.open_rasterio(cog_url, chunks="auto")
da_era5_upsampled = prep_raster(da_era5, gdf)

# da_era5.rio.to_raster("da_era5.tif")
# da_era5_upsampled.rio.to_raster("da_era5_upsampled.tif")

coords_transform = da_era5_upsampled.rio.set_spatial_dims(
    x_dim="x", y_dim="y"
).rio.transform()
```

#### 1.1: Rasterstats in database

```python
with engine.connect() as con:
    query = text(
        f"SELECT * FROM public.era5 WHERE iso3='{iso3}' AND adm_level='{adm_level}' AND valid_date='{date}'"
    )
    df_db = pd.read_sql_query(query, con)
df_db.rename(columns={"pcode": f"ADM{adm_level}_PCODE"}, inplace=True)
df_db.columns = [
    f"{col}_db" if col != f"ADM{adm_level}_PCODE" else col for col in df_db.columns
]
```

#### 1.2: Calculate comparisons using `exactextract` and `rasterstats` packages

```python
df_ee = exact_extract(
    da_era5,
    gdf,
    stats,
    output="pandas",
    include_cols=f"ADM{adm_level}_PCODE",
).sort_values(f"ADM{adm_level}_PCODE")

df_ee.columns = [
    f"{col}_ee" if col != f"ADM{adm_level}_PCODE" else col for col in df_ee.columns
]

results_zs = zonal_stats(
    vectors=gdf[["geometry"]],
    raster=da_era5_upsampled.values[0],
    affine=coords_transform,
    nodata=np.nan,
    all_touched=False,
    stats=stats,
)
df_zs = (
    pd.DataFrame.from_dict(results_zs)
    .join(gdf)[stats + [f"ADM{adm_level}_PCODE"]]
    .sort_values(f"ADM{adm_level}_PCODE")
)
df_zs.columns = [
    f"{col}_zs" if col != f"ADM{adm_level}_PCODE" else col for col in df_zs.columns
]

assert len(df_zs) == len(df_ee) == len(df_db)
```

#### 1.3 Compare using percent differences

```python
df_comparison = df_db.merge(df_ee, on=f"ADM{adm_level}_PCODE").merge(
    df_zs, on=f"ADM{adm_level}_PCODE"
)

for stat in stats:
    df_comparison[f"{stat}_diff_db_ee"] = abs(
        (df_comparison[f"{stat}_ee"] - df_comparison[f"{stat}_db"])
        / ((df_comparison[f"{stat}_ee"] + df_comparison[f"{stat}_db"]) / 2)
        * 100
    ).round(4)

    df_comparison[f"{stat}_diff_db_zs"] = abs(
        (df_comparison[f"{stat}_zs"] - df_comparison[f"{stat}_db"])
        / ((df_comparison[f"{stat}_zs"] + df_comparison[f"{stat}_db"]) / 2)
        * 100
    ).round(4)
```

#### 1.4 Look for any values that are > 5% absolute difference

```python
threshold = 5

diff_cols = [col for col in df_comparison.columns if "_diff_" in col]
significant_diffs = df_comparison[diff_cols].abs().gt(threshold).any(axis=1)
significant_rows = df_comparison[significant_diffs].copy()
max_diffs = significant_rows[diff_cols].abs().max(axis=1)
significant_rows = significant_rows.assign(max_difference=max_diffs)
significant_rows = significant_rows.sort_values("max_difference", ascending=False)

significant_rows
```

# SEAS5

#### Read in the SEAS5 COG

```python
cog_name = f"seas5/monthly/processed/precip_em_i{date}_lt0.tif"
cog_url = get_cog_url(MODE, cog_name)
da_seas5 = rxr.open_rasterio(cog_url, chunks="auto")
da_seas5_upsampled = prep_raster(da_seas5, gdf)

# da_seas5_upsampled.rio.to_raster("da_seas5_upsampled.tif")

coords_transform = da_seas5_upsampled.rio.set_spatial_dims(
    x_dim="x", y_dim="y"
).rio.transform()
```

#### 2.1 Rasterstats in database

```python
with engine.connect() as con:
    query = text(
        f"SELECT * FROM public.seas5 WHERE iso3='{iso3}' AND adm_level='{adm_level}' AND valid_date='{date}' and leadtime=0"
    )
    df_db = pd.read_sql_query(query, con)
df_db.rename(columns={"pcode": f"ADM{adm_level}_PCODE"}, inplace=True)
df_db.columns = [
    f"{col}_db" if col != f"ADM{adm_level}_PCODE" else col for col in df_db.columns
]
```

#### 2.2 Calculate comparisons using `exactextract` and `rasterstats` packages

```python
df_ee = exact_extract(
    da_seas5,
    gdf,
    stats,
    output="pandas",
    include_cols=f"ADM{adm_level}_PCODE",
).sort_values(f"ADM{adm_level}_PCODE")

df_ee.columns = [
    f"{col}_ee" if col != f"ADM{adm_level}_PCODE" else col for col in df_ee.columns
]

results_zs = zonal_stats(
    vectors=gdf[["geometry"]],
    raster=da_seas5_upsampled.values[0],
    affine=coords_transform,
    nodata=np.nan,
    all_touched=False,
    stats=stats,
)
df_zs = (
    pd.DataFrame.from_dict(results_zs)
    .join(gdf)[stats + [f"ADM{adm_level}_PCODE"]]
    .sort_values(f"ADM{adm_level}_PCODE")
)
df_zs.columns = [
    f"{col}_zs" if col != f"ADM{adm_level}_PCODE" else col for col in df_zs.columns
]

assert len(df_zs) == len(df_ee) == len(df_db)
```

#### 2.3 Compare using percent differences

```python
df_comparison = df_db.merge(df_ee, on=f"ADM{adm_level}_PCODE").merge(
    df_zs, on=f"ADM{adm_level}_PCODE"
)

for stat in stats:
    df_comparison[f"{stat}_diff_db_ee"] = abs(
        (df_comparison[f"{stat}_ee"] - df_comparison[f"{stat}_db"])
        / ((df_comparison[f"{stat}_ee"] + df_comparison[f"{stat}_db"]) / 2)
        * 100
    ).round(4)

    df_comparison[f"{stat}_diff_db_zs"] = abs(
        (df_comparison[f"{stat}_zs"] - df_comparison[f"{stat}_db"])
        / ((df_comparison[f"{stat}_zs"] + df_comparison[f"{stat}_db"]) / 2)
        * 100
    ).round(4)
```

#### 2.4 Look for any values that are > 5% absolute difference

```python
threshold = 5

diff_cols = [col for col in df_comparison.columns if "_diff_" in col]
significant_diffs = df_comparison[diff_cols].abs().gt(threshold).any(axis=1)
significant_rows = df_comparison[significant_diffs].copy()
max_diffs = significant_rows[diff_cols].abs().max(axis=1)
significant_rows = significant_rows.assign(max_difference=max_diffs)
significant_rows = significant_rows.sort_values("max_difference", ascending=False)

significant_rows
```

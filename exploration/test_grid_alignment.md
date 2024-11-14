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

## Grid alignment investigation

This notebook investigates the source of some grid misalignment identified during preparation of the source raster data. We're looking at a sample SEAS5 COG and demonstrating how the order of clipping and resampling operations can impact the output raster stats.

Update: After further investigation, see [this comment](https://github.com/OCHA-DAP/ds-raster-stats/pull/13#issuecomment-2463353673) for a potential solution.

```python
from src.utils.raster_utils import upsample_raster
from src.utils.cog_utils import get_cog_url

import rioxarray as rxr
import geopandas as gpd
from rasterstats import zonal_stats
import numpy as np
import pandas as pd

MODE = "prod"
date = "2024-01-01"
```

#### 1. Read in source raster and geopandas dataframe

```python
# Or replace with other locally saved shapefile
gdf = gpd.read_file("data/eth_adm2_simplified.shp")
minx, miny, maxx, maxy = gdf.total_bounds

cog_name = f"seas5/monthly/processed/precip_em_i{date}_lt0.tif"
cog_url = get_cog_url(MODE, cog_name)
da_seas5 = rxr.open_rasterio(cog_url, chunks="auto")
```

#### Option A: 1) clip raster, then 2) upsample to 0.05 degree resolution

```python
da_1 = da_seas5.sel(x=slice(minx, maxx), y=slice(maxy, miny)).persist()
da_1 = upsample_raster(da_1)

print(da_1.rio.transform())
print(da_1.rio.resolution())
```

#### Option B: 1) Upsample to 0.05 degrees, then 2) clip raster

```python
da_2 = upsample_raster(da_seas5)
da_2 = da_2.sel(x=slice(minx, maxx), y=slice(maxy, miny)).persist()

print(da_2.rio.transform())
print(da_2.rio.resolution())
```

#### Now compare output zonal stats from both `da_1` and `da_2`

```python
stats = ["mean", "median", "min", "max", "count"]

da_1_results = zonal_stats(
    vectors=gdf[["geometry"]],
    raster=da_1.values[0],
    affine=da_1.rio.transform(),
    nodata=np.nan,
    all_touched=False,
    stats=stats,
)

da_2_results = zonal_stats(
    vectors=gdf[["geometry"]],
    raster=da_2.values[0],
    affine=da_2.rio.transform(),
    nodata=np.nan,
    all_touched=False,
    stats=stats,
)

df_1 = pd.DataFrame(da_1_results)
df_2 = pd.DataFrame(da_2_results)
```

We can see different results between both dataframes...

```python
df_1
```

```python
df_2
```

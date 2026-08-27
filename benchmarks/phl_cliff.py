"""Why does the PR #49 kernel jump at 125 slices in the PHL reproduction?

At 100 slices it takes 0.0096s, at 125 it takes 0.0762s -- 8x the time
for 25% more data. Either a real cliff in the kernel, or contention
during the original run (that measurement was last in its sweep and the
machine had other jobs on it).

This replays only the kernel, no network, on the real PHL ADM2 geometry
against a synthetic raster matching the real clip (66 x 49 at 0.25 deg),
sweeping slice counts finely around the suspect point. Tiny memory.
"""

import os
import sys
import time

sys.path.insert(0, "/Users/tdowning/OCHA/repos/ds-raster-stats")
os.chdir("/Users/tdowning/OCHA/repos/ds-raster-stats")

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import rioxarray  # noqa: F401,E402
import xarray as xr  # noqa: E402

from src.utils.iso3_utils import load_shp_cached  # noqa: E402
from src.utils.zonal_utils import build_weights, weighted_zonal_stats  # noqa: E402

# the real PHL ERA5 clip from the reproduction
NY, NX, RES = 66, 49, 0.25
X0, Y0 = 116.0, 21.5

shp = load_shp_cached("PHL", "prod")
gdf = gpd.read_file(f"{shp}/phl_adm2.shp")
print(f"[cliff] PHL ADM2 polygons: {len(gdf)}", flush=True)

rng = np.random.default_rng(3)


def grid(n):
    da = xr.DataArray(
        rng.gamma(2.0, 50.0, size=(n, NY, NX)),
        dims=["date", "y", "x"],
        coords={
            "date": pd.date_range(end="2024-12-01", periods=n, freq="MS"
                                  ).strftime("%Y-%m-%d"),
            "x": X0 + RES * (np.arange(NX) + 0.5),
            "y": Y0 - RES * (np.arange(NY) + 0.5),
        },
    )
    da.rio.write_crs("EPSG:4326", inplace=True)
    return da


base = grid(1)
W = build_weights(gdf, "ADM2_PCODE", base)
print(f"[cliff] weight matrix: {W.shape}, nnz={W.nnz}", flush=True)

cube_max = rng.gamma(2.0, 50.0, size=(220, NY, NX))
weighted_zonal_stats(cube_max[:5], W, 25.0)  # warm

print("[cliff] slices   min(s)    median(s)   per-slice(ms)", flush=True)
for n in [25, 50, 75, 100, 110, 120, 124, 125, 126, 130, 150, 200]:
    cube = np.ascontiguousarray(cube_max[:n])
    ts = []
    for _ in range(9):
        t0 = time.perf_counter()
        weighted_zonal_stats(cube, W, 25.0)
        ts.append(time.perf_counter() - t0)
    ts.sort()
    print(
        f"[cliff] {n:6}  {ts[0]:8.4f}  {ts[len(ts)//2]:10.4f}  "
        f"{ts[0] / n * 1000:12.3f}",
        flush=True,
    )
print("[cliff] DONE", flush=True)

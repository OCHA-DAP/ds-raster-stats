"""2-D scaling benchmark: admin units x date-slices, simultaneously.

The earlier sweeps varied one axis at a time. This walks the full grid so
the interaction is visible -- in particular whether the speedup is a
product of two independent effects or whether one axis dominates.

Uses the REAL runners (legacy fast_zonal_stats_runner vs the PR's
zonal_stats_runner), not just the kernels, so per-row validation and
DataFrame assembly are included. Geometry is synthetic (a jittered grid
of polygons over a Nigeria-sized bbox) so the admin count can be dialled
freely; polygons are deliberately not pixel-aligned, so exactextract does
real fractional work.

Runs anywhere: laptop or Databricks. Results to JSON.
"""

import json
import os
import platform
import shutil
import sys
import time

try:
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _root = os.getcwd()
sys.path.insert(0, _root)
os.chdir(_root)

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import rioxarray  # noqa: F401,E402
import xarray as xr  # noqa: E402
from shapely.geometry import box  # noqa: E402

import src.utils.raster_utils as raster_utils  # noqa: E402
import src.utils.zonal_utils as zonal_utils  # noqa: E402
from src.utils.raster_utils import (  # noqa: E402
    fast_zonal_stats_runner,
    upsample_raster,
)
from src.utils.zonal_utils import zonal_stats_runner  # noqa: E402

# The legacy per-row validator has no float tolerance, so an admin unit
# lying entirely inside one native pixel (all upsampled subpixels share a
# value => min == max exactly, but nanmean is off by an ulp) makes it
# raise. Never observed in prod (0 of 2,595 qa rows) but easy to hit with
# a synthetic grid. Clamp before validating so the benchmark measures the
# same work without the spurious raise. The PR's vectorised validator
# already carries this tolerance.
_orig_validate = raster_utils.validate_stats


def _tolerant_validate(iso3, stats):
    lo, hi = stats.get("min"), stats.get("max")
    if lo == lo and hi == hi:  # both non-NaN
        for k in ("mean", "median"):
            v = stats.get(k)
            if v == v:
                if v < lo:
                    stats[k] = lo
                elif v > hi:
                    stats[k] = hi
    return _orig_validate(iso3, stats)


raster_utils.validate_stats = _tolerant_validate

OUT = os.environ.get("BENCH2D_OUT", "/dbfs/tmp/rasterstats_bench_2d.json")

# Nigeria-sized bbox on the SEAS5 grid: 0.4 deg native, ~30 x 25 pixels
RES = 0.4
NX, NY = 30, 25
X0, Y0 = 2.6, 14.0

N_ADMINS = [10, 50, 200, 800, 3000]
N_DATES = [1, 10, 50, 200, 700]


def log(m):
    print(f"[2d] {m}", flush=True)


def make_raster(n_slices):
    rng = np.random.default_rng(11)
    data = rng.gamma(2.0, 50.0, size=(n_slices, NY, NX))
    da = xr.DataArray(
        data,
        dims=["date", "y", "x"],
        # daily and ending in the past: the legacy validator rejects a
        # valid_date after today, and 700 monthly slices would run into
        # the 2050s
        coords={
            "date": pd.date_range(
                end="2024-12-31", periods=n_slices, freq="D"
            ).strftime("%Y-%m-%d"),
            "x": X0 + RES * (np.arange(NX) + 0.5),
            "y": Y0 - RES * (np.arange(NY) + 0.5),
        },
    )
    da.rio.write_crs("EPSG:4326", inplace=True)
    return da


def make_admins(n):
    """Jittered grid of polygons over the bbox -- not pixel-aligned."""
    rng = np.random.default_rng(7)
    k = int(np.ceil(np.sqrt(n)))
    xs = np.linspace(X0, X0 + RES * NX, k + 1)
    ys = np.linspace(Y0 - RES * NY, Y0, k + 1)
    polys, codes = [], []
    for i in range(k):
        for j in range(k):
            if len(polys) >= n:
                break
            jx = rng.uniform(-0.3, 0.3) * (xs[1] - xs[0])
            jy = rng.uniform(-0.3, 0.3) * (ys[1] - ys[0])
            polys.append(
                box(
                    xs[i],
                    ys[j],
                    min(xs[i + 1] + jx, xs[-1]),
                    min(ys[j + 1] + jy, ys[-1]),
                )
            )
            codes.append(f"Z{len(polys):05d}")
    return gpd.GeoDataFrame(
        {"geometry": polys, "ADM2_PCODE": codes}, crs="EPSG:4326"
    )


def time_it(fn, reps):
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return min(ts)


results = {
    "machine": {
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
    },
    "grid": {"n_admins": N_ADMINS, "n_dates": N_DATES},
    "native_grid": f"{NY}x{NX} at {RES} deg",
    "cells": [],
}

for nd in N_DATES:
    da = make_raster(nd)
    da_up = upsample_raster(da)  # legacy path works on the upsampled cube
    for na in N_ADMINS:
        gdf = make_admins(na)
        reps = 3 if (na * nd) <= 20000 else 1

        # legacy: upsample already done above (shared, as in prep_raster);
        # time only the stats runner, matching the published split
        fast_zonal_stats_runner(da_up, gdf.copy(), 2, "TST")  # warm
        t_leg = time_it(
            lambda: fast_zonal_stats_runner(da_up, gdf.copy(), 2, "TST"), reps
        )

        shutil.rmtree(zonal_utils.WEIGHTS_CACHE_DIR, ignore_errors=True)
        zonal_stats_runner(da, gdf, 2, "TST")  # warm + build weights
        t_new = time_it(lambda: zonal_stats_runner(da, gdf, 2, "TST"), reps)

        cell = {
            "n_admins": na,
            "n_dates": nd,
            "legacy_s": round(t_leg, 4),
            "new_s": round(t_new, 4),
            "speedup": round(t_leg / t_new, 2),
            "rows": na * nd,
        }
        results["cells"].append(cell)
        log(
            f"admins={na:5} dates={nd:4}: legacy {t_leg:8.3f}s  "
            f"new {t_new:7.4f}s  speedup {cell['speedup']:6.1f}x"
        )
        json.dump(results, open(OUT, "w"), indent=1)

log("DONE")
print("RESULTS_JSON_BEGIN")
print(json.dumps(results))
print("RESULTS_JSON_END")

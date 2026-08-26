"""One-off Databricks benchmark for the weighted zonal-stats method
(PR #49): the laptop benchmarks rerun on job compute + Azure-internal
networking. Read-only against prod blob; writes nothing to any DB.

Scenarios (mirrors the local harness published at the repo's Pages site
under /benchmarks/):
  A. update run   -- 1 SEAS5 issuance, 5 countries, ADM0-2
  B. archival     -- 100 issuances x 7 leadtimes, BDI/TCD/NGA,
                     prep (fetch) vs stats split
  C. admin scale  -- synthetic kernel, 10..3000 admin units
  D. stacking     -- serial vs threaded COG opens (disjoint date ranges
                     per mode so GDAL caching cannot favour the second run)

Results: JSON printed between RESULTS_JSON_BEGIN/END markers and written
to /dbfs/tmp/rasterstats_dbx_bench.json.
"""

import json
import os
import platform
import shutil
import sys
import time

# __file__ is undefined under Databricks spark_python_task; the git_source
# checkout root is the working directory there
try:
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _root = os.getcwd()
sys.path.insert(0, _root)

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import sparse  # noqa: E402

import src.utils.cog_utils as cog_utils  # noqa: E402
import src.utils.zonal_utils as zonal_utils  # noqa: E402
from src.utils.cog_utils import stack_cogs  # noqa: E402
from src.utils.iso3_utils import load_shp_cached  # noqa: E402
from src.utils.raster_utils import (  # noqa: E402
    fast_zonal_stats,
    fast_zonal_stats_runner,
    prep_raster,
)
from src.utils.zonal_utils import (  # noqa: E402
    clip_raster,
    weighted_zonal_stats,
    zonal_stats_runner,
)

results = {
    "machine": {
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
        "environment": "databricks-job-compute",
        "node_type": os.getenv("DB_NODE_TYPE", "unknown"),
    }
}


def log(msg):
    print(f"[bench] {msg}", flush=True)


def run_legacy(ds, shp_dir, iso3, max_adm=2):
    t0 = time.perf_counter()
    gdf0 = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm0.shp")
    ds_up = prep_raster(ds, gdf0)
    t_prep = time.perf_counter() - t0
    t0 = time.perf_counter()
    for lvl in range(max_adm + 1):
        gdf = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm{lvl}.shp")
        fast_zonal_stats_runner(ds_up, gdf, lvl, iso3)
    return {"prep": t_prep, "stats": time.perf_counter() - t0}


def run_new(ds, shp_dir, iso3, max_adm=2):
    t0 = time.perf_counter()
    gdf0 = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm0.shp")
    ds_clip = clip_raster(ds, gdf0)
    t_prep = time.perf_counter() - t0
    t0 = time.perf_counter()
    for lvl in range(max_adm + 1):
        gdf = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm{lvl}.shp")
        zonal_stats_runner(ds_clip, gdf, lvl, iso3)
    return {"prep": t_prep, "stats": time.perf_counter() - t0}


# ---------------------------------------------------------------- D
# Stacking first (cold caches). Disjoint date ranges per mode.
log("D: stacking serial vs threaded...")
real_tpe = cog_utils.ThreadPoolExecutor


class SerialTPE(real_tpe):
    def __init__(self, max_workers=None):
        super().__init__(max_workers=1)


stacking = {}
for dataset, ranges in [
    (
        "seas5",
        {
            "threaded": pd.date_range("2022-01-01", "2022-07-01", freq="MS"),
            "serial": pd.date_range("2021-01-01", "2021-07-01", freq="MS"),
        },
    ),
    (
        "imerg",
        {
            "threaded": pd.date_range("2025-03-01", "2025-03-30", freq="D"),
            "serial": pd.date_range("2025-02-01", "2025-02-28", freq="D"),
        },
    ),
]:
    stacking[dataset] = {}
    for mode, dates in ranges.items():
        cog_utils.ThreadPoolExecutor = (
            SerialTPE if mode == "serial" else real_tpe
        )
        t0 = time.perf_counter()
        ds_tmp = stack_cogs(list(dates), dataset, "prod")
        dt = time.perf_counter() - t0
        n = len(dates)
        stacking[dataset][mode] = {
            "seconds": round(dt, 2),
            "n_dates": n,
            "s_per_cog": round(dt / max(len(ds_tmp.date), 1), 3),
        }
        log(f"D {dataset} {mode}: {dt:.1f}s for {n} dates")
        del ds_tmp
cog_utils.ThreadPoolExecutor = real_tpe
results["stacking"] = stacking

# ---------------------------------------------------------------- A
log("A: update-run scenario...")
ds1 = stack_cogs([pd.Timestamp("2026-07-01")], "seas5", "prod")
scen_a = {}
for iso3 in ["BDI", "TCD", "SSD", "ETH", "NGA"]:
    shp_dir = load_shp_cached(iso3, "prod")
    legacy = run_legacy(ds1, shp_dir, iso3)
    shutil.rmtree(zonal_utils.WEIGHTS_CACHE_DIR, ignore_errors=True)
    new_cold = run_new(ds1, shp_dir, iso3)
    new_warm = run_new(ds1, shp_dir, iso3)
    scen_a[iso3] = {
        "legacy": legacy,
        "new_cold": new_cold,
        "new_warm": new_warm,
    }
    log(
        f"A {iso3}: legacy {legacy['prep']+legacy['stats']:.1f}s, "
        f"cold {new_cold['prep']+new_cold['stats']:.1f}s, "
        f"warm {new_warm['prep']+new_warm['stats']:.1f}s"
    )
results["update_run"] = scen_a
del ds1

# ---------------------------------------------------------------- B
log("B: archival chunk, stacking 100 issuances...")
t0 = time.perf_counter()
dates100 = list(pd.date_range("2016-01-01", "2024-04-01", freq="MS"))
ds100 = stack_cogs(dates100, "seas5", "prod")
t_stack = time.perf_counter() - t0
log(f"B: stacked in {t_stack:.1f}s")
scen_b = {"stack_seconds": round(t_stack, 1)}
for iso3 in ["BDI", "TCD", "NGA"]:
    shp_dir = load_shp_cached(iso3, "prod")
    legacy = run_legacy(ds100, shp_dir, iso3)
    log(
        f"B {iso3} legacy: prep {legacy['prep']:.1f}s "
        f"stats {legacy['stats']:.1f}s"
    )
    new_warm = run_new(ds100, shp_dir, iso3)
    log(
        f"B {iso3} new: prep {new_warm['prep']:.1f}s "
        f"stats {new_warm['stats']:.1f}s"
    )
    scen_b[iso3] = {"legacy": legacy, "new_warm": new_warm}
results["archival_chunk"] = scen_b
del ds100

# ---------------------------------------------------------------- C
log("C: admin scaling...")
rng = np.random.default_rng(42)
NAT_H, NAT_W, UP = 25, 30, 8
UP_H, UP_W = NAT_H * UP, NAT_W * UP
cube = rng.gamma(2.0, 50.0, size=(100, NAT_H, NAT_W))
scen_c = []
for n_adms in [10, 100, 774, 3000]:
    seed_y = rng.uniform(0, UP_H, n_adms)
    seed_x = rng.uniform(0, UP_W, n_adms)
    yy, xx = np.mgrid[0:UP_H, 0:UP_W]
    admin_up = (
        ((yy[..., None] - seed_y) ** 2 + (xx[..., None] - seed_x) ** 2)
        .argmin(axis=2)
        .astype(float)
    )
    ny = np.arange(UP_H) // UP
    nx = np.arange(UP_W) // UP
    native_id = (ny[:, None] * NAT_W + nx[None, :]).ravel()
    W = sparse.coo_matrix(
        (
            np.full(UP_H * UP_W, 1 / UP**2),
            (admin_up.ravel().astype(int), native_id),
        ),
        shape=(n_adms, NAT_H * NAT_W),
    ).tocsr()
    up = np.repeat(np.repeat(cube[0], UP, axis=0), UP, axis=1)
    fast_zonal_stats(up, admin_up, n_adms)
    weighted_zonal_stats(cube[:5], W, scale=UP**2)
    t_leg = []
    for _ in range(3):
        t0 = time.perf_counter()
        for k in range(100):
            up = np.repeat(np.repeat(cube[k], UP, axis=0), UP, axis=1)
            fast_zonal_stats(up, admin_up, n_adms)
        t_leg.append(time.perf_counter() - t0)
    t_new = []
    for _ in range(3):
        t0 = time.perf_counter()
        weighted_zonal_stats(cube, W, scale=UP**2)
        t_new.append(time.perf_counter() - t0)
    scen_c.append(
        {
            "n_adms": n_adms,
            "legacy": round(min(t_leg), 3),
            "new": round(min(t_new), 4),
        }
    )
    log(f"C n_adms={n_adms}: legacy {min(t_leg):.3f}s, new {min(t_new):.4f}s")
results["admin_scaling"] = {
    "n_slices": 100,
    "timing": "best of 3 after warmup",
    "rows": scen_c,
}

# ---------------------------------------------------------------- out
payload = json.dumps(results, indent=1)
print("RESULTS_JSON_BEGIN")
print(payload)
print("RESULTS_JSON_END")
try:
    with open("/dbfs/tmp/rasterstats_dbx_bench.json", "w") as f:
        f.write(payload)
    log("wrote /dbfs/tmp/rasterstats_dbx_bench.json")
except Exception as e:
    log(f"dbfs write failed (non-fatal): {e}")
log("DONE")

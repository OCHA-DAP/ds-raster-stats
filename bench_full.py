"""Speed benchmarks: legacy (upsample + per-slice rasterize) vs new
(exactextract weight-matrix) zonal stats in ds-raster-stats.

Real prod COGs + COD boundaries; no DB writes (the write path is
identical in both methods). Results -> JSON for the GH Pages report.
"""

import json
import os
import platform
import shutil
import sys
import time

os.environ["DSCI_AZ_BLOB_PROD_SAS_WRITE"] = os.environ[
    "DSCI_AZ_BLOB_PROD_SAS"
]

sys.path.insert(0, "/Users/tdowning/OCHA/repos/ds-raster-stats")
os.chdir("/Users/tdowning/OCHA/repos/ds-raster-stats")

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

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
    build_weights,
    clip_raster,
    weighted_zonal_stats,
    zonal_stats_runner,
)

OUT = (
    "/private/tmp/claude-501/-Users-tdowning-OCHA-repos/"
    "440241f8-24d0-449a-9fe3-8a498a385bb7/scratchpad/bench_results.json"
)

results = {
    "machine": {
        "platform": platform.platform(),
        "processor": platform.machine(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
    },
    "run_date": "2026-08-26",
}


def log(msg):
    print(f"[bench] {msg}", flush=True)


def clear_weights_cache():
    shutil.rmtree(zonal_utils.WEIGHTS_CACHE_DIR, ignore_errors=True)


def run_legacy(ds, shp_dir, iso3, max_adm=2):
    t0 = time.perf_counter()
    gdf0 = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm0.shp")
    ds_up = prep_raster(ds, gdf0)
    t_prep = time.perf_counter() - t0
    t0 = time.perf_counter()
    n_rows = 0
    for lvl in range(max_adm + 1):
        gdf = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm{lvl}.shp")
        df = fast_zonal_stats_runner(ds_up, gdf, lvl, iso3)
        n_rows += len(df)
    t_stats = time.perf_counter() - t0
    return {"prep": t_prep, "stats": t_stats, "rows": n_rows}


def run_new(ds, shp_dir, iso3, max_adm=2):
    t0 = time.perf_counter()
    gdf0 = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm0.shp")
    ds_clip = clip_raster(ds, gdf0)
    t_prep = time.perf_counter() - t0
    t0 = time.perf_counter()
    n_rows = 0
    for lvl in range(max_adm + 1):
        gdf = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm{lvl}.shp")
        df = zonal_stats_runner(ds_clip, gdf, lvl, iso3)
        n_rows += len(df)
    t_stats = time.perf_counter() - t0
    return {"prep": t_prep, "stats": t_stats, "rows": n_rows}


# ---------------------------------------------------------------- A
# Update-run scenario: one SEAS5 issuance (7 leadtimes), ADM0-2
COUNTRIES = ["BDI", "TCD", "SSD", "ETH", "NGA"]
log("A: stacking 1 issuance...")
ds1 = stack_cogs([pd.Timestamp("2026-07-01")], "seas5", "prod")

scen_a = {}
for iso3 in COUNTRIES:
    shp_dir = load_shp_cached(iso3, "prod")
    legacy = run_legacy(ds1, shp_dir, iso3)
    clear_weights_cache()
    new_cold = run_new(ds1, shp_dir, iso3)
    new_warm = run_new(ds1, shp_dir, iso3)
    n_adm2 = len(gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm2.shp"))
    scen_a[iso3] = {
        "n_adm2": n_adm2,
        "legacy": legacy,
        "new_cold": new_cold,
        "new_warm": new_warm,
    }
    log(
        f"A {iso3}: legacy {legacy['prep']+legacy['stats']:.1f}s, "
        f"new cold {new_cold['prep']+new_cold['stats']:.1f}s, "
        f"warm {new_warm['prep']+new_warm['stats']:.1f}s"
    )
results["update_run"] = scen_a
json.dump(results, open(OUT, "w"), indent=1)

# ---------------------------------------------------------------- B
# Archival-chunk scenario: 100 issuances x 7 leadtimes, ADM0-2
log("B: stacking 100 issuances...")
t0 = time.perf_counter()
dates100 = list(pd.date_range("2016-01-01", "2024-04-01", freq="MS"))
ds100 = stack_cogs(dates100, "seas5", "prod")
t_stack100 = time.perf_counter() - t0
log(f"B: stacked 700 COGs in {t_stack100:.1f}s")

scen_b = {"stack_seconds": t_stack100}
for iso3 in ["BDI", "TCD", "NGA"]:
    shp_dir = load_shp_cached(iso3, "prod")
    legacy = run_legacy(ds100, shp_dir, iso3)
    log(f"B {iso3}: legacy {legacy['prep']+legacy['stats']:.1f}s")
    new_warm = run_new(ds100, shp_dir, iso3)  # cache warm from A
    scen_b[iso3] = {"legacy": legacy, "new_warm": new_warm}
    log(f"B {iso3}: new warm {new_warm['prep']+new_warm['stats']:.1f}s")
results["archival_chunk"] = scen_b
json.dump(results, open(OUT, "w"), indent=1)
del ds100

# ---------------------------------------------------------------- C
# Admin-count scaling (synthetic, NGA-like grid): legacy core vs new
# core as the number of admin units grows
log("C: admin scaling...")
rng = np.random.default_rng(42)
NAT_H, NAT_W, UP = 25, 30, 8
UP_H, UP_W = NAT_H * UP, NAT_W * UP
N_SLICES = 100
cube = rng.gamma(2.0, 50.0, size=(N_SLICES, NAT_H, NAT_W))

scen_c = []
for n_adms in [10, 100, 774, 3000]:
    seed_y = rng.uniform(0, UP_H, n_adms)
    seed_x = rng.uniform(0, UP_W, n_adms)
    yy, xx = np.mgrid[0:UP_H, 0:UP_W]
    d2 = (yy[..., None] - seed_y) ** 2 + (xx[..., None] - seed_x) ** 2
    admin_up = d2.argmin(axis=2).astype(float)

    t0 = time.perf_counter()
    for k in range(N_SLICES):
        up = np.repeat(np.repeat(cube[k], UP, axis=0), UP, axis=1)
        fast_zonal_stats(up, admin_up, n_adms)
    t_legacy = time.perf_counter() - t0

    # aggregate the upsampled assignment to native coverage weights
    from scipy import sparse

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
    t0 = time.perf_counter()
    weighted_zonal_stats(cube, W, scale=UP**2)
    t_new = time.perf_counter() - t0

    scen_c.append(
        {"n_adms": n_adms, "legacy": t_legacy, "new": t_new}
    )
    log(f"C n_adms={n_adms}: legacy {t_legacy:.2f}s, new {t_new:.3f}s")
results["admin_scaling"] = {
    "n_slices": N_SLICES,
    "grid": f"{NAT_H}x{NAT_W} native (0.4deg), {UP}x upsample",
    "rows": scen_c,
}
json.dump(results, open(OUT, "w"), indent=1)

# ---------------------------------------------------------------- D
# COG stacking: threaded (new) vs serial (old behaviour)
log("D: stacking benchmark...")
dates7 = list(pd.date_range("2023-01-01", "2023-07-01", freq="MS"))

real_tpe = cog_utils.ThreadPoolExecutor


class SerialTPE(real_tpe):
    def __init__(self, max_workers=None):
        super().__init__(max_workers=1)


t0 = time.perf_counter()
stack_cogs(dates7, "seas5", "prod")
t_threaded = time.perf_counter() - t0
cog_utils.ThreadPoolExecutor = SerialTPE
t0 = time.perf_counter()
stack_cogs(dates7, "seas5", "prod")
t_serial = time.perf_counter() - t0
cog_utils.ThreadPoolExecutor = real_tpe
results["stacking"] = {
    "n_cogs": 49,
    "serial": t_serial,
    "threaded": t_threaded,
}
log(f"D: 49 cogs serial {t_serial:.1f}s, threaded {t_threaded:.1f}s")

json.dump(results, open(OUT, "w"), indent=1)
log("DONE")

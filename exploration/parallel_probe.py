"""Where is the parallelism actually available?

After the method change, ~94% of archival per-country time is the lazy
fetch in clip_raster (each country pulls its window from all 700 remote
COGs). Threading the COG *opens* was already measured as useless -- opens
are lazy. This probes the parts that were never measured:

  P1. Does the fetch itself parallelize? dask threaded scheduler,
      1 vs 2 vs 4 vs 8 workers.
  P2. Is the fetch redundant across countries? Persist the stack ONCE
      for a multi-country bbox, then slice per country in memory, versus
      the current per-country remote fetch. The shared arm runs FIRST
      and on a DISJOINT issuance range, so any transfer-layer caching
      works against the saving being claimed rather than for it.
  P3. How does the existing process pool scale over countries?

Read-only against prod blob.
"""

import json
import os
import sys
import time
from multiprocessing import Pool

# get_cog_url reads the _WRITE SAS even for reads; this shim only
# satisfies that import-time expectation for a read-only script and
# should not be copied into anything that actually writes.
os.environ.setdefault(
    "DSCI_AZ_BLOB_PROD_SAS_WRITE", os.environ.get("DSCI_AZ_BLOB_PROD_SAS", "")
)

sys.path.insert(0, os.getcwd())

import dask  # noqa: E402
import geopandas as gpd  # noqa: E402
import pandas as pd  # noqa: E402

from src.utils.cog_utils import stack_cogs  # noqa: E402
from src.utils.iso3_utils import load_shp_cached  # noqa: E402
from src.utils.zonal_utils import clip_raster, zonal_stats_runner  # noqa: E402

SCRATCH = os.environ.get("BENCH_OUT_DIR", "/dbfs/tmp")
DATES = list(
    pd.date_range("2020-01-01", "2023-12-01", freq="MS")
)  # 48 issuances
COUNTRIES = ["BDI", "TCD", "NGA", "ETH"]
results = {"n_issuances": len(DATES), "n_cogs": len(DATES) * 7}


def log(m):
    print(f"[par] {m}", flush=True)


def fetch_one(iso3, ds):
    shp = load_shp_cached(iso3, "prod")
    gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
    t0 = time.perf_counter()
    clip = clip_raster(ds, gdf0)
    _ = clip.values  # force the pull
    return time.perf_counter() - t0, clip


# ---------------------------------------------------------------- P1
log("P1: does the fetch parallelize with dask threads?")
p1 = {}
for nthreads in [1, 2, 4, 8]:
    with dask.config.set(scheduler="threads", num_workers=nthreads):
        ds = stack_cogs(DATES, "seas5", "prod")
        dt, _ = fetch_one("BDI", ds)
    p1[nthreads] = round(dt, 2)
    log(f"P1 dask threads={nthreads}: fetch {dt:.1f}s")
    del ds
results["p1_dask_threads_fetch_seconds"] = p1
base = p1[1]
results["p1_speedup_vs_1_thread"] = {
    k: round(base / v, 2) for k, v in p1.items()
}
json.dump(results, open(f"{SCRATCH}/parallel_probe.json", "w"), indent=1)

# ---------------------------------------------------------------- P2
log("P2: is the per-country fetch redundant?")
# Disjoint issuance ranges per arm, and the shared arm first, so that
# transfer-layer caching cannot inflate the saving this measures.
DATES_SHARED = list(pd.date_range("2016-01-01", "2019-12-01", freq="MS"))
DATES_PERCOUNTRY = DATES

bounds = []
for iso3 in COUNTRIES:
    shp = load_shp_cached(iso3, "prod")
    bounds.append(gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp").total_bounds)
b = pd.DataFrame(bounds, columns=["minx", "miny", "maxx", "maxy"])
union = gpd.GeoDataFrame(
    geometry=gpd.GeoSeries.from_wkt(
        [
            "POLYGON(({0} {1},{2} {1},{2} {3},{0} {3},{0} {1}))".format(
                b["minx"].min(),
                b["miny"].min(),
                b["maxx"].max(),
                b["maxy"].max(),
            )
        ]
    ),
    crs="EPSG:4326",
)
ds_shared = stack_cogs(DATES_SHARED, "seas5", "prod")
t0 = time.perf_counter()
shared = clip_raster(ds_shared, union)
_ = shared.values
t_shared = time.perf_counter() - t0
log(f"P2 single shared fetch (union bbox, cold range): {t_shared:.1f}s")

t0 = time.perf_counter()
for iso3 in COUNTRIES:
    shp = load_shp_cached(iso3, "prod")
    gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
    sub = clip_raster(shared, gdf0)
    _ = sub.values
t_slice = time.perf_counter() - t0
log(f"P2 slicing {len(COUNTRIES)} countries out of memory: {t_slice:.1f}s")
del ds_shared, shared

ds = stack_cogs(DATES_PERCOUNTRY, "seas5", "prod")
per_country = {}
for iso3 in COUNTRIES:
    dt, _ = fetch_one(iso3, ds)
    per_country[iso3] = round(dt, 2)
    log(f"P2 per-country fetch {iso3}: {dt:.1f}s")
del ds

results["p2_note"] = (
    "shared arm run first on a disjoint issuance range so transfer "
    "caching biases against the saving"
)
results["p2_per_country_fetch_seconds"] = per_country
results["p2_per_country_total"] = round(sum(per_country.values()), 2)
results["p2_shared_fetch_seconds"] = round(t_shared, 2)
results["p2_inmemory_slice_seconds"] = round(t_slice, 2)
results["p2_total_shared"] = round(t_shared + t_slice, 2)
results["p2_saving_factor"] = round(
    results["p2_per_country_total"] / (t_shared + t_slice), 2
)
json.dump(results, open(f"{SCRATCH}/parallel_probe.json", "w"), indent=1)


# ---------------------------------------------------------------- P3
def worker(iso3):
    t0 = time.perf_counter()
    ds_w = stack_cogs(DATES, "seas5", "prod")
    shp = load_shp_cached(iso3, "prod")
    gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
    clip = clip_raster(ds_w, gdf0)
    for lvl in [0, 1, 2]:
        try:
            gdf = gpd.read_file(f"{shp}/{iso3.lower()}_adm{lvl}.shp")
        except Exception:
            continue
        zonal_stats_runner(clip, gdf, lvl, iso3)
    return time.perf_counter() - t0


log("P3: process-pool scaling over countries")
p3 = {}
for nproc in [1, 2, 4]:
    t0 = time.perf_counter()
    with Pool(nproc) as pool:
        pool.map(worker, COUNTRIES)
    dt = time.perf_counter() - t0
    p3[nproc] = round(dt, 1)
    log(f"P3 {nproc} processes over {len(COUNTRIES)} countries: {dt:.1f}s")
results["p3_pool_seconds"] = p3
results["p3_speedup_vs_1"] = {k: round(p3[1] / v, 2) for k, v in p3.items()}

json.dump(results, open(f"{SCRATCH}/parallel_probe.json", "w"), indent=1)
log("DONE")

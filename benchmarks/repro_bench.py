"""Reproduce the 2024 Confluence 'Raster stats performance' benchmarks
(retired DSCI Confluence -> Raster Statistics -> Methodology, screenshots
2024-09-11) with the ORIGINAL code from the `exact-extract` branch:

  - compute_zonal_statistics      (baseline: python-rasterstats per date)
  - fast_compute_zonal_statistics (2024 'fast': rasterize-once + numpy)

plus the PR #49 weight-matrix kernel as a third line.

Setup matches the originals as far as they are recoverable:
ERA5 monthly precip at native 0.25 deg (the committed driver uses
prep_raster(upsample=False)), ETH and PHL, ADM2 CODs, sweeps over
N dates (full ADM2 set) and N polygons (fixed 10 dates), best of N
iterations (10 for the fast/new methods; 3 for the rasterstats baseline,
which is too slow for 10).
The rasterize / weight-build steps are outside the timed section for
fast/new respectively, mirroring the original harness where
`admin_raster` was a precomputed argument.
"""

import json
import os
import sys
import time

os.environ["DSCI_AZ_BLOB_PROD_SAS_WRITE"] = os.environ[
    "DSCI_AZ_BLOB_PROD_SAS"
]

SCRATCH = (
    "/private/tmp/claude-501/-Users-tdowning-OCHA-repos/"
    "440241f8-24d0-449a-9fe3-8a498a385bb7/scratchpad"
)
sys.path.insert(0, SCRATCH + "/repro2024")
sys.path.insert(0, "/Users/tdowning/OCHA/repos/ds-raster-stats")
os.chdir("/Users/tdowning/OCHA/repos/ds-raster-stats")

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import raster_utils_2024 as r24  # noqa: E402
from src.utils.cog_utils import stack_cogs  # noqa: E402
from src.utils.iso3_utils import load_shp_cached  # noqa: E402
from src.utils.zonal_utils import (  # noqa: E402
    build_weights,
    weighted_zonal_stats,
)

OUT = SCRATCH + "/repro2024_results.json"
N_DATES_SWEEP = [5, 25, 50, 75, 100, 125]
POLY_DATES = 10
ITER_FAST = 10
ITER_BASE = 3


def log(msg):
    print(f"[repro] {msg}", flush=True)


def best_time(fn, n_iter):
    """Minimum, not mean.

    The mean absorbs contention from anything else running on the box;
    on the first pass that inflated the largest (last-measured) points by
    up to 6x and made the kernel look as though it had a cliff. The
    minimum estimates the uncontended cost, which is what a scaling
    comparison wants.
    """
    times = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times)


log("stacking 125 ERA5 months from prod...")
dates = list(pd.date_range("2014-01-01", "2024-05-01", freq="MS"))
ds = stack_cogs(dates, "era5", "prod")
log(f"stacked: {dict(ds.sizes)}")

results = {"n_dates_sweep": {}, "n_polygons_sweep": {}}

for iso3 in ["eth", "phl"]:
    shp_dir = load_shp_cached(iso3.upper(), "prod")
    gdf0 = gpd.read_file(f"{shp_dir}/{iso3}_adm0.shp")
    gdf2 = gpd.read_file(f"{shp_dir}/{iso3}_adm2.shp")
    # 2024 driver: prep_raster(ds, gdf, upsample=False) -> clip + persist
    ds_clip = r24.prep_raster(ds, gdf0, upsample=False)
    n_poly = len(gdf2)
    log(f"{iso3}: clip {dict(ds_clip.sizes)}, {n_poly} ADM2 polygons")

    transform = ds_clip.rio.transform()
    width, height = ds_clip.rio.width, ds_clip.rio.height
    scale = (abs(ds_clip.rio.resolution()[0]) / 0.05) ** 2

    # untimed, as in the original harness
    admin_raster = r24.rasterize_admin(
        gdf2.copy(), width, height, transform, all_touched=False
    )
    adm_ids = gdf2["ADM2_PCODE"]
    W = build_weights(gdf2, "ADM2_PCODE", ds_clip)
    values_all = np.asarray(ds_clip.values, dtype=np.float64)

    # sanity: baseline and fast agree on means (same centroid rule)
    df_b = r24.compute_zonal_statistics(
        ds_clip.isel(date=slice(0, 3)), gdf2.copy(), "ADM2_PCODE", 2, iso3
    )
    df_f = r24.fast_compute_zonal_statistics(
        ds_clip.isel(date=slice(0, 3)), admin_raster, 2, iso3, adm_ids
    )
    b = df_b.sort_values(["valid_date", "pcode"])["mean"].to_numpy()
    f = df_f.sort_values(["valid_date", "pcode"])["mean"].to_numpy()
    ok = np.isnan(b) == np.isnan(f)
    agree = np.allclose(b[~np.isnan(b)], f[~np.isnan(f)], rtol=1e-6)
    log(f"{iso3}: baseline-vs-fast mean agreement: {agree}")

    rows = []
    for n in N_DATES_SWEEP:
        ds_sub = ds_clip.isel(date=slice(0, n))
        vals_sub = values_all[:n]
        t_base = best_time(
            lambda: r24.compute_zonal_statistics(
                ds_sub, gdf2.copy(), "ADM2_PCODE", 2, iso3
            ),
            ITER_BASE,
        )
        t_fast = best_time(
            lambda: r24.fast_compute_zonal_statistics(
                ds_sub, admin_raster, 2, iso3, adm_ids
            ),
            ITER_FAST,
        )
        t_new = best_time(
            lambda: weighted_zonal_stats(vals_sub, W, scale), ITER_FAST
        )
        rows.append(
            {"n": n, "baseline": t_base, "fast": t_fast, "new": t_new}
        )
        log(
            f"{iso3} dates n={n}: baseline {t_base:.3f}s, "
            f"fast {t_fast:.3f}s, new {t_new:.4f}s"
        )
    results["n_dates_sweep"][iso3] = {
        "n_polygons": n_poly,
        "agreement": bool(agree),
        "rows": rows,
    }
    json.dump(results, open(OUT, "w"), indent=1)

    poly_ns = sorted(set([1, 10, 25, 50, 75, n_poly]))
    poly_ns = [n for n in poly_ns if n <= n_poly]
    ds_sub = ds_clip.isel(date=slice(0, POLY_DATES))
    vals_sub = values_all[:POLY_DATES]
    rows = []
    for n in poly_ns:
        gsub = gdf2.iloc[:n].reset_index(drop=True)
        araster = r24.rasterize_admin(
            gsub.copy(), width, height, transform, all_touched=False
        )
        if np.all(np.isnan(araster)):
            # no pixel centroids in this subset at native resolution;
            # the 2024 fast function cannot handle it
            log(f"{iso3} polys n={n}: no pixel centroids, skipping")
            continue
        wsub = build_weights(gsub, "ADM2_PCODE", ds_clip)
        ids = gsub["ADM2_PCODE"]
        t_base = best_time(
            lambda: r24.compute_zonal_statistics(
                ds_sub, gsub.copy(), "ADM2_PCODE", 2, iso3
            ),
            ITER_BASE,
        )
        t_fast = best_time(
            lambda: r24.fast_compute_zonal_statistics(
                ds_sub, araster, 2, iso3, ids
            ),
            ITER_FAST,
        )
        t_new = best_time(
            lambda: weighted_zonal_stats(vals_sub, wsub, scale), ITER_FAST
        )
        rows.append(
            {"n": n, "baseline": t_base, "fast": t_fast, "new": t_new}
        )
        log(
            f"{iso3} polys n={n}: baseline {t_base:.3f}s, "
            f"fast {t_fast:.3f}s, new {t_new:.4f}s"
        )
    results["n_polygons_sweep"][iso3] = {"n_dates": POLY_DATES, "rows": rows}
    json.dump(results, open(OUT, "w"), indent=1)

log("DONE")

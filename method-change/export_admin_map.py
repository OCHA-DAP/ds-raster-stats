"""Per-admin-unit geometric sensitivity, with geometry, for the map.

geom_sensitivity.py reports one median per country. This emits the value
for EVERY admin unit at the deepest level the pipeline computes, together
with simplified geometry, so the map can show the actual units rather
than country outlines.

Output is quantised and delta-encoded on the cluster so the artefact that
comes down is small enough to serve from a static page:

  rings are integer deltas in units of QUANT degrees, [x0,y0,dx,dy,...]

Writes admin_map.json to $GEOM_OUT_DIR (default /dbfs/tmp on Databricks).
"""

import json
import os
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
from sqlalchemy import create_engine  # noqa: E402

from exploration.geom_sensitivity import (  # noqa: E402
    DATASETS,
    country_grid,
    tvd_rows,
    weights_centroid,
    weights_exact,
)
from src.utils.iso3_utils import load_shp_cached  # noqa: E402

OUT_DIR = os.environ.get(
    "GEOM_OUT_DIR", "/dbfs/tmp" if os.path.isdir("/dbfs/tmp") else "/tmp"
)
QUANT = float(os.environ.get("QUANT", "0.01"))
TOLERANCE = float(os.environ.get("SIMPLIFY_TOLERANCE", "0.03"))
MIN_SPAN = float(os.environ.get("MIN_SPAN", "0.06"))


def log(m):
    print(f"[admin] {m}", flush=True)


def encode(geom):
    """Simplified polygon -> list of delta-encoded integer rings."""
    g = geom.simplify(TOLERANCE, preserve_topology=True)
    if g.is_empty:
        g = geom
    parts = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
    rings = []
    for p in parts:
        b = p.bounds
        if max(b[2] - b[0], b[3] - b[1]) < MIN_SPAN:
            continue
        xs, ys = p.exterior.coords.xy
        qx = np.round(np.asarray(xs) / QUANT).astype(int)
        qy = np.round(np.asarray(ys) / QUANT).astype(int)
        keep = np.ones(len(qx), bool)
        keep[1:] = (np.diff(qx) != 0) | (np.diff(qy) != 0)
        qx, qy = qx[keep], qy[keep]
        if len(qx) < 4:
            continue
        out = [int(qx[0]), int(qy[0])]
        out.extend(
            int(v) for pair in zip(np.diff(qx), np.diff(qy)) for v in pair
        )
        rings.append(out)
    if not rings:  # too small to draw: fall back to a marker
        c = geom.representative_point()
        return None, [round(c.x, 3), round(c.y, 3)]
    return rings, None


eng = create_engine(
    "postgresql+psycopg2://{}:{}@chd-rasterstats-prod.postgres.database"
    ".azure.com/postgres?sslmode=require".format(
        os.getenv("DSCI_AZ_DB_PROD_UID"), os.getenv("DSCI_AZ_DB_PROD_PW")
    )
)
countries = pd.read_sql(
    "SELECT iso3, max_adm_level, floodscan FROM public.iso3 ORDER BY iso3",
    eng,
)
log(f"{len(countries)} countries, quant {QUANT} deg, tol {TOLERANCE} deg")

feats = []
for _, crow in countries.iterrows():
    iso3 = str(crow["iso3"]).strip()
    lvl = int(crow["max_adm_level"])
    t0 = time.perf_counter()
    try:
        shp = load_shp_cached(iso3, "prod")
        gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
        path = f"{shp}/{iso3.lower()}_adm{lvl}.shp"
        if not os.path.exists(path):
            log(f"{iso3}: no adm{lvl}")
            continue
        gdf = gpd.read_file(path)
        pcol = f"ADM{lvl}_PCODE"
        if pcol not in gdf.columns:
            log(f"{iso3}: no {pcol}")
            continue
        bad = ~gdf.geometry.is_valid
        if bad.any():
            gdf.loc[bad, "geometry"] = gdf.loc[bad, "geometry"].make_valid()

        dsets = [
            d
            for d in DATASETS
            if d != "floodscan" or bool(crow.get("floodscan"))
        ]
        tvds = {}
        for name in dsets:
            cfg = DATASETS[name]
            gx0, gy0, W, H = country_grid(
                gdf0.total_bounds, cfg["res"], cfg["x0"], cfg["y0"]
            )
            if W * H > 40_000_000:
                continue
            E = weights_exact(gdf, gx0, gy0, W, H, cfg["res"])
            C = weights_centroid(gdf, gx0, gy0, W, H, cfg["res"])
            t, se, sc = tvd_rows(E, C)
            # legacy finds no pixel at all -> not a perturbation but a
            # missing value; flag with -1 so the map can show it apart
            t = np.where((se > 0) & (sc == 0), -1.0, t)
            tvds[name] = t

        for i in range(len(gdf)):
            rings, pt = encode(gdf.geometry.iloc[i])
            rec = {
                "i": iso3,
                "l": lvl,
                "p": str(gdf[pcol].iloc[i]),
            }
            if rings:
                rec["r"] = rings
            else:
                rec["m"] = pt
            for name, arr in tvds.items():
                v = arr[i]
                if not np.isnan(v):
                    rec[name[0] if name != "floodscan" else "f"] = round(
                        float(v), 4
                    )
            feats.append(rec)
        log(
            f"{iso3} adm{lvl} n={len(gdf)} [{time.perf_counter() - t0:.1f}s] "
            f"total={len(feats)}"
        )
    except Exception as e:
        log(f"{iso3}: FAILED {type(e).__name__}: {e}")

out = {
    "quant": QUANT,
    "keys": {"s": "seas5", "e": "era5", "i": "imerg", "f": "floodscan"},
    "features": feats,
}
path = os.path.join(OUT_DIR, "admin_map.json")
with open(path, "w") as fh:
    json.dump(out, fh, separators=(",", ":"))
log(
    f"wrote {len(feats)} admin units to {path} "
    f"({os.path.getsize(path) / 1e6:.1f} MB)"
)
log("DONE")

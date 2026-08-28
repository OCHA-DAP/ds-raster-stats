"""Export simplified ADM0 outlines for every country in the pipeline.

Companion to geom_sensitivity.py: that produces the per-country numbers,
this produces the shapes to draw them on. Kept separate so the map can be
rebuilt without re-running the analysis.

Writes GeoJSON to $GEOM_OUT_DIR (default /dbfs/tmp on Databricks).
"""

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
import pandas as pd  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402

from src.utils.iso3_utils import load_shp_cached  # noqa: E402

OUT_DIR = os.environ.get(
    "GEOM_OUT_DIR", "/dbfs/tmp" if os.path.isdir("/dbfs/tmp") else "/tmp"
)
TOLERANCE = float(os.environ.get("SIMPLIFY_TOLERANCE", "0.12"))


def log(m):
    print(f"[adm0] {m}", flush=True)


eng = create_engine(
    "postgresql+psycopg2://{}:{}@chd-rasterstats-prod.postgres.database"
    ".azure.com/postgres?sslmode=require".format(
        os.getenv("DSCI_AZ_DB_PROD_UID"), os.getenv("DSCI_AZ_DB_PROD_PW")
    )
)
df = pd.read_sql(
    "SELECT iso3, max_adm_level FROM public.iso3 ORDER BY iso3", eng
)
log(f"{len(df)} countries, simplify tolerance {TOLERANCE} deg")

shapes = []
for _, row in df.iterrows():
    iso3 = str(row["iso3"]).strip()
    t0 = time.perf_counter()
    try:
        shp = load_shp_cached(iso3, "prod")
        g = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
        g = g.dissolve()[["geometry"]]
        g["geometry"] = g.geometry.simplify(
            tolerance=TOLERANCE, preserve_topology=True
        )
        g["iso3"] = iso3
        g["adm_level"] = int(row["max_adm_level"])
        shapes.append(g)
        log(f"{iso3} [{time.perf_counter() - t0:.1f}s]")
    except Exception as e:
        log(f"{iso3}: FAILED {type(e).__name__}: {e}")

allg = gpd.GeoDataFrame(pd.concat(shapes, ignore_index=True), crs="EPSG:4326")
path = os.path.join(OUT_DIR, "adm0_simplified.geojson")
allg.to_file(path, driver="GeoJSON")
log(
    f"wrote {len(allg)} countries to {path} "
    f"({os.path.getsize(path) / 1e6:.1f} MB)"
)
log("DONE")

"""Compare values produced by the new exactextract weight-matrix method
against the legacy upsample-and-rasterize values already in the prod DB.

Read-only: prod COGs + prod boundaries + prod DB reads. Writes nothing.

Covers all four datasets so the effect can be read against native
resolution (SEAS5 0.4 deg -> FloodScan 0.0833 deg), across countries
chosen to span continental / coastal / small-polygon geometry.
"""

import json
import os
import sys

os.environ["DSCI_AZ_BLOB_PROD_SAS_WRITE"] = os.environ["DSCI_AZ_BLOB_PROD_SAS"]
os.environ.setdefault("PGSSLMODE", "require")

sys.path.insert(0, "/Users/tdowning/OCHA/repos/ds-raster-stats")
os.chdir("/Users/tdowning/OCHA/repos/ds-raster-stats")

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

from src.utils.cog_utils import stack_cogs  # noqa: E402
from src.utils.iso3_utils import load_shp_cached  # noqa: E402
from src.utils.zonal_utils import clip_raster, zonal_stats_runner  # noqa: E402

SCRATCH = (
    "/private/tmp/claude-501/-Users-tdowning-OCHA-repos/"
    "440241f8-24d0-449a-9fe3-8a498a385bb7/scratchpad"
)

ENGINE = create_engine(
    "postgresql+psycopg2://{}:{}@chd-rasterstats-prod.postgres.database"
    ".azure.com/postgres?sslmode=require".format(
        os.getenv("DSCI_AZ_DB_PROD_UID"), os.getenv("DSCI_AZ_DB_PROD_PW")
    )
)

# native resolution (deg) -> upsample factor to 0.05
RESOLUTION = {
    "seas5": 0.4,
    "era5": 0.25,
    "imerg": 0.1,
    "floodscan": 0.0833,
}

# continental / coastal / small, all with ADM2 coverage in prod
COUNTRIES = ["NGA", "TCD", "ETH", "SOM", "MOZ", "HTI", "SLV", "MMR"]
AFRICA_ONLY = {"NGA", "TCD", "ETH", "SOM", "MOZ"}  # floodscan coverage

DATES = {
    "seas5": pd.Timestamp("2026-07-01"),  # issued
    "era5": pd.Timestamp("2026-06-01"),
    "imerg": pd.Timestamp("2026-08-20"),
    "floodscan": pd.Timestamp("2026-08-20"),
}

STATS = ["mean", "median", "min", "max", "sum", "std", "count"]


def log(m):
    print(f"[cmp] {m}", flush=True)


def new_values(dataset, date, iso3s):
    ds = stack_cogs([date], dataset, "prod")
    out = []
    for iso3 in iso3s:
        shp = load_shp_cached(iso3, "prod")
        gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
        clip = clip_raster(ds, gdf0)
        max_adm = pd.read_sql_query(
            text("SELECT max_adm_level FROM public.iso3 WHERE iso3 = :i"),
            ENGINE.connect(),
            params={"i": iso3},
        )["max_adm_level"][0]
        for lvl in range(int(max_adm) + 1):
            gdf = gpd.read_file(f"{shp}/{iso3.lower()}_adm{lvl}.shp")
            df = zonal_stats_runner(clip, gdf, lvl, iso3)
            if df is not None:
                out.append(df)
    return pd.concat(out, ignore_index=True)


def legacy_values(dataset, date, iso3s):
    col = "issued_date" if dataset == "seas5" else "valid_date"
    q = text(
        f"SELECT * FROM public.{dataset} "
        f"WHERE iso3 IN :isos AND {col} = :d"
    ).bindparams(**{"isos": tuple(iso3s)})
    return pd.read_sql_query(
        q, ENGINE.connect(), params={"isos": tuple(iso3s), "d": str(date.date())}
    )


results = {}
frames = {}
for dataset, date in DATES.items():
    isos = [c for c in COUNTRIES if dataset != "floodscan" or c in AFRICA_ONLY]
    log(f"{dataset}: computing new values for {len(isos)} countries...")
    new = new_values(dataset, date, isos)
    old = legacy_values(dataset, date, isos)
    log(f"{dataset}: new {len(new)} rows, legacy {len(old)} rows")
    if not len(old):
        log(f"{dataset}: NO legacy rows for {date.date()}, skipping")
        continue

    keys = ["iso3", "pcode", "valid_date", "adm_level"]
    if dataset == "seas5":
        keys.append("leadtime")
    if dataset == "floodscan":
        keys.append("band")
    for df in (new, old):
        df["valid_date"] = pd.to_datetime(df["valid_date"])
    m = old.merge(new, on=keys, suffixes=("_old", "_new"))
    log(f"{dataset}: {len(m)} matched rows")
    if not len(m):
        continue

    # legacy `count` = number of 0.05 deg pixels in the polygon;
    # convert to native-pixel equivalents to express polygon size
    # relative to the raster it is measured on
    up = (RESOLUTION[dataset] / 0.05) ** 2
    m["native_px"] = m["count_old"] / up

    per_stat = {}
    for s in STATS:
        o, n = m[f"{s}_old"].astype(float), m[f"{s}_new"].astype(float)
        both = o.notna() & n.notna()
        denom = o.abs().where(o.abs() > 1e-9)
        rel = ((n - o).abs() / denom)[both].dropna()
        signed = ((n - o) / denom)[both].dropna()
        per_stat[s] = {
            "n": int(both.sum()),
            "median_abs_rel": float(rel.median()),
            "p90_abs_rel": float(rel.quantile(0.9)),
            "p99_abs_rel": float(rel.quantile(0.99)),
            "median_signed_rel": float(signed.median()),
            "within_1pct": float((rel <= 0.01).mean()),
            "within_5pct": float((rel <= 0.05).mean()),
            "within_10pct": float((rel <= 0.10).mean()),
            "only_new_has_value": int((o.isna() & n.notna()).sum()),
            "only_old_has_value": int((n.isna() & o.notna()).sum()),
        }

    # mean difference bucketed by polygon size in native pixels
    o, n = m["mean_old"].astype(float), m["mean_new"].astype(float)
    m["mean_rel"] = (n - o).abs() / o.abs().where(o.abs() > 1e-9)
    m["size_bucket"] = pd.cut(
        m["native_px"],
        [0, 1, 4, 16, 64, np.inf],
        labels=["<1 px", "1-4 px", "4-16 px", "16-64 px", ">64 px"],
    )
    by_size = (
        m.groupby("size_bucket", observed=True)["mean_rel"]
        .agg(["median", "quantile", "size"])
        .rename(columns={"quantile": "p50b"})
    )
    by_size_d = {
        str(k): {
            "median_rel": (
                None if pd.isna(v["median"]) else float(v["median"])
            ),
            "n": int(v["size"]),
        }
        for k, v in by_size.iterrows()
    }
    by_adm = {
        str(int(k)): {
            "median_rel": (
                None if pd.isna(v.median()) else float(v.median())
            ),
            "n": int(v.notna().sum()),
        }
        for k, v in m.groupby("adm_level")["mean_rel"]
    }

    results[dataset] = {
        "native_resolution_deg": RESOLUTION[dataset],
        "upsample_factor": (RESOLUTION[dataset] / 0.05),
        "date": str(date.date()),
        "countries": isos,
        "matched_rows": len(m),
        "rows_only_new": int(len(new) - len(m)),
        "rows_only_legacy": int(len(old) - len(m)),
        "per_stat": per_stat,
        "mean_by_polygon_size": by_size_d,
        "mean_by_adm_level": by_adm,
    }
    frames[dataset] = m
    log(
        f"{dataset}: mean median-rel-diff "
        f"{per_stat['mean']['median_abs_rel']:.3%}, "
        f"within 5%: {per_stat['mean']['within_5pct']:.1%}"
    )

json.dump(results, open(f"{SCRATCH}/value_comparison.json", "w"), indent=1)
for k, f in frames.items():
    f.to_csv(f"{SCRATCH}/cmp_{k}.csv", index=False)
log("DONE")

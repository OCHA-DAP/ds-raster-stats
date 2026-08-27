"""Does the method change reorder the historical record?

AA triggers are rank/return-period comparisons ("driest 1-in-5 years"),
so the operational question is not how much a value moves but whether
the SAME years land in the trigger tail.

For each series -- one admin unit, one leadtime, one issued month,
across all years in the archive -- this ranks the years under the legacy
values (prod DB) and under the new exactextract values, and asks:

  * Spearman rank correlation
  * how many years change rank at all
  * for RP in {3, 4, 5, 10}: is the SET of activating years identical?
    (both tails: driest = drought triggers, wettest = flood triggers)

Read-only. Writes nothing to any database.
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
from scipy.stats import spearmanr  # noqa: E402
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

COUNTRIES = ["NER", "TCD", "ETH", "SOM"]
ISSUED_MONTHS = [3, 7]  # a Sahel-season and a HoA-season issuance
YEARS = range(1981, 2025)
RPS = [3, 4, 5, 10]
STAT = "mean"


def log(m):
    print(f"[rank] {m}", flush=True)


def build(iso3, months):
    """New-method stats for every issuance in `months` across YEARS."""
    dates = [
        pd.Timestamp(year=y, month=m, day=1) for y in YEARS for m in months
    ]
    ds = stack_cogs(dates, "seas5", "prod")
    shp = load_shp_cached(iso3, "prod")
    gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
    clip = clip_raster(ds, gdf0)
    max_adm = pd.read_sql_query(
        text("SELECT max_adm_level FROM public.iso3 WHERE iso3 = :i"),
        ENGINE.connect(),
        params={"i": iso3},
    )["max_adm_level"][0]
    out = []
    for lvl in range(int(max_adm) + 1):
        gdf = gpd.read_file(f"{shp}/{iso3.lower()}_adm{lvl}.shp")
        df = zonal_stats_runner(clip, gdf, lvl, iso3)
        if df is not None:
            out.append(df)
    new = pd.concat(out, ignore_index=True)
    new["valid_date"] = pd.to_datetime(new["valid_date"])
    new["issued_date"] = pd.to_datetime(new["issued_date"])
    return new


def legacy(iso3, months):
    q = text(
        "SELECT iso3, pcode, adm_level, valid_date, issued_date, leadtime, "
        f"{STAT} FROM public.seas5 WHERE iso3 = :i "
        "AND EXTRACT(MONTH FROM issued_date) = ANY(:months) "
        "AND EXTRACT(YEAR FROM issued_date) BETWEEN :y0 AND :y1"
    )
    df = pd.read_sql_query(
        q,
        ENGINE.connect(),
        params={
            "i": iso3,
            "months": list(months),
            "y0": min(YEARS),
            "y1": max(YEARS),
        },
    )
    df["valid_date"] = pd.to_datetime(df["valid_date"])
    df["issued_date"] = pd.to_datetime(df["issued_date"])
    return df


def tail_years(series, n, low=True):
    """Set of years in the n most extreme positions."""
    s = series.sort_values(ascending=low)
    return set(s.index[:n])


summary = []
detail_rows = []
for iso3 in COUNTRIES:
    log(f"{iso3}: computing new values across {len(list(YEARS))} years...")
    new = build(iso3, ISSUED_MONTHS)
    old = legacy(iso3, ISSUED_MONTHS)
    log(f"{iso3}: new {len(new)} rows, legacy {len(old)} rows")

    keys = ["iso3", "pcode", "adm_level", "valid_date", "leadtime"]
    m = old.merge(
        new[keys + [STAT]], on=keys, suffixes=("_old", "_new"), how="inner"
    )
    m["year"] = m["issued_date"].dt.year
    m["issued_month"] = m["issued_date"].dt.month
    log(f"{iso3}: {len(m)} matched rows")

    for (pcode, lvl, lt, im), g in m.groupby(
        ["pcode", "adm_level", "leadtime", "issued_month"]
    ):
        g = g.dropna(subset=[f"{STAT}_old", f"{STAT}_new"])
        if len(g) < 20:
            continue
        s_old = g.set_index("year")[f"{STAT}_old"].astype(float)
        s_new = g.set_index("year")[f"{STAT}_new"].astype(float)
        n_years = len(s_old)

        rho = spearmanr(s_old, s_new).statistic
        r_old = s_old.rank(method="first")
        r_new = s_new.rank(method="first")
        n_moved = int((r_old != r_new).sum())
        max_shift = float((r_old - r_new).abs().max())

        row = {
            "iso3": iso3,
            "pcode": pcode,
            "adm_level": int(lvl),
            "leadtime": int(lt),
            "issued_month": int(im),
            "n_years": n_years,
            "spearman": float(rho),
            "n_rank_moved": n_moved,
            "max_rank_shift": max_shift,
        }
        for rp in RPS:
            n_act = max(1, round(n_years / rp))
            for direction, low in [("dry", True), ("wet", False)]:
                a = tail_years(s_old, n_act, low)
                b = tail_years(s_new, n_act, low)
                row[f"same_{direction}_rp{rp}"] = a == b
                row[f"ndiff_{direction}_rp{rp}"] = len(a ^ b) // 2
        detail_rows.append(row)

    log(f"{iso3}: {len(detail_rows)} series so far")

d = pd.DataFrame(detail_rows)
d.to_csv(f"{SCRATCH}/rank_detail.csv", index=False)

out = {
    "n_series": int(len(d)),
    "countries": COUNTRIES,
    "issued_months": ISSUED_MONTHS,
    "years": [min(YEARS), max(YEARS)],
    "median_n_years": float(d["n_years"].median()),
    "spearman": {
        "min": float(d["spearman"].min()),
        "p01": float(d["spearman"].quantile(0.01)),
        "median": float(d["spearman"].median()),
        "frac_above_0999": float((d["spearman"] > 0.999).mean()),
    },
    "rank_movement": {
        "frac_series_with_no_rank_change": float((d["n_rank_moved"] == 0).mean()),
        "median_years_moved": float(d["n_rank_moved"].median()),
        "median_max_shift": float(d["max_rank_shift"].median()),
        "p99_max_shift": float(d["max_rank_shift"].quantile(0.99)),
    },
    "activation_sets": {},
}
for rp in RPS:
    for direction in ["dry", "wet"]:
        same = d[f"same_{direction}_rp{rp}"]
        nd = d[f"ndiff_{direction}_rp{rp}"]
        out["activation_sets"][f"{direction}_1in{rp}"] = {
            "frac_identical": float(same.mean()),
            "n_series_differing": int((~same).sum()),
            "median_years_swapped_when_differing": (
                float(nd[~same].median()) if (~same).any() else 0.0
            ),
            "max_years_swapped": int(nd.max()),
        }

json.dump(out, open(f"{SCRATCH}/rank_comparison.json", "w"), indent=1)
log(json.dumps(out["activation_sets"], indent=1))
log("DONE")

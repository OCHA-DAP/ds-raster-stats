"""Does the method change move the Ethiopia drought trigger?

ETH drought (frameworks/eth-drought/2026-06-09) is the one framework that
re-ranks SEAS5 against the historical record at monitoring time, so it is
the most exposed to a change in the underlying values.

This replicates the deployed logic from ds-aa-eth-drought-monitoring
(src/analysis/{mam,jjas}.py) exactly:

  value  = zonal mean * days_in_month, summed over the season months
  rank   = rank within pcode, ascending, method="min", over 1997-2025
  RP     = (29 + 1) / rank
  zone   = RP >= 5
  fire   = zone count >= 15 (MAM, Feb issue) / >= 35 (JJAS, Apr or May)

and runs it twice: once on the legacy values in the prod DB, once on
values recomputed with the new exactextract method. Read-only.
"""

import json
import os
import sys

os.environ["DSCI_AZ_BLOB_PROD_SAS_WRITE"] = os.environ["DSCI_AZ_BLOB_PROD_SAS"]
os.environ.setdefault("PGSSLMODE", "require")

sys.path.insert(0, "/Users/tdowning/OCHA/repos/ds-raster-stats")
os.chdir("/Users/tdowning/OCHA/repos/ds-raster-stats")

import geopandas as gpd  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402

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

N_HIST_YEARS = 29
YEARS = range(1997, 2026)
DAYS = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
SEASONS = {
    "MAM": {"issued": [2], "months": [3, 4, 5], "threshold": 15},
    "JJAS": {"issued": [4, 5], "months": [6, 7, 8, 9], "threshold": 35},
}


def log(m):
    print(f"[eth] {m}", flush=True)


zones = {
    s: set(
        pd.read_csv(f"{SCRATCH}/zones_{s.lower()}.csv")["adm2_pcode"].unique()
    )
    for s in SEASONS
}
log({s: len(z) for s, z in zones.items()})

# ---- new-method values for every relevant issuance ----
issued_months = sorted({m for c in SEASONS.values() for m in c["issued"]})
dates = [
    pd.Timestamp(year=y, month=m, day=1) for y in YEARS for m in issued_months
]
log(f"stacking {len(dates)} SEAS5 issuances (Feb/Apr/May, 1997-2025)...")
ds = stack_cogs(dates, "seas5", "prod")
shp = load_shp_cached("ETH", "prod")
gdf0 = gpd.read_file(f"{shp}/eth_adm0.shp")
clip = clip_raster(ds, gdf0)
gdf2 = gpd.read_file(f"{shp}/eth_adm2.shp")
new = zonal_stats_runner(clip, gdf2, 2, "ETH")
new["valid_date"] = pd.to_datetime(new["valid_date"])
new["issued_date"] = pd.to_datetime(new["issued_date"])
log(f"new-method rows: {len(new)}")

# ---- legacy values from the prod DB ----
all_pcodes = sorted(zones["MAM"] | zones["JJAS"])
q = (
    "SELECT pcode, valid_date, issued_date, leadtime, mean FROM public.seas5 "
    "WHERE adm_level = 2 AND iso3 = 'ETH' "
    "AND valid_date BETWEEN '1997-01-01' AND '2025-12-31' "
    "AND pcode IN ({})".format(", ".join(f"'{p}'" for p in all_pcodes))
)
old = pd.read_sql(q, ENGINE)
old["valid_date"] = pd.to_datetime(old["valid_date"])
old["issued_date"] = pd.to_datetime(old["issued_date"])
log(f"legacy rows: {len(old)}")


def prep(df):
    df = df.copy()
    df["issued_month"] = df["issued_date"].dt.month
    df["valid_month"] = df["valid_date"].dt.month
    df["year"] = df["valid_date"].dt.year
    df["mean"] = df["mean"].astype(float) * df["valid_month"].map(DAYS)
    return df


def season_series(df, season, issued_month):
    """Seasonal total mm per (year, pcode), as the deployed code builds it."""
    c = SEASONS[season]
    d = df[
        (df["issued_month"] == issued_month)
        & (df["valid_month"].isin(c["months"]))
        & (df["pcode"].isin(zones[season]))
    ]
    return d.groupby(["year", "pcode"], as_index=False)["mean"].sum()


def add_rp(d):
    d = d.copy()
    d["rank"] = d.groupby("pcode")["mean"].rank(method="min", ascending=True)
    d["return_period"] = ((N_HIST_YEARS + 1) / d["rank"]).round(1)
    return d


def zone_counts(d, min_rp=5):
    hit = d[d["return_period"] >= min_rp]
    return {y: int((hit["year"] == y).sum()) for y in YEARS}


results = {}
per_zone_moves = []
old_p, new_p = prep(old), prep(new)

for season, cfg in SEASONS.items():
    for im in cfg["issued"]:
        key = f"{season}_issued{im:02d}"
        so = add_rp(season_series(old_p, season, im))
        sn = add_rp(season_series(new_p, season, im))
        merged = so.merge(sn, on=["year", "pcode"], suffixes=("_old", "_new"))
        if merged.empty:
            log(f"{key}: no overlap, skipping")
            continue

        co, cn = zone_counts(so), zone_counts(sn)
        thr = cfg["threshold"]
        fires_old = {y: co[y] >= thr for y in YEARS}
        fires_new = {y: cn[y] >= thr for y in YEARS}
        flipped = [y for y in YEARS if fires_old[y] != fires_new[y]]

        rank_moved = int((merged["rank_old"] != merged["rank_new"]).sum())
        rp_class_changed = int(
            (
                (merged["return_period_old"] >= 5)
                != (merged["return_period_new"] >= 5)
            ).sum()
        )
        results[key] = {
            "season": season,
            "issued_month": im,
            "threshold_zones": thr,
            "n_zones": len(zones[season]),
            "n_zone_years": len(merged),
            "zone_years_rank_moved": rank_moved,
            "zone_years_crossing_rp5": rp_class_changed,
            "counts_old": co,
            "counts_new": cn,
            "max_count_diff": max(abs(co[y] - cn[y]) for y in YEARS),
            "years_activating_old": [y for y in YEARS if fires_old[y]],
            "years_activating_new": [y for y in YEARS if fires_new[y]],
            "activation_flips": flipped,
        }
        log(
            f"{key}: zone-years rank-moved {rank_moved}/{len(merged)}, "
            f"crossing RP5 {rp_class_changed}, max count diff "
            f"{results[key]['max_count_diff']}, activation flips {flipped}"
        )
        merged["key"] = key
        per_zone_moves.append(merged)

pd.concat(per_zone_moves, ignore_index=True).to_csv(
    f"{SCRATCH}/eth_zone_detail.csv", index=False
)
json.dump(results, open(f"{SCRATCH}/eth_trigger_check.json", "w"), indent=1)
log("DONE")

"""How much can the method change move a value? A data-free answer.

The difference between the legacy whole-pixel rule and exact coverage
fractions is a property of GEOMETRY ALONE -- polygon versus pixel grid.
No raster values are needed to bound it.

For each admin polygon on a dataset's native grid we build two weight
vectors over native pixels:

  e_p  exact fractional coverage of native pixel p by the polygon
       (what PR #49 uses)
  c_p  count of 0.05-degree upsampled cells assigned to the polygon by
       the centroid rule whose own centre falls in native pixel p
       (what the legacy method effectively averages over -- nearest
       resampling gives every upsampled cell the value of the native
       pixel containing its centre, so this replicates it exactly,
       including for non-integer upsample factors like FloodScan's)

Normalise both to sum 1 and take the total variation distance

  TVD = 0.5 * sum_p | e_p/sum(e) - c_p/sum(c) |    in [0, 1]

This is the share of averaging weight that lands on different pixels.
The realised change in a mean is TVD x (local spread of pixel values),
so TVD alone is the geometric sensitivity, and the map of it says where
the method change can matter before any data is read.

Polygons the legacy rule misses entirely (no centroid anywhere) are
reported separately: those are undefined under the old method, not
merely perturbed.
"""

import json
import os
import sys
import time

os.environ.setdefault(
    "DSCI_AZ_BLOB_PROD_SAS_WRITE", os.environ.get("DSCI_AZ_BLOB_PROD_SAS", "")
)

try:
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _root = os.getcwd()
sys.path.insert(0, _root)
os.chdir(_root)

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from exactextract import exact_extract  # noqa: E402
from exactextract.raster import NumPyRasterSource  # noqa: E402
from rasterio.features import rasterize  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402
from scipy import sparse  # noqa: E402

from src.utils.iso3_utils import load_shp_cached  # noqa: E402

OUT_DIR = os.environ.get(
    "GEOM_OUT_DIR", "/dbfs/tmp" if os.path.isdir("/dbfs/tmp") else "/tmp"
)
UPSAMPLED = 0.05

# native resolution and global grid origin per dataset. SEAS5/ERA5/IMERG
# are on whole-degree-aligned global grids; FloodScan is 300 arc-seconds.
DATASETS = {
    "seas5": {"res": 0.4, "x0": -180.0, "y0": 90.0},
    "era5": {"res": 0.25, "x0": -180.0, "y0": 90.0},
    "imerg": {"res": 0.1, "x0": -180.0, "y0": 90.0},
    "floodscan": {"res": 1.0 / 12.0, "x0": -180.0, "y0": 90.0},
}


def log(m):
    print(f"[geom] {m}", flush=True)


def country_grid(bounds, res, x0, y0, pad=1):
    """Native-grid window covering `bounds`, snapped to the global grid."""
    minx, miny, maxx, maxy = bounds
    i0 = int(np.floor((minx - x0) / res)) - pad
    i1 = int(np.ceil((maxx - x0) / res)) + pad
    j0 = int(np.floor((y0 - maxy) / res)) - pad
    j1 = int(np.ceil((y0 - miny) / res)) + pad
    W, H = i1 - i0, j1 - j0
    gx0 = x0 + i0 * res
    gy0 = y0 - j0 * res
    return gx0, gy0, W, H


def weights_exact(gdf, gx0, gy0, W, H, res):
    """Sparse (n_polys, H*W) of exact fractional coverage."""
    src = NumPyRasterSource(
        np.zeros((H, W)),
        xmin=gx0,
        ymin=gy0 - H * res,
        xmax=gx0 + W * res,
        ymax=gy0,
        srs_wkt=None,
    )
    df = exact_extract(src, gdf, ["cell_id", "coverage"], output="pandas")
    rows, cols, vals = [], [], []
    for i, (cid, cov) in enumerate(zip(df["cell_id"], df["coverage"])):
        keep = cov > 0
        rows.append(np.full(int(keep.sum()), i))
        cols.append(np.asarray(cid)[keep])
        vals.append(np.asarray(cov)[keep])
    return sparse.coo_matrix(
        (
            np.concatenate(vals).astype(np.float64),
            (
                np.concatenate(rows).astype(np.int64),
                np.concatenate(cols).astype(np.int64),
            ),
        ),
        shape=(len(gdf), H * W),
    ).tocsr()


def weights_centroid(gdf, gx0, gy0, W, H, res):
    """Sparse (n_polys, H*W): legacy centroid rule on the 0.05 deg grid,
    folded back onto native pixels the way nearest resampling does."""
    factor = res / UPSAMPLED
    UW, UH = int(W * factor), int(H * factor)
    ures_x = (W * res) / UW
    ures_y = (H * res) / UH
    transform = from_origin(gx0, gy0, ures_x, ures_y)

    geoms = [
        (g, i)
        for i, g in enumerate(
            gdf.geometry.simplify(tolerance=0.001, preserve_topology=True)
        )
    ]
    labels = rasterize(
        shapes=geoms,
        out_shape=(UH, UW),
        transform=transform,
        fill=-1,
        all_touched=False,
        dtype="int32",
    )
    hit = labels >= 0
    if not hit.any():
        return sparse.csr_matrix((len(gdf), H * W))
    uj, ui = np.nonzero(hit)
    poly = labels[uj, ui]
    # centre of each upsampled cell -> containing native pixel
    cx = gx0 + (ui + 0.5) * ures_x
    cy = gy0 - (uj + 0.5) * ures_y
    ni = np.clip(((cx - gx0) / res).astype(int), 0, W - 1)
    nj = np.clip(((gy0 - cy) / res).astype(int), 0, H - 1)
    flat = nj * W + ni
    return sparse.coo_matrix(
        (np.ones(len(poly)), (poly, flat)), shape=(len(gdf), H * W)
    ).tocsr()


def tvd_rows(A, B):
    """Total variation distance between row-normalised sparse matrices."""
    sa = np.asarray(A.sum(axis=1)).ravel()
    sb = np.asarray(B.sum(axis=1)).ravel()
    out = np.full(A.shape[0], np.nan)
    ok = (sa > 0) & (sb > 0)
    if not ok.any():
        return out, sa, sb
    An = A.multiply(1.0 / np.where(sa > 0, sa, 1)[:, None]).tocsr()
    Bn = B.multiply(1.0 / np.where(sb > 0, sb, 1)[:, None]).tocsr()
    D = An - Bn
    D.data = np.abs(D.data)
    out[ok] = 0.5 * np.asarray(D.sum(axis=1)).ravel()[ok]
    return out, sa, sb


def analyse(iso3, adm_level, datasets=None):
    shp = load_shp_cached(iso3, "prod")
    gdf0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
    lvl_path = f"{shp}/{iso3.lower()}_adm{adm_level}.shp"
    if not os.path.exists(lvl_path):
        return None
    gdf = gpd.read_file(lvl_path)
    pcode_col = f"ADM{adm_level}_PCODE"
    if pcode_col not in gdf.columns:
        return None
    inval = ~gdf.geometry.is_valid
    if inval.any():
        gdf.loc[inval, "geometry"] = gdf.loc[inval, "geometry"].make_valid()

    res_out = {
        "iso3": iso3,
        "adm_level": int(adm_level),
        "n_polygons": int(len(gdf)),
        "datasets": {},
    }
    for name in datasets or DATASETS:
        cfg = DATASETS[name]
        gx0, gy0, W, H = country_grid(
            gdf0.total_bounds, cfg["res"], cfg["x0"], cfg["y0"]
        )
        if W * H > 40_000_000:
            log(f"{iso3}/{name}: grid too large ({W}x{H}), skipping")
            continue
        E = weights_exact(gdf, gx0, gy0, W, H, cfg["res"])
        C = weights_centroid(gdf, gx0, gy0, W, H, cfg["res"])
        tvd, se, sc = tvd_rows(E, C)
        missed = int(((se > 0) & (sc == 0)).sum())
        vals = tvd[~np.isnan(tvd)]
        res_out["datasets"][name] = {
            "median_tvd": float(np.median(vals)) if len(vals) else None,
            "p90_tvd": float(np.quantile(vals, 0.9)) if len(vals) else None,
            "mean_tvd": float(np.mean(vals)) if len(vals) else None,
            "frac_tvd_gt_10pct": (
                float((vals > 0.10).mean()) if len(vals) else None
            ),
            "n_missed_by_legacy": missed,
            "frac_missed_by_legacy": float(missed / max(len(gdf), 1)),
            "native_pixels_median": float(np.median(se)),
        }
    return res_out


def country_list():
    """iso3 + the deepest admin level the pipeline computes, from prod."""
    from sqlalchemy import create_engine

    eng = create_engine(
        "postgresql+psycopg2://{}:{}@chd-rasterstats-prod.postgres"
        ".database.azure.com/postgres?sslmode=require".format(
            os.getenv("DSCI_AZ_DB_PROD_UID"), os.getenv("DSCI_AZ_DB_PROD_PW")
        )
    )
    df = pd.read_sql(
        "SELECT iso3, max_adm_level, floodscan FROM public.iso3 "
        "ORDER BY iso3",
        eng,
    )
    return df


if __name__ == "__main__":
    only = sys.argv[1].split(",") if len(sys.argv) > 1 else None
    df = country_list()
    if only:
        df = df[df["iso3"].isin(only)]
    log(f"{len(df)} countries")

    out, shapes = [], []
    for _, row in df.iterrows():
        iso3 = str(row["iso3"]).strip()
        lvl = int(row["max_adm_level"])
        # FloodScan only covers the African subset flagged in public.iso3
        dsets = list(DATASETS)
        if "floodscan" in row and not bool(row["floodscan"]):
            dsets = [d for d in dsets if d != "floodscan"]
        t0 = time.perf_counter()
        try:
            r = analyse(iso3, lvl, datasets=dsets)
        except Exception as e:
            log(f"{iso3}: FAILED {type(e).__name__}: {e}")
            continue
        if r is None:
            log(f"{iso3}: no adm{lvl} layer, skipping")
            continue
        out.append(r)
        try:
            shp = load_shp_cached(iso3, "prod")
            g0 = gpd.read_file(f"{shp}/{iso3.lower()}_adm0.shp")
            g0 = g0.dissolve()[["geometry"]]
            g0["geometry"] = g0.geometry.simplify(
                tolerance=0.15, preserve_topology=True
            )
            g0["iso3"] = iso3
            shapes.append(g0)
        except Exception as e:
            log(f"{iso3}: geometry export failed: {e}")
        s_ = " ".join(
            f"{k}={v['median_tvd']:.3f}" for k, v in r["datasets"].items()
        )
        log(
            f"{iso3} adm{lvl} n={r['n_polygons']} "
            f"[{time.perf_counter() - t0:.1f}s] {s_}"
        )
        json.dump(
            out, open(os.path.join(OUT_DIR, "geom_sensitivity.json"), "w")
        )

    if shapes:
        allg = gpd.GeoDataFrame(
            pd.concat(shapes, ignore_index=True), crs="EPSG:4326"
        )
        allg.to_file(
            os.path.join(OUT_DIR, "adm0_simplified.geojson"),
            driver="GeoJSON",
        )
        log(f"wrote geometry for {len(allg)} countries")
    log("DONE")

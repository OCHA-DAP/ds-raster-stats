"""
Weight-matrix based zonal statistics.

This module replaces the upsample-then-rasterize approach in
``raster_utils.py``. Instead of physically resampling every raster to
0.05 degrees and re-scanning each admin polygon for every date slice,
we compute a sparse coverage-weight matrix ``W`` once per (country,
admin level) with ``exactextract`` -- ``W[adm, pixel]`` is the exact
fraction of the native pixel's area covered by the admin polygon --
and then compute all statistics for every date (and leadtime/band)
slice at once with sparse matrix products.

``count`` and ``sum`` are scaled by the area ratio between the native
resolution and ``UPSAMPLED_RESOLUTION`` so that values remain on the
same scale as stats computed by the legacy method (which counted
upsampled pixels). ``mean``/``median``/``std`` are scale-invariant.
``min``/``max`` are taken over all pixels with any coverage, where the
legacy method included only pixels with at least one upsampled-pixel
centroid inside the polygon.

Weight matrices only depend on the admin boundaries and the raster
grid, so they are cached on disk and reused across date chunks and
worker processes.
"""

import hashlib
import logging
import os
import tempfile
import warnings

import numpy as np
import pandas as pd
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource
from scipy import sparse

from src.config.settings import UPSAMPLED_RESOLUTION
from src.utils.general_utils import add_months_to_date

logger = logging.getLogger(__name__)

# Default under the system temp dir: relative dot-dirs fail on Databricks
# (the workspace FUSE filesystem rejects them with EINVAL)
WEIGHTS_CACHE_DIR = os.getenv(
    "WEIGHTS_CACHE_DIR",
    os.path.join(tempfile.gettempdir(), "raster-stats-cache", "weights"),
)

STATS = ["mean", "max", "min", "median", "sum", "std", "count"]


def clip_raster(ds, gdf_adm, logger=None):
    """
    Clip a raster dataset to the bounds of the provided administrative
    regions (padded by one pixel so that boundary polygons keep full
    coverage) and persist it in memory. Unlike the legacy
    ``prep_raster``, the data is NOT upsampled -- stats are computed at
    native resolution against exact coverage fractions.

    Parameters
    ----------
    ds : xarray.DataArray
        The input raster dataset. Must have `x` and `y` coordinates.
    gdf_adm : geopandas.GeoDataFrame
        Administrative boundaries; their bounding box defines the clip.

    Returns
    -------
    xarray.DataArray
        The clipped raster dataset, persisted in memory.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    res = abs(ds.rio.resolution()[0])
    minx, miny, maxx, maxy = gdf_adm.total_bounds
    logger.debug("Clipping raster to iso3 bounds and persisting in memory...")
    ds_clip = ds.sel(
        x=slice(minx - res, maxx + res), y=slice(maxy + res, miny - res)
    ).persist()
    if ds_clip.sizes["x"] == 0 or ds_clip.sizes["y"] == 0:
        raise ValueError(
            "Raster has no pixels within the country bounds "
            f"({minx:.2f}, {miny:.2f}, {maxx:.2f}, {maxy:.2f})"
        )
    if ds_clip.rio.crs is None:
        ds_clip = ds_clip.rio.write_crs("EPSG:4326")

    # FloodScan COGs have an integer `band` dimension; map to the
    # band names used in the output table (same mapping as the legacy
    # `upsample_raster`)
    if "band" in ds_clip.dims and ds_clip["band"].dtype.kind in "iuf":
        ds_clip = ds_clip.assign_coords(
            band=[
                "SFED" if int(b) == 1 else "MFED"
                for b in ds_clip["band"].values
            ]
        )
    return ds_clip


def _grid_params(ds):
    """Return (xmin, ymin, xmax, ymax, height, width) of a raster grid."""
    transform = ds.rio.transform()
    height = ds.rio.height
    width = ds.rio.width
    xmin = transform.c
    ymax = transform.f
    xmax = xmin + transform.a * width
    ymin = ymax + transform.e * height
    return xmin, ymin, xmax, ymax, height, width


def build_weights(gdf, pcode_col, ds):
    """
    Build a sparse coverage-weight matrix for a set of admin polygons
    over the grid of a (clipped) raster dataset.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Administrative boundaries with a pcode column.
    pcode_col : str
        Name of the pcode column (e.g. "ADM1_PCODE").
    ds : xarray.DataArray
        Raster whose grid defines the pixels. Only the grid is used,
        not the values.

    Returns
    -------
    scipy.sparse.csr_matrix
        Shape (n_adms, height * width); entry (a, p) is the fraction
        of pixel p's area covered by admin a. Row order matches
        ``gdf`` row order.
    """
    xmin, ymin, xmax, ymax, height, width = _grid_params(ds)

    # exactextract requires valid geometries
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        gdf = gdf.copy()
        gdf.loc[invalid, "geometry"] = gdf.loc[
            invalid, "geometry"
        ].make_valid()

    rast = NumPyRasterSource(
        np.zeros((height, width)),
        xmin=xmin,
        ymin=ymin,
        xmax=xmax,
        ymax=ymax,
        srs_wkt=None,
    )
    df_cov = exact_extract(
        rast,
        gdf[[pcode_col, "geometry"]],
        ["cell_id", "coverage"],
        output="pandas",
    )

    rows = []
    cols = []
    vals = []
    for i, (cell_ids, coverage) in enumerate(
        zip(df_cov["cell_id"], df_cov["coverage"])
    ):
        keep = coverage > 0
        rows.append(np.full(keep.sum(), i))
        cols.append(cell_ids[keep])
        vals.append(coverage[keep])

    W = sparse.coo_matrix(
        (
            np.concatenate(vals).astype(np.float64),
            (
                np.concatenate(rows).astype(np.int64),
                np.concatenate(cols).astype(np.int64),
            ),
        ),
        shape=(len(gdf), height * width),
    ).tocsr()
    return W


def _weights_cache_path(iso3, adm_level, ds, gdf):
    """Cache filename keyed on the exact grid AND the exact boundaries.

    The boundary hash matters: the COD polygon cache in blob is refreshed
    by hand, and without it a refreshed boundary would silently reuse
    weights built against the previous one.
    """
    xmin, ymin, xmax, ymax, height, width = _grid_params(ds)
    h = hashlib.md5(
        f"{xmin:.6f}_{ymin:.6f}_{xmax:.6f}_{ymax:.6f}_{height}_{width}".encode()
    )
    for wkb in gdf.geometry.to_wkb():
        h.update(wkb)
    return os.path.join(
        WEIGHTS_CACHE_DIR,
        f"{iso3.lower()}_adm{adm_level}_{h.hexdigest()[:16]}.npz",
    )


def load_or_build_weights(gdf, pcode_col, iso3, adm_level, ds, logger=None):
    """
    Load the coverage-weight matrix for (iso3, adm_level, grid,
    boundaries) from the on-disk cache, or build and cache it. The cache
    lets weights be reused across date chunks and worker processes
    within a run (and across runs when the cache directory persists).
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    path = _weights_cache_path(iso3, adm_level, ds, gdf)
    if os.path.exists(path):
        try:
            return sparse.load_npz(path)
        except Exception:
            logger.warning(f"Could not read weights cache {path}, rebuilding")

    W = build_weights(gdf, pcode_col, ds)

    os.makedirs(WEIGHTS_CACHE_DIR, exist_ok=True)
    # Atomic write so concurrent workers never read a partial file
    fd, tmp_path = tempfile.mkstemp(dir=WEIGHTS_CACHE_DIR, suffix=".npz.tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            sparse.save_npz(f, W)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    return W


def weighted_zonal_stats(values, W, scale):
    """
    Compute zonal statistics for a stack of raster slices against a
    coverage-weight matrix.

    Parameters
    ----------
    values : numpy.ndarray
        Shape (n_slices, height, width). One slice per date (x
        leadtime / band).
    W : scipy.sparse.csr_matrix
        Coverage weights, shape (n_adms, height * width).
    scale : float
        Area ratio between the native resolution and
        ``UPSAMPLED_RESOLUTION``; applied to `count` and `sum` so
        values remain on the legacy upsampled-pixel scale.

    Returns
    -------
    dict of str -> numpy.ndarray
        Each stat maps to an array of shape (n_adms, n_slices).
    """
    n_slices = values.shape[0]
    n_adms = W.shape[0]

    X = values.reshape(n_slices, -1).T.astype(np.float64)  # (n_px, n_slices)
    finite = np.isfinite(X)
    Xf = np.where(finite, X, 0.0)

    w_count = W @ finite  # sum of coverage over finite pixels
    sums = W @ Xf
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(w_count > 0, sums / w_count, np.nan)

    # min / max / median / std need per-admin access to member pixel
    # values; W is sparse so each admin only touches its own pixels
    mins = np.full((n_adms, n_slices), np.nan)
    maxs = np.full((n_adms, n_slices), np.nan)
    medians = np.full((n_adms, n_slices), np.nan)
    stds = np.full((n_adms, n_slices), np.nan)

    indptr, indices, data = W.indptr, W.indices, W.data
    with warnings.catch_warnings():
        # All-NaN slices for an admin are expected (no coverage)
        warnings.simplefilter("ignore", category=RuntimeWarning)
        for a in range(n_adms):
            px = indices[indptr[a] : indptr[a + 1]]
            if len(px) == 0:
                continue
            w = data[indptr[a] : indptr[a + 1]]
            vals = X[px]  # (n_member_px, n_slices)
            mins[a] = np.nanmin(vals, axis=0)
            maxs[a] = np.nanmax(vals, axis=0)
            stds[a] = _weighted_std(vals, w, mean[a])
            medians[a] = _weighted_median(vals, w)

    # An admin unit smaller than half a pixel-equivalent rounds to zero
    # even though it has coverage and a valid mean. Keep the legacy
    # invariant "a non-null mean implies count >= 1", so downstream
    # `count > 0` filters don't silently drop the sub-pixel admin units
    # that exact coverage fractions are meant to rescue.
    counts = np.rint(w_count * scale).astype(int)
    counts = np.where((w_count > 0) & (counts == 0), 1, counts)

    return {
        "mean": mean,
        "max": maxs,
        "min": mins,
        "median": medians,
        "sum": sums * scale,
        "std": stds,
        "count": counts,
    }


def _weighted_std(vals, w, mean_row):
    """
    Coverage-weighted population std for one admin across all slices.

    Two-pass (subtracts the mean first) to avoid the catastrophic
    cancellation of the E[x^2] - E[x]^2 form for near-constant values.

    Parameters
    ----------
    vals : numpy.ndarray
        Shape (n_member_px, n_slices).
    w : numpy.ndarray
        Coverage weights, shape (n_member_px,).
    mean_row : numpy.ndarray
        Precomputed weighted means, shape (n_slices,).
    """
    finite = np.isfinite(vals)
    dev2 = np.where(finite, (vals - mean_row[None, :]) ** 2, 0.0)
    w_tot = w @ finite
    with np.errstate(invalid="ignore", divide="ignore"):
        var = np.where(w_tot > 0, (w @ dev2) / w_tot, np.nan)
    return np.sqrt(var)


def _weighted_median(vals, w):
    """
    Coverage-weighted median for one admin across all slices.

    Equivalent to the median of each pixel value replicated by its
    weight: the value(s) where the cumulative weight crosses half the
    total. Reduces exactly to ``np.median`` for integer weights.

    Parameters
    ----------
    vals : numpy.ndarray
        Shape (n_member_px, n_slices). May contain NaN.
    w : numpy.ndarray
        Coverage weights, shape (n_member_px,).

    Returns
    -------
    numpy.ndarray
        Shape (n_slices,). NaN where an admin has no finite pixels.
    """
    n_px, n_slices = vals.shape
    order = np.argsort(vals, axis=0)  # NaNs sort to the end
    v_sorted = np.take_along_axis(vals, order, axis=0)
    w_sorted = np.take_along_axis(
        np.broadcast_to(w[:, None], vals.shape), order, axis=0
    ).astype(np.float64)
    w_sorted = np.where(np.isnan(v_sorted), 0.0, w_sorted)

    cw = np.cumsum(w_sorted, axis=0)
    tot = cw[-1]
    half = tot / 2

    # First index where cumulative weight reaches half the total; if
    # the boundary falls exactly between two pixels, average them
    reached = cw >= half[None, :] - 1e-9 * np.maximum(tot, 1)
    lo_idx = np.clip(reached.argmax(axis=0), 0, n_px - 1)
    cols = np.arange(n_slices)
    at_boundary = np.isclose(cw[lo_idx, cols], half, rtol=1e-9, atol=1e-12)
    hi_idx = np.clip(lo_idx + at_boundary.astype(int), 0, n_px - 1)
    # Don't step past the last finite value
    hi_idx = np.where(np.isnan(v_sorted[hi_idx, cols]), lo_idx, hi_idx)

    median = 0.5 * (v_sorted[lo_idx, cols] + v_sorted[hi_idx, cols])
    return np.where(tot > 0, median, np.nan)


def zonal_stats_runner(
    ds,
    gdf,
    adm_level,
    iso3,
    logger=None,
):
    """
    Run weighted zonal stats for a clipped raster dataset over one
    admin level, for all dates (and leadtimes / bands) at once.

    Produces the same rows as the legacy ``fast_zonal_stats_runner``:
    one row per admin unit per date (per leadtime / band), with columns
    ``mean, max, min, median, sum, std, count, valid_date,
    [issued_date, leadtime | band,] pcode, adm_level, iso3``.

    Parameters
    ----------
    ds : xarray.DataArray
        Clipped raster (from ``clip_raster``) with dimensions `x`, `y`,
        `date`, and optionally `leadtime` or `band`.
    gdf : geopandas.GeoDataFrame
        Admin boundaries with an ``ADM{adm_level}_PCODE`` column.
    adm_level : int
        The administrative level of the boundaries.
    iso3 : str
        ISO3 code for the country.

    Returns
    -------
    pandas.DataFrame
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    dims = list(ds.dims)
    fourth_dim = next(
        (dim for dim in dims if dim not in {"x", "y", "date"}), None
    )
    ds = ds.transpose(
        *(["date", fourth_dim, "y", "x"] if fourth_dim else ["date", "y", "x"])
    )

    native_res = abs(ds.rio.resolution()[0])
    scale = (native_res / UPSAMPLED_RESOLUTION) ** 2

    W = load_or_build_weights(
        gdf, f"ADM{adm_level}_PCODE", iso3, adm_level, ds, logger=logger
    )

    dates = ds.date.values
    fourth_vals = ds[fourth_dim].values if fourth_dim else [None]
    n_dates = len(dates)
    n_fourth = len(fourth_vals)

    values = np.asarray(ds.values, dtype=np.float64).reshape(
        n_dates * n_fourth, ds.rio.height, ds.rio.width
    )

    # Legacy behaviour: all-NaN date/leadtime (or band) combos are
    # skipped -- expected for invalid forecast combos and band gaps
    slice_has_data = np.isfinite(values).any(axis=(1, 2))
    if fourth_dim is None:
        # The legacy 3D path never skipped all-NaN dates
        slice_has_data[:] = True
    keep = np.flatnonzero(slice_has_data)
    if len(keep) == 0:
        return None

    stats = weighted_zonal_stats(values[keep], W, scale)

    # Assemble the output frame: rows ordered by (date, fourth_dim)
    # then admin unit, matching the legacy row order
    pcodes = gdf[f"ADM{adm_level}_PCODE"].to_numpy()
    n_adms = len(pcodes)
    n_kept = len(keep)

    date_idx = keep // n_fourth
    fourth_idx = keep % n_fourth

    df_stats = pd.DataFrame(
        {
            stat: stats[stat].T.ravel()  # slice-major, admin-minor
            for stat in STATS
        }
    )
    df_stats["valid_date"] = np.repeat(dates[date_idx], n_adms)
    if fourth_dim:
        fourth_col = np.repeat(fourth_vals[fourth_idx], n_adms)
        if fourth_dim == "leadtime":
            df_stats["issued_date"] = [
                add_months_to_date(str(d)[:10], -int(lt))
                for d, lt in zip(
                    np.repeat(dates[date_idx], n_adms), fourth_col
                )
            ]
        df_stats[fourth_dim] = fourth_col
    df_stats["pcode"] = np.tile(pcodes, n_kept)
    df_stats["adm_level"] = adm_level
    df_stats["iso3"] = iso3

    return df_stats


def validate_stats_df(df, forecast):
    """
    Vectorized version of the legacy per-row ``validate_stats`` checks.
    Raises ValueError describing the first offending rows, if any.

    Parameters
    ----------
    df : pandas.DataFrame
        Output of ``zonal_stats_runner`` (all admin levels together).
    forecast : bool
        Whether the dataset has `issued_date` / `leadtime` columns.
    """
    errors = []

    def check(mask, message):
        if mask.any():
            errors.append(f"{message} ({int(mask.sum())} rows)")

    has_minmax = df["min"].notna() & df["max"].notna()
    # Small relative tolerance: the weighted mean of a constant-valued
    # admin can differ from min == max by float rounding (well below
    # the REAL precision the DB stores and checks against)
    tol = 1e-9 * (df["min"].abs() + df["max"].abs()) + 1e-12
    check(has_minmax & (df["min"] > df["max"]), "min > max")
    check(
        has_minmax
        & df["mean"].notna()
        & ~df["mean"].between(df["min"] - tol, df["max"] + tol),
        "mean outside [min, max]",
    )
    check(
        has_minmax
        & df["median"].notna()
        & ~df["median"].between(df["min"] - tol, df["max"] + tol),
        "median outside [min, max]",
    )
    check(df["std"].notna() & (df["std"] < 0), "std < 0")
    check(df["count"] < 0, "count < 0")
    check(~df["adm_level"].between(0, 4), "adm_level outside [0, 4]")
    check(
        ~df["iso3"].str.fullmatch("[A-Z]{3}"),
        "invalid iso3",
    )

    valid_date = pd.to_datetime(df["valid_date"])
    if forecast:
        issued_date = pd.to_datetime(df["issued_date"])
        check(valid_date < issued_date, "valid_date < issued_date")
        check(~df["leadtime"].between(0, 6), "leadtime outside [0, 6]")
        months_diff = (valid_date.dt.year - issued_date.dt.year) * 12 + (
            valid_date.dt.month - issued_date.dt.month
        )
        check(
            df["leadtime"].astype(int) != months_diff,
            "leadtime != months between issued_date and valid_date",
        )
    else:
        check(
            valid_date > pd.Timestamp.now(),
            "valid_date in the future",
        )

    if errors:
        raise ValueError(f"Validation error(s): {'; '.join(errors)}")

import datetime
import hashlib
import logging
import re
import warnings
from collections import OrderedDict

import coloredlogs
import numpy as np
import pandas as pd
import xarray as xr
from rasterio.enums import Resampling
from rasterio.features import rasterize

from src.config.settings import LOG_LEVEL, UPSAMPLED_RESOLUTION
from src.utils.database_utils import postgres_upsert
from src.utils.general_utils import add_days_to_date, add_months_to_date

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def validate_dimensions(ds):
    required_dims = {"x", "y", "date"}
    missing_dims = required_dims - set(ds.dims)
    if missing_dims:
        raise ValueError(
            f"Dataset missing required dimensions: {missing_dims}"
        )
    # Get the fourth dimension if it exists (not x, y, or date)
    dims = list(ds.dims)
    fourth_dim = next(
        (dim for dim in dims if dim not in {"x", "y", "date"}), None
    )
    return fourth_dim


def validate_stats(iso3, df_stats):
    """
    Check that zonal stats are consistent, raising a ValueError naming the
    first offending value. Checks whole columns at once rather than row by
    row, since a country can have hundreds of thousands of rows.
    """
    if df_stats.empty:
        return

    def validation_check(mask, message):
        if mask.any():
            raise ValueError(message(df_stats[mask].iloc[0]))

    min_, max_ = df_stats["min"], df_stats["max"]
    # min/mean/median are only checked against each other when min and max
    # are both set
    has_range = min_.notna() & max_.notna()
    validation_check(
        has_range & ~(min_ <= max_),
        lambda s: f"Validation error: min {s['min']} is not <= max {s['max']} ",
    )
    mean = df_stats["mean"]
    validation_check(
        has_range & mean.notna() & ~((min_ <= mean) & (mean <= max_)),
        lambda s: (
            f"Validation error: mean {s['mean']} is not between "
            f"min {s['min']} and max {s['max']} "
        ),
    )
    median = df_stats["median"]
    validation_check(
        has_range & median.notna() & ~((min_ <= median) & (median <= max_)),
        lambda s: (
            f"Validation error: median {s['median']} is not"
            f" between min {s['min']} & max {s['max']}"
        ),
    )

    std = df_stats["std"]
    validation_check(
        std.notna() & ~(std >= 0),
        lambda s: f"Validation error: std {s['std']} is not >= 0",
    )
    validation_check(
        ~(df_stats["count"] >= 0),
        lambda s: f"Validation error: count {s['count']} is not >= 0",
    )
    validation_check(
        ~df_stats["adm_level"].between(0, 4),
        lambda s: (
            f"Validation error: adm_level {s['adm_level']} is not between 0 and 4"
        ),
    )
    if not re.search("^[A-Z]{3}$", iso3):
        raise ValueError(f"Validation error: iso3 {iso3} is not valid")

    valid_date = pd.to_datetime(df_stats["valid_date"])
    if "issued_date" in df_stats:
        # Forecast tables
        issued_date = pd.to_datetime(df_stats["issued_date"])
        validation_check(
            ~(valid_date >= issued_date),
            lambda s: (
                f"Validation error: issued_date {s['issued_date']} is not <= "
                f"valid_date {s['valid_date']}"
            ),
        )
        # TODO figure out better logic for leadtime in days and in months
        if "leadtime" in df_stats:
            validation_check(
                ~df_stats["leadtime"].astype(int).between(0, 16),
                lambda s: (
                    f"Validation error: leadtime {s['leadtime']} is not "
                    "between 0 and 16"
                ),
            )
    else:
        # Observational tables
        validation_check(
            ~(valid_date <= datetime.datetime.now()),
            lambda s: (
                f"Validation error: valid_date {s['valid_date']} is not <= "
                "current date"
            ),
        )


def fast_zonal_stats_runner(
    ds,
    gdf,
    adm_level,
    iso3,
    stats=["mean", "max", "min", "median", "sum", "std", "count"],
    rast_fill=np.nan,
    save_to_database=False,
    engine=None,
    dataset=None,
    logger=None,
):
    """
    Run zonal stats calculations for a raster dataset over administrative boundaries
    and optionally save the results to a database.

    Parameters
    ----------
    ds : xarray.Dataset
        The input raster dataset. Must have dimensions 'x', 'y', and 'date',
        with an optional fourth dimension (e.g., 'band' or 'leadtime').
    gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing the administrative boundaries.
    adm_level : int
        The administrative level for the boundaries (e.g., 0 for country, 1 for state).
    iso3 : str
        ISO3 code for the country.
    stats : list of str, optional
        List of statistics to compute. Supported values are "mean", "max", "min",
        "median", "sum", "std", and "count".
    rast_fill : float, optional
        Value to fill the raster for missing data. Default is np.nan.
    save_to_database : bool, optional
        If True, the results will be saved to the database. Default is False.
    engine : sqlalchemy.engine.base.Engine, optional
        SQLAlchemy engine for the database connection. Required if `save_to_database` is True.
    dataset : str, optional
        The name of the dataset/table in the database. Required if `save_to_database` is True.

    Returns
    -------
    pandas.DataFrame or None
        A DataFrame containing the calculated zonal statistics for each date and
        administrative unit. If `save_to_database` is True, the DataFrame is saved
        to the database and None is returned.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    fourth_dim = validate_dimensions(ds)

    adm_ids = gdf[f"ADM{adm_level}_PCODE"]
    n_adms = len(adm_ids)
    # Rasterize the adm bounds and group the pixels by admin unit. This is
    # the same for every slice, and for every task with the same shapes and
    # grid, so it's cached
    admin_raster = None
    groups = cached_admin_pixel_groups(
        gdf,
        ds.rio.width,
        ds.rio.height,
        ds.rio.transform(),
        n_adms,
        rast_fill,
    )

    outputs = []
    for date in ds.date.values:
        logger.debug(f"Calculating for {date}...")
        ds_sel = ds.sel(date=date)

        if fourth_dim:  # 4D case
            # Read the date's data once, rather than once per check and stat
            date_values = ds_sel.transpose(fourth_dim, "y", "x").values
            for val, values in zip(ds_sel[fourth_dim].values, date_values):
                # Skip if all values are NaN
                if bool(np.all(np.isnan(values))):
                    continue
                results = fast_zonal_stats(
                    values,
                    admin_raster,
                    n_adms,
                    stats=stats,
                    rast_fill=rast_fill,
                    groups=groups,
                )
                for i, result in enumerate(results):
                    result["valid_date"] = date
                    if fourth_dim == "leadtime":
                        if dataset == "chirps":
                            result["issued_date"] = date
                            result["valid_date"] = add_days_to_date(date, val)
                        else:
                            result["issued_date"] = add_months_to_date(
                                date, -val
                            )

                    result["pcode"] = adm_ids[i]
                    result["adm_level"] = adm_level
                    result[
                        fourth_dim
                    ] = val  # Store the fourth dimension value
                outputs.extend(results)
        else:  # 3D case
            results = fast_zonal_stats(
                ds_sel.transpose("y", "x").values,
                admin_raster,
                n_adms,
                stats=stats,
                rast_fill=rast_fill,
                groups=groups,
            )
            for i, result in enumerate(results):
                result["valid_date"] = date
                result["pcode"] = adm_ids[i]
                result["adm_level"] = adm_level
            outputs.extend(results)

    df_stats = pd.DataFrame(outputs)
    df_stats["iso3"] = iso3

    if save_to_database and engine and dataset:
        logger.warning(
            f"In raster utils, writing {len(df_stats)} rows to database..."
        )
        df_stats.to_sql(
            dataset,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=100000,
            method=postgres_upsert,
        )
        return
    return df_stats


def admin_pixel_groups(admin_raster, n_adms=None, rast_fill=np.nan):
    """
    Work out where each admin unit's pixels go in the padded
    (n_features, largest_geom) array that `fast_zonal_stats` computes over.

    This only depends on the admin raster, so it can be computed once and
    reused for every date/leadtime slice of the source raster.

    Parameters
    ----------
    admin_raster : numpy.ndarray
        A raster (2D array) of admin unit ids, NaN outside all units.
    n_adms: int, optional
        Number of admin units (as not all may be present in the admin_raster)
    rast_fill : float, optional
        Fill value marking pixels outside all units. Default is np.nan.

    Returns
    -------
    tuple
        (pixel_idx, rows, cols, n_features, largest_geom): flat indices into
        the raster of every pixel inside a unit, the row (unit id) and column
        each one goes to, and the shape of the padded array.
    """
    flat = np.asarray(admin_raster, dtype=float).ravel()
    inside = ~np.isnan(flat)
    if not np.isnan(rast_fill):
        inside &= flat != rast_fill
    valid = np.flatnonzero(inside)

    geom_ids, pixel_count = np.unique(flat[valid], return_counts=True)
    largest_geom = pixel_count.max()
    n_features = n_adms if n_adms else (int(geom_ids.max()) + 1)

    # A stable sort keeps each unit's pixels in raster order
    pixel_idx = valid[np.argsort(flat[valid], kind="stable")]
    rows = flat[pixel_idx].astype(int)
    group_starts = np.repeat(np.cumsum(pixel_count) - pixel_count, pixel_count)
    cols = np.arange(len(pixel_idx)) - group_starts

    return pixel_idx, rows, cols, n_features, largest_geom


# Admin pixel groups by (shapes, grid), kept across tasks in the same worker
# process, least recently used first. Capped in bytes, since a country's
# groups take ~12 bytes per pixel.
_ADMIN_GROUPS_CACHE = OrderedDict()
_ADMIN_GROUPS_CACHE_MAX_BYTES = 512 * 1024**2


def cached_admin_pixel_groups(
    gdf, src_width, src_height, src_transform, n_adms, rast_fill=np.nan
):
    """
    `rasterize_admin` followed by `admin_pixel_groups`, cached so the same
    shapes on the same grid are only rasterized once per process.

    The cache key hashes the geometries themselves, so a different set of
    shapes never gets another's result.
    """
    shapes_hash = hashlib.sha1()
    for geom_wkb in gdf.geometry.to_wkb():
        shapes_hash.update(geom_wkb)
    shapes_hash.update(np.asarray(gdf.index).tobytes())
    key = (
        shapes_hash.hexdigest(),
        src_width,
        src_height,
        tuple(src_transform),
        n_adms,
        repr(rast_fill),
    )
    if key in _ADMIN_GROUPS_CACHE:
        _ADMIN_GROUPS_CACHE.move_to_end(key)
        return _ADMIN_GROUPS_CACHE[key]

    admin_raster = rasterize_admin(
        gdf.copy(), src_width, src_height, src_transform, all_touched=False
    )
    pixel_idx, rows, cols, n_features, largest_geom = admin_pixel_groups(
        admin_raster, n_adms, rast_fill
    )
    # Smaller index types, so more countries fit in the cache
    groups = (
        pixel_idx.astype(np.int32),
        rows.astype(np.int32),
        cols.astype(np.int32),
        n_features,
        largest_geom,
    )

    _ADMIN_GROUPS_CACHE[key] = groups
    cache_bytes = sum(
        sum(a.nbytes for a in g[:3]) for g in _ADMIN_GROUPS_CACHE.values()
    )
    max_bytes = _ADMIN_GROUPS_CACHE_MAX_BYTES
    while cache_bytes > max_bytes and len(_ADMIN_GROUPS_CACHE) > 1:
        _, evicted = _ADMIN_GROUPS_CACHE.popitem(last=False)
        cache_bytes -= sum(a.nbytes for a in evicted[:3])
    return groups


def fast_zonal_stats(
    src_raster,
    admin_raster,
    n_adms=None,
    stats=["mean", "max", "min", "median", "sum", "std", "count"],
    rast_fill=np.nan,
    groups=None,
):
    """
    Compute zonal statistics for a source raster dataset over given administrative regions.
    Implementation is adapted from `python-rasterstats`, following these performance recommendations:
    https://github.com/sdtaylor/python-rasterstats/commit/12d0432128bdb66aacaf7a65e753f28616febe11

    Parameters
    ----------
    src_raster : numpy.ndarray
        The source raster data array (2D array) for which statistics are computed.
    admin_raster : numpy.ndarray
        A raster (2D array) representing administrative regions, where each unique value
        corresponds to a different administrative unit.
    n_adms: int, optional
        Number of admin units (as not all may be present in the admin_raster)
    stats : list of str, optional
        List of statistics to compute. Supported values are "mean", "max", "min",
        "median", "sum", "std", and "count".
    rast_fill : float, optional
        Value to use as a fill for missing data in the raster. Default is np.nan.
    groups : tuple, optional
        The output of `admin_pixel_groups` for `admin_raster`. Pass it when
        computing stats for many rasters over the same admin raster, so the
        grouping is only worked out once.

    Returns
    -------
    list of dict
        A list of dictionaries, where each dictionary contains the computed statistics
        for a particular administrative unit.
    """
    if groups is None:
        groups = admin_pixel_groups(admin_raster, n_adms, rast_fill)
    pixel_idx, rows, cols, n_features, largest_geom = groups

    sorted_array = np.full((n_features, largest_geom), rast_fill, dtype=float)
    sorted_array[rows, cols] = np.asarray(src_raster).ravel()[pixel_idx]

    feature_stats = [{} for i in range(n_features)]

    # TODO: Temp suppress while developing!
    # This is suppressing warnings when all values in a slice are NA,
    # which is expected in some cases where there are no pixel centroids in an adm
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        stat_functions = {
            "mean": np.nanmean,
            "median": np.nanmedian,
            "max": np.nanmax,
            "min": np.nanmin,
            "sum": np.nansum,
            "std": np.nanstd,
            "count": lambda x, axis: np.sum(~np.isnan(x), axis=axis),
            "unique": lambda x, axis: np.array(
                [len(np.unique(row[~np.isnan(row)])) for row in x]
            ),
        }

        for stat in stats:
            if stat in stat_functions:
                values = stat_functions[stat](sorted_array, axis=1)
                for i, value in enumerate(values):
                    feature_stats[i][stat] = value

    return feature_stats


def upsample_raster(
    ds, resampled_resolution=UPSAMPLED_RESOLUTION, logger=None
):
    """
    Upsample a raster to a higher resolution using nearest neighbor resampling,
    via the `Resampling.nearest` method from `rasterio`.

    Parameters
    ----------
    ds : xarray.Dataset
        The raster data set to upsample. Must have dimensions 'x', 'y', and 'date',
        with an optional fourth dimension (e.g., 'band' or 'leadtime').
    resampled_resolution : float, optional
        The desired resolution for the upsampled raster.

    Returns
    -------
    xarray.Dataset
        The upsampled raster as a Dataset with the new resolution.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    fourth_dim = validate_dimensions(ds)

    # Assuming square resolution
    input_resolution = ds.rio.resolution()[0]
    upscale_factor = input_resolution / resampled_resolution

    logger.debug(
        f"Input resolution is {input_resolution}. Upscaling by a factor of {upscale_factor}."
    )

    new_width = int(ds.rio.width * upscale_factor)
    new_height = int(ds.rio.height * upscale_factor)

    logger.debug(
        f"New raster will have a width of {new_width} pixels and height of {new_height} pixels."
    )

    if ds.rio.crs is None:
        logger.warning(
            "Input raster data did not have CRS defined. Setting to EPSG:4326."
        )
        ds = ds.rio.write_crs("EPSG:4326")

    if fourth_dim:  # 4D case
        resampled_arrays = []

        for val in ds[fourth_dim].values:
            ds_ = ds.sel({fourth_dim: val})
            ds_ = ds_.rio.reproject(
                ds_.rio.crs,
                shape=(new_height, new_width),
                resampling=Resampling.nearest,
                nodata=np.nan,
            )
            # Expand along the fourth dimension
            if fourth_dim == "band":
                # Falls under different bands, use the long_name instead of integer value
                if int(ds_["band"]) == 1:
                    ds_ = ds_.expand_dims({"band": ["SFED"]})
                else:
                    ds_ = ds_.expand_dims({"band": ["MFED"]})
            else:
                ds_ = ds_.expand_dims([fourth_dim])
            resampled_arrays.append(ds_)

        ds_resampled = xr.combine_by_coords(
            resampled_arrays, combine_attrs="drop"
        )
    else:  # 3D case (x, y, date)
        ds_resampled = ds.rio.reproject(
            ds.rio.crs,
            shape=(new_height, new_width),
            resampling=Resampling.nearest,
            nodata=np.nan,
        )

    return ds_resampled


def prep_raster(ds, gdf_adm, logger=None, upsample=True):
    """
    Prepares and resamples a raster dataset by clipping it to the bounds of the
    provided administrative regions and then upsampling the result.

    The clipped dataset is persisted in memory to optimize performance for
    subsequent operations.

    Parameters
    ----------
    ds : xarray.Dataset
        The input raster dataset to be clipped and resampled. It is assumed that
        the dataset has `x` and `y` coordinates corresponding to longitude and latitude.
    gdf_adm : geopandas.GeoDataFrame
        A GeoDataFrame containing the administrative boundaries. The function uses
        the bounding box of this GeoDataFrame to clip the raster dataset.

    Returns
    -------
    xarray.Dataset
        The clipped and upsampled raster dataset.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.NullHandler())

    logger.debug("Clipping raster to iso3 bounds and persisting in memory...")
    minx, miny, maxx, maxy = gdf_adm.total_bounds
    ds_clip = ds.sel(x=slice(minx, maxx), y=slice(maxy, miny)).persist()
    if upsample:
        logger.debug("Upsampling raster...")
        ds_clip = upsample_raster(ds_clip, logger=logger)
    logger.debug("Raster prep completed.")
    return ds_clip


def rasterize_admin(
    gdf,
    src_width,
    src_height,
    src_transform,
    rast_fill=np.nan,
    all_touched=False,
):
    """
    Rasterize a GeoDataFrame of administrative boundaries.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the geometries to rasterize.
    src_width : int
        Width of the output raster in pixels.
    src_height : int
        Height of the output raster in pixels.
    src_transform : affine.Affine
        Affine transform defining the spatial reference for the output raster.
    rast_fill : float, optional
        Fill value for areas outside the geometries. Default is `np.nan`.
    all_touched : bool, optional
        Whether to rasterize pixels that are touched by geometries' boundaries.
        Default is `False` (only pixels whose center falls within a geometry are rasterized).

    Returns
    -------
    numpy.ndarray
        A 2D array representing the rasterized administrative regions. Each admin region is given an id
        that matches the index location in the input gdf. If `all_touched=True`, then some admin regions
        may not be present in the output raster (if they do not have overlap with any pixel centroids)
    """
    gdf["geometry"] = gdf["geometry"].simplify(
        tolerance=0.001, preserve_topology=True
    )
    geometries = [
        (geom, value)
        for geom, value in zip(gdf.geometry, gdf.reset_index()["index"])
    ]
    admin_raster = rasterize(
        shapes=geometries,
        out_shape=(src_height, src_width),
        transform=src_transform,
        fill=rast_fill,
        all_touched=all_touched,
    )
    return admin_raster

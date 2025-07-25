import datetime
import logging
import re
import warnings

import coloredlogs
import numpy as np
import pandas as pd
import xarray as xr
from dateutil.relativedelta import relativedelta
from rasterio.enums import Resampling
from rasterio.features import rasterize

from src.config.settings import LOG_LEVEL, UPSAMPLED_RESOLUTION
from src.utils.database_utils import postgres_upsert
from src.utils.general_utils import add_months_to_date

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


def validate_stats(iso3, stats):
    logger.debug("Validating rows...")

    pattern = re.compile("^[A-Z]{3}$")

    if "valid_date" in stats:
        if type(stats["valid_date"]) == np.str_:
            valid_date = datetime.datetime.strptime(
                stats["valid_date"], "%Y-%m-%d"
            )
        else:
            valid_date = pd.to_datetime(stats["valid_date"]).to_pydatetime()

    # All tables
    if np.isnan(stats["min"]) or np.isnan(stats["max"]):
        logger.debug("Min and/or max is np.nan")
    else:
        if not (stats["min"] <= stats["max"]):
            raise ValueError(
                f"Validation error: min {stats['min']} is not <= max {stats['max']} "
            )

        if np.isnan(stats["mean"]):
            logger.debug("Mean is np.nan")
        else:
            if not (stats["min"] <= stats["mean"] <= stats["max"]):
                raise ValueError(
                    f"Validation error: mean {stats['mean']} is not between min {stats['min']} and max {stats['max']} "
                )

        if np.isnan(stats["median"]):
            logger.debug("Median is np.nan")
        else:
            if not (stats["min"] <= stats["median"] <= stats["max"]):
                raise ValueError(
                    f"Validation error: median {stats['median']} is not between min {stats['min']} & max {stats['max']}"
                )

    if np.isnan(stats["std"]):
        logger.debug("Std is np.nan")
    else:
        if not stats["std"] >= 0:
            raise ValueError(
                f"Validation error: std {stats['std']} is not >= 0"
            )

    if not stats["count"] >= 0:
        raise ValueError(
            f"Validation error: count {stats['count']} is not >= 0"
        )

    if not (0 <= stats["adm_level"] <= 4):
        raise ValueError(
            f"Validation error: adm_level {stats['adm_level']} is not between 0 and 4"
        )
    elif not pattern.search(iso3):
        raise ValueError(f"Validation error: iso3 {iso3} is not valid")

    # Forecast tables
    if "issued_date" in stats:
        issued_date = stats["issued_date"]
        if type(issued_date) != datetime.datetime:
            issued_date = datetime.datetime.strptime(
                stats["issued_date"], "%Y-%m-%d"
            )
        if not valid_date >= issued_date:
            raise ValueError(
                f"Validation error: issued_date {issued_date} is not <= valid_date {valid_date}"
            )
        elif "leadtime" in stats:
            leadtime = int(stats["leadtime"])
            if not (0 <= leadtime <= 6):
                raise ValueError(
                    f"Validation error: leadtime {stats['leadtime']} is not between 0 and 6"
                )
            elif leadtime != relativedelta(valid_date, issued_date).months:
                raise ValueError(
                    f"Validation error: leadtime {leadtime} should match the diff between issued_date {issued_date} and"
                    f" valid_date {valid_date}"
                )
    else:
        # Observational tables
        if not valid_date <= datetime.datetime.now():
            raise ValueError(
                f"Validation error: valid_date {stats['valid_date']} is not <= current date"
            )

    return stats


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

    # Rasterize the adm bounds
    src_transform = ds.rio.transform()
    src_width = ds.rio.width
    src_height = ds.rio.height
    admin_raster = rasterize_admin(
        gdf, src_width, src_height, src_transform, all_touched=False
    )
    adm_ids = gdf[f"ADM{adm_level}_PCODE"]
    n_adms = len(adm_ids)

    outputs = []
    for date in ds.date.values:
        logger.debug(f"Calculating for {date}...")
        ds_sel = ds.sel(date=date)

        if fourth_dim:  # 4D case
            for val in ds_sel[fourth_dim].values:
                ds__ = ds_sel.sel({fourth_dim: val})
                # Skip if all values are NaN
                if bool(np.all(np.isnan(ds__.values))):
                    continue
                results = fast_zonal_stats(
                    ds__.values,
                    admin_raster,
                    n_adms,
                    stats=stats,
                    rast_fill=rast_fill,
                )
                for i, result in enumerate(results):
                    result["valid_date"] = date
                    # Special handling for leadtime dimension
                    if fourth_dim == "leadtime":
                        result["issued_date"] = add_months_to_date(date, -val)
                    result["pcode"] = adm_ids[i]
                    result["adm_level"] = adm_level
                    result[
                        fourth_dim
                    ] = val  # Store the fourth dimension value
                    validate_stats(iso3, result)
                outputs.extend(results)
        else:  # 3D case
            results = fast_zonal_stats(
                ds_sel.values,
                admin_raster,
                n_adms,
                stats=stats,
                rast_fill=rast_fill,
            )
            for i, result in enumerate(results):
                result["valid_date"] = date
                result["pcode"] = adm_ids[i]
                result["adm_level"] = adm_level
                validate_stats(iso3, result)
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


def fast_zonal_stats(
    src_raster,
    admin_raster,
    n_adms=None,
    stats=["mean", "max", "min", "median", "sum", "std", "count"],
    rast_fill=np.nan,
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

    Returns
    -------
    list of dict
        A list of dictionaries, where each dictionary contains the computed statistics
        for a particular administrative unit.
    """

    stacked_arrays = np.stack([src_raster, admin_raster])

    # Don't include the fill nans in our counts
    drop_nans = stacked_arrays[1][~np.isnan(stacked_arrays[1])]
    geom_ids, pixel_count = np.unique(drop_nans, return_counts=True)

    if geom_ids[0] == rast_fill:
        geom_ids = geom_ids[1:]
        pixel_count = pixel_count[1:]

    largest_geom = pixel_count.max()
    n_features = n_adms if n_adms else (int(geom_ids.max()) + 1)

    sorted_array = np.empty(shape=(n_features, largest_geom))
    sorted_array[:] = rast_fill
    for geom_i, n_pixels in zip(geom_ids, pixel_count):
        sorted_array[int(geom_i), 0:n_pixels] = stacked_arrays[0][
            stacked_arrays[1] == geom_i
        ]

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


def prep_raster(ds, gdf_adm, logger=None):
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
    logger.debug("Upsampling raster...")
    ds_resampled = upsample_raster(ds_clip, logger=logger)
    logger.debug("Raster prep completed.")
    return ds_resampled


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

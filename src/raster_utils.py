import logging
import warnings

import coloredlogs
import numpy as np
import pandas as pd
import xarray as xr
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterstats import zonal_stats

from config import LOG_LEVEL
from src.database_utils import postgres_upsert

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def compute_zonal_statistics(
    ds,
    gdf,
    id_col,
    admin_level,
    iso3,
    geom_col="geometry",
    lat_coord="y",
    lon_coord="x",
    stats=None,
    all_touched=False,
    simplify_geom=True,
):
    """
    Compute zonal statistics for a raster dataset using a GeoDataFrame of polygons.

    The function uses the `zonal_stats` function from the `rasterstats`
    package to perform the computations. See more docs here:
    https://pythonhosted.org/rasterstats/manual.html#zonal-statistics

    Parameters
    ----------
    ds : xarray.Dataset
        The raster data set to perform zonal statistics on.
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the polygon geometries for the zones.
    id_col : str
        The column in `gdf` that contains unique identifiers for the polygons.
    admin_level : int
        Admin level of the input boundaries. Used to label the output data.
    iso3 : str
        ISO3 country code.
    geom_col : str, optional
        The column in `gdf` that contains the geometry of the polygons. Default is "geometry".
    lat_coord : str, optional
        The name of the latitude coordinate in the DataArray. Default is "y".
    lon_coord : str, optional
        The name of the longitude coordinate in the DataArray. Default is "x".
    stats : list of str, optional
        List of statistics to compute. If None, a default set of statistics is used,
        including mean, median, std, min, max, sum, count.
    all_touched : bool, optional
        Whether to include all pixels touched by geometries, or only those whose center
        is within the polygon. Default is False.

    Returns
    -------
    df_stats : pandas.DataFrame
        A DataFrame with the computed zonal statistics, including the unique identifier
        from `gdf` and the calculated statistics.
    """

    if not stats:
        stats = ["mean", "median", "std", "min", "max", "sum", "count"]
    coords_transform = ds.rio.set_spatial_dims(
        x_dim=lon_coord, y_dim=lat_coord
    ).rio.transform()

    if simplify_geom:
        gdf[geom_col] = gdf[geom_col].simplify(tolerance=0.001, preserve_topology=True)

    outputs = []
    for date in ds.date.values:
        da_ = ds.sel(date=date)
        # Forecast data will have 3 dims, since we have a leadtime
        nd = len(list(da_.dims))
        if nd == 3:
            for lt in da_.leadtime.values:
                da__ = da_.sel(leadtime=lt)
                # Some leadtime/date combos are invalid and so don't have any data
                if bool(np.all(np.isnan(da__.values))):
                    continue
                result = zonal_stats(
                    vectors=gdf[[geom_col]],
                    raster=da__.values,
                    affine=coords_transform,
                    nodata=np.nan,
                    all_touched=all_touched,
                    stats=stats,
                )

                # TODO: How slow is this? Is this still better than going to a df?
                for i, stat in enumerate(result):
                    stat["valid_date"] = date
                    stat["leadtime"] = lt
                    stat["pcode"] = gdf[id_col][i]
                    stat["adm_level"] = admin_level
                outputs.extend(result)
        # Non forecast data
        elif nd == 2:
            result = zonal_stats(
                vectors=gdf[[geom_col]],
                raster=da_.values,
                affine=coords_transform,
                nodata=np.nan,
                all_touched=all_touched,
                stats=stats,
            )

            for i, stat in enumerate(result):
                stat["valid_date"] = date
                stat["pcode"] = gdf[id_col][i]
                stat["adm_level"] = admin_level

            outputs.extend(result)
        else:
            raise Exception("Input Dataset must have 2 or 3 dimensions.")

    df_stats = pd.DataFrame(outputs)
    df_stats = df_stats.round(2)
    df_stats["iso3"] = iso3

    return df_stats


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
):
    # TODO: Pre-compute and save
    # Rasterize the adm bounds
    src_transform = ds.rio.transform()
    src_width = ds.rio.width
    src_height = ds.rio.height
    admin_raster = rasterize_admin(
        gdf, src_width, src_height, src_transform, all_touched=False
    )
    adm_ids = gdf[f"ADM{adm_level}_PCODE"]

    outputs = []
    # TODO: Can this be vectorized further?
    for date in ds.date.values:
        logger.debug(f"Calculating for {date}...")
        src_raster = ds.sel(date=date).values
        stats = fast_zonal_stats(src_raster, admin_raster, stats, rast_fill=rast_fill)

        for i, stat in enumerate(stats):
            stat["valid_date"] = date
            stat["pcode"] = adm_ids[i]
            stat["adm_level"] = adm_level

        outputs.extend(stats)

    df_stats = pd.DataFrame(outputs)
    df_stats = df_stats.round(2)
    df_stats["iso3"] = iso3

    logger.info(f"Writing {len(df_stats)} rows to database...")
    if save_to_database and engine and dataset:
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


# Adapted from https://github.com/sdtaylor/python-rasterstats/commit/12d0432128bdb66aacaf7a65e753f28616febe11
def fast_zonal_stats(
    src_raster,
    admin_raster,
    stats=["mean", "max", "min", "median", "sum", "std", "count"],
    rast_fill=np.nan,
):
    """
    A more performant approach to compute zonal statistics for a
    raster dataset over administrative regions.

    """

    stacked_arrays = np.stack([src_raster, admin_raster])

    # Don't include the fill nans in our counts
    drop_nans = stacked_arrays[1][~np.isnan(stacked_arrays[1])]
    geom_ids, pixel_count = np.unique(drop_nans, return_counts=True)

    if geom_ids[0] == rast_fill:
        geom_ids = geom_ids[1:]
        pixel_count = pixel_count[1:]

    largest_geom = pixel_count.max()
    n_features = int(geom_ids.max()) + 1

    sorted_array = np.empty(shape=(n_features, largest_geom))
    sorted_array[:] = rast_fill
    for geom_i, n_pixels in zip(geom_ids, pixel_count):
        sorted_array[int(geom_i), 0:n_pixels] = stacked_arrays[0][
            stacked_arrays[1] == geom_i
        ]

    feature_stats = [{} for i in range(n_features)]

    # TODO: Temp suppress while developing!
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        if "mean" in stats:
            for i, value in enumerate(np.nanmean(sorted_array, 1)):
                feature_stats[i]["mean"] = value
        if "median" in stats:
            for i, value in enumerate(np.nanmedian(sorted_array, 1)):
                feature_stats[i]["median"] = value
        if "max" in stats:
            for i, value in enumerate(np.nanmax(sorted_array, 1)):
                feature_stats[i]["max"] = value
        if "min" in stats:
            for i, value in enumerate(np.nanmin(sorted_array, 1)):
                feature_stats[i]["min"] = value
        if "sum" in stats:
            for i, value in enumerate(np.nansum(sorted_array, 1)):
                feature_stats[i]["sum"] = value
        if "std" in stats:
            for i, value in enumerate(np.nanstd(sorted_array, 1)):
                feature_stats[i]["std"] = value
        if "count" in stats:
            for i, value in enumerate(np.sum(~np.isnan(sorted_array), axis=1)):
                feature_stats[i]["count"] = value

    return feature_stats


def upsample_raster(ds, resampled_resolution=0.05):
    """
    Upsample a raster to a higher resolution using nearest neighbor resampling,
    via the `Resampling.nearest` method from `rasterio`.

    Parameters
    ----------
    ds : xarray.Dataset
        The raster data set to upsample. Must not have >4 dimensions.
    resampled_resolution : float, optional
        The desired resolution for the upsampled raster. Default is 0.05.

    Returns
    -------
    xarray.Dataset
        The upsampled raster as a Dataset with the new resolution.
    """
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

    # Forecast data will have 4 dims, since we have a leadtime
    nd = len(list(ds.dims))
    if nd == 4:
        resampled_arrays = []
        for lt in ds.leadtime.values:
            ds_ = ds.sel(leadtime=lt)
            ds_ = ds_.rio.reproject(
                ds_.rio.crs,
                shape=(ds_.rio.height * 2, ds_.rio.width * 2),
                resampling=Resampling.nearest,
                nodata=np.nan,
            )
            ds_ = ds_.expand_dims(["leadtime"])
            resampled_arrays.append(ds_)

        ds_resampled = xr.combine_by_coords(resampled_arrays, combine_attrs="drop")
    elif (nd == 2) or (nd == 3):
        ds_resampled = ds.rio.reproject(
            ds.rio.crs,
            shape=(new_height, new_width),
            resampling=Resampling.nearest,
            nodata=np.nan,
        )
    else:
        raise Exception("Input Dataset must have 2, 3, or 4 dimensions.")

    return ds_resampled


def prep_raster(ds, gdf_adm):
    logger.debug("Clipping raster to iso3 bounds and persisting in memory...")
    minx, miny, maxx, maxy = gdf_adm.total_bounds
    ds_clip = ds.sel(x=slice(minx, maxx), y=slice(maxy, miny)).persist()
    logger.debug("Upsampling raster...")
    ds_resampled = upsample_raster(ds_clip)
    logger.debug("Raster prep completed.")
    return ds_resampled


def rasterize_admin(
    gdf, src_width, src_height, src_transform, rast_fill=np.nan, all_touched=False
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
        A 2D array representing the rasterized administrative regions.
    """
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.001, preserve_topology=True)
    geometries = [
        (geom, value) for geom, value in zip(gdf.geometry, gdf.reset_index()["index"])
    ]
    admin_raster = rasterize(
        shapes=geometries,
        out_shape=(src_height, src_width),
        transform=src_transform,
        fill=rast_fill,
        all_touched=all_touched,
    )
    return admin_raster

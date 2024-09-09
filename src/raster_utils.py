import logging

import coloredlogs
import numpy as np
import pandas as pd
import xarray as xr
from rasterio.enums import Resampling
from rasterstats import zonal_stats

from config import LOG_LEVEL

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
    minx, miny, maxx, maxy = gdf_adm.total_bounds
    ds_clip = ds.sel(x=slice(minx, maxx), y=slice(maxy, miny)).persist()
    ds_resampled = upsample_raster(ds_clip)
    return ds_resampled

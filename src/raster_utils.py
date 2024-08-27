import numpy as np
import pandas as pd
from rasterio.enums import Resampling
from rasterstats import zonal_stats


def compute_zonal_statistics(
    da,
    gdf,
    id_col,
    geom_col="geometry",
    lat_coord="y",
    lon_coord="x",
    stats=None,
    all_touched=False,
    date=None,
):
    """
    Compute zonal statistics for a raster dataset using a GeoDataFrame of polygons.

    The function uses the `zonal_stats` function from the `rasterstats`
    package to perform the computations. See more docs here:
    https://pythonhosted.org/rasterstats/manual.html#zonal-statistics

    Parameters
    ----------
    da : xarray.DataArray
        The raster data array to perform zonal statistics on.
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the polygon geometries for the zones.
    id_col : str
        The column in `gdf` that contains unique identifiers for the polygons.
    geom_col : str, optional
        The column in `gdf` that contains the geometry of the polygons. Default is "geometry".
    lat_coord : str, optional
        The name of the latitude coordinate in the DataArray. Default is "y".
    lon_coord : str, optional
        The name of the longitude coordinate in the DataArray. Default is "x".
    stats : list of str, optional
        List of statistics to compute. If None, a default set of statistics is used,
        including mean, std, min, max, sum, count, and percentiles.
    all_touched : bool, optional
        Whether to include all pixels touched by geometries, or only those whose center
        is within the polygon. Default is False.
    date : str or datetime-like, optional
        Date to associate with the computed statistics.
        Added as a "date" column in the result. Default is None.

    Returns
    -------
    df_stats : pandas.DataFrame
        A DataFrame with the computed zonal statistics, including the unique identifier
        from `gdf` and the calculated statistics.
    """

    if not stats:
        stats = ["mean", "std", "min", "max", "sum", "count"]
        percentiles = [f"percentile_{x}" for x in list(range(10, 100, 10))]
        stats.extend(percentiles)

    coords_transform = da.rio.set_spatial_dims(
        x_dim=lon_coord, y_dim=lat_coord
    ).rio.transform()

    stats = zonal_stats(
        vectors=gdf[[geom_col]],
        raster=da.values,
        affine=coords_transform,
        nodata=np.nan,
        all_touched=all_touched,
        stats=stats,
    )
    df_stats = pd.DataFrame(stats).round(2)
    df_stats = gdf[[id_col]].merge(df_stats, left_index=True, right_index=True)
    if date:
        df_stats["date"] = pd.to_datetime(date)
    return df_stats


def upsample_raster(da, resampled_resolution=0.05):
    """
    Upsample a raster to a higher resolution using nearest neighbor resampling.
    The function uses nearest neighbor resampling via the `Resampling.nearest`
    method from `rasterio`.

    Parameters
    ----------
    da : xarray.DataArray
        The raster data array to upsample.
    resampled_resolution : float, optional
        The desired resolution for the upsampled raster. Default is 0.05.

    Returns
    -------
    xarray.DataArray
        The upsampled raster as a DataArray with the new resolution.



    """
    # Assuming square resolution
    input_resolution = da.rio.resolution()[0]
    upscale_factor = int(input_resolution / resampled_resolution)

    new_width = da.rio.width * upscale_factor
    new_height = da.rio.height * upscale_factor

    return da.rio.reproject(
        da.rio.crs,
        shape=(new_height, new_width),
        resampling=Resampling.nearest,
        nodata=np.nan,
    )

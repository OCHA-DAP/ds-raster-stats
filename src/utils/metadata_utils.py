import logging
import tempfile
from pathlib import Path

import coloredlogs
import geopandas as gpd
import numpy as np
import pandas as pd
from rasterio.enums import Resampling
from rasterstats import zonal_stats

from src.config.settings import LOG_LEVEL
from src.utils.cog_utils import stack_cogs
from src.utils.database_utils import create_polygon_table, postgres_upsert
from src.utils.iso3_utils import get_iso3_data, load_shp_from_azure

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def get_available_datasets():
    """
    Get list of available datasets from config directory.

    Returns
    -------
    List[str]
        List of dataset names (based on config file names)
    """
    config_dir = Path("src") / "config"
    return [f.stem for f in config_dir.glob("*.yml")]


def select_name_column(df, adm_level):
    """
    Select the appropriate name column from the administrative boundary GeoDataFrame.

    Parameters
    ----------
    df : geopandas.GeoDataFrame
        The GeoDataFrame containing administrative boundary data.
    adm_level : int
        The administrative level to find the name column for.

    Returns
    -------
    str
        The name of the selected column.
    """
    pattern = f"^ADM{adm_level}_[A-Z]{{2}}$"
    adm_columns = df.filter(regex=pattern).columns
    return adm_columns[0]


def calculate_polygon_stats(
    gdf, dataset_name, dataset_data, dataset_transform, upscale_factor
):
    """
    Calculate polygon statistics for a specific dataset.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        The GeoDataFrame containing polygon geometries.
    dataset_name : str
        The name of the dataset.
    dataset_data : numpy.ndarray
        The raster data array for the dataset.
    dataset_transform : affine.Affine
        The affine transform for the raster data.
    upscale_factor : float
        The factor by which the raster has been upsampled.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing polygon statistics for the dataset.
    """
    stats = zonal_stats(
        vectors=gdf[["geometry"]],
        raster=dataset_data,
        affine=dataset_transform,
        nodata=np.nan,
        all_touched=True,
        stats=["unique", "count"],
    )

    df_out = pd.DataFrame.from_dict(stats)
    df_out[f"{dataset_name}_frac_raw_pixels"] = df_out["count"] / (upscale_factor**2)
    df_out = df_out.rename(
        columns={
            "unique": f"{dataset_name}_n_intersect_raw_pixels",
            "count": f"{dataset_name}_n_upsampled_pixels",
        }
    )
    return df_out


def prepare_dataset_raster(dataset, start_date, end_date, upsampled_resolution):
    """
    Prepare raster data for a specific dataset.

    Parameters
    ----------
    dataset : str
        The name of the dataset to prepare.
    start_date : str
        Start date for the raster data.
    end_date : str
        End date for the raster data.
    upsampled_resolution : float, optional
        The desired output resolution after upsampling, by default 0.05.

    Returns
    -------
    dict
        Dictionary containing the prepared dataset information including data,
        upscale factor, and transform.
    """
    ds = stack_cogs(start_date, end_date, dataset)
    ds.values = np.arange(ds.size).reshape(ds.shape)
    ds = ds.astype(np.float32)

    if "leadtime" in ds.dims:
        ds = ds.sel(leadtime=1)

    input_resolution = ds.rio.resolution()[0]
    upscale_factor = input_resolution / upsampled_resolution
    new_width = int(ds.rio.width * upscale_factor)
    new_height = int(ds.rio.height * upscale_factor)

    ds_resampled = ds.rio.reproject(
        ds.rio.crs,
        shape=(new_height, new_width),
        resampling=Resampling.nearest,
        nodata=np.nan,
    )

    coords_transform = ds_resampled.rio.set_spatial_dims(
        x_dim="x", y_dim="y"
    ).rio.transform()

    return {
        "data": ds_resampled.values[0],
        "upscale_factor": upscale_factor,
        "transform": coords_transform,
    }


def process_polygon_metadata(engine, mode, upsampled_resolution, sel_iso3s=None):
    """
    Process and store polygon metadata for all administrative levels and datasets.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object used to connect to the database.
    mode : str
        The mode to run in ('dev', 'prod', etc.).
    upsampled_resolution : float, optional
        The desired output resolution for raster data.
    sel_iso3s : list of str, optional
        List of ISO3 codes to process. If None, processes all available.

    Returns
    -------
    None
    """
    datasets = get_available_datasets()
    create_polygon_table(engine, datasets)

    dataset_info = {}
    for dataset in datasets:
        dataset_info[dataset] = prepare_dataset_raster(
            dataset, "2024-01-01", "2024-01-01", upsampled_resolution
        )

    df_iso3s = get_iso3_data(sel_iso3s, engine)

    with tempfile.TemporaryDirectory() as td:
        for _, row in df_iso3s.iterrows():
            iso3 = row["iso3"]
            logger.info(f"Processing polygon metadata for {iso3}...")
            max_adm = row["max_adm_level"]

            load_shp_from_azure(iso3, td, mode)

            for i in range(0, max_adm + 1):
                try:
                    gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{i}.shp")

                    for dataset in datasets:
                        df_stats = calculate_polygon_stats(
                            gdf,
                            dataset,
                            dataset_info[dataset]["data"],
                            dataset_info[dataset]["transform"],
                            dataset_info[dataset]["upscale_factor"],
                        )
                        gdf = gdf.join(df_stats)

                    # Calculate area in square kilometers
                    gdf = gdf.to_crs("ESRI:54009")
                    gdf["area"] = gdf.geometry.area / 1_000_000

                    name_column = select_name_column(gdf, i)
                    extract_cols = [f"ADM{i}_PCODE", name_column, "area"]
                    dataset_cols = gdf.columns[
                        gdf.columns.str.contains(
                            "_n_intersect_raw_pixels|"
                            "_frac_raw_pixels|"
                            "_n_upsampled_pixels"
                        )
                    ]

                    gdf = gdf[extract_cols + dataset_cols.tolist()]
                    gdf = gdf.rename(
                        columns={f"ADM{i}_PCODE": "pcode", name_column: "name"}
                    )
                    gdf["adm_level"] = i
                    gdf["name_language"] = name_column[-2:]
                    gdf["iso3"] = iso3

                    print(gdf.head(5))
                    gdf.to_csv("test.csv")

                    gdf.to_sql(
                        "polygon",
                        con=engine,
                        if_exists="append",
                        index=False,
                        method=postgres_upsert,
                    )
                except Exception as e:
                    logger.error(f"Error processing {iso3} at ADM{i}: {str(e)}")
                    continue

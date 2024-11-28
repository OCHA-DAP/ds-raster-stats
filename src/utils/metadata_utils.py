import logging
import tempfile
from pathlib import Path

import coloredlogs
import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr
from rioxarray.exceptions import NoDataInBounds

from src.config.settings import LOG_LEVEL, load_pipeline_config
from src.utils.cloud_utils import get_container_client
from src.utils.cog_utils import get_cog_url
from src.utils.database_utils import create_polygon_table, postgres_upsert
from src.utils.iso3_utils import get_iso3_data, load_shp_from_azure
from src.utils.raster_utils import (
    fast_zonal_stats,
    prep_raster,
    rasterize_admin,
)

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


def get_single_cog(dataset, mode):
    container_client = get_container_client(mode, "raster")
    config = load_pipeline_config(dataset)
    prefix = config["blob_prefix"]
    cogs_list = [
        x.name for x in container_client.list_blobs(name_starts_with=prefix)
    ]
    cog_url = get_cog_url(mode, cogs_list[0])
    return rxr.open_rasterio(cog_url, chunks="auto")


def process_polygon_metadata(
    engine, mode, upsampled_resolution, sel_iso3s=None
):
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
    df_iso3s = get_iso3_data(None, engine)

    with tempfile.TemporaryDirectory() as td:
        for _, row in df_iso3s.iterrows():
            iso3 = row["iso3"]
            logger.info(f"Processing polygon metadata for {iso3}...")
            max_adm = row["max_adm_level"]
            load_shp_from_azure(iso3, td, mode)
            try:
                for i in range(0, max_adm + 1):
                    gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{i}.shp")
                    for dataset in datasets:
                        da = get_single_cog(dataset, mode)
                        input_resolution = da.rio.resolution()
                        gdf_adm0 = gpd.read_file(
                            f"{td}/{iso3.lower()}_adm0.shp"
                        )
                        # We want all values to be unique, so that we can count the total
                        # number of unique cells from the raw source that contribute to the stats
                        da.values = np.arange(da.size).reshape(da.shape)
                        da = da.astype(np.float32)
                        # Dummy `date` dimension to pass `validate_dims`
                        da = da.expand_dims({"date": 1})

                        try:
                            da_clipped = prep_raster(da, gdf_adm0)
                        except NoDataInBounds:
                            logger.error(
                                f"{dataset} has no coverage at adm level {i}"
                            )
                            continue

                        da_clipped = prep_raster(da, gdf_adm0, logger=logger)
                        output_resolution = da_clipped.rio.resolution()
                        upscale_factor = (
                            input_resolution[0] / output_resolution[0]
                        )

                        src_transform = da_clipped.rio.transform()
                        src_width = da_clipped.rio.width
                        src_height = da_clipped.rio.height

                        admin_raster = rasterize_admin(
                            gdf,
                            src_width,
                            src_height,
                            src_transform,
                            all_touched=False,
                        )
                        adm_ids = gdf[f"ADM{i}_PCODE"]
                        n_adms = len(adm_ids)

                        results = fast_zonal_stats(
                            da_clipped.values[0][0],
                            admin_raster,
                            n_adms,
                            stats=["count", "unique"],
                            rast_fill=np.nan,
                        )
                        df_results = pd.DataFrame.from_dict(results)
                        df_results[f"{dataset}_frac_raw_pixels"] = df_results[
                            "count"
                        ] / (upscale_factor**2)
                        df_results = df_results.rename(
                            columns={
                                "unique": f"{dataset}_n_intersect_raw_pixels",
                                "count": f"{dataset}_n_upsampled_pixels",
                            }
                        )
                        gdf = gdf.join(df_results)

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

                    df = gdf[extract_cols + dataset_cols.tolist()]
                    df = df.rename(
                        columns={f"ADM{i}_PCODE": "pcode", name_column: "name"}
                    )
                    df["adm_level"] = i
                    df["name_language"] = name_column[-2:]
                    df["iso3"] = iso3
                    df["standard"] = True

                    df.to_sql(
                        "polygon",
                        con=engine,
                        if_exists="append",
                        index=False,
                        method=postgres_upsert,
                    )
            except Exception as e:
                logger.error(f"Error: {e}")

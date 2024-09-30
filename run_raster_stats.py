import logging
import tempfile
import traceback

import coloredlogs
import geopandas as gpd

from src.config.settings import LOG_LEVEL, load_pipeline_config, parse_pipeline_config
from src.utils.cog_utils import stack_cogs
from src.utils.database_utils import (
    create_dataset_table,
    create_qa_table,
    db_engine,
    insert_qa_table,
)
from src.utils.inputs import cli_args
from src.utils.iso3_utils import create_iso3_df, get_iso3_data, load_shp
from src.utils.raster_utils import fast_zonal_stats_runner, prep_raster

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


if __name__ == "__main__":
    args = cli_args()
    dataset = args.dataset
    logger.info(f"Updating data for {dataset}...")

    engine = db_engine(args.mode)
    create_qa_table(engine)
    settings = load_pipeline_config(dataset)
    start, end, is_forecast = parse_pipeline_config(settings, args.test)
    create_dataset_table(dataset, engine, is_forecast)
    if args.build_iso3:
        logger.info("Creating ISO3 table in Postgres database...")
        create_iso3_df(engine)

    sel_iso3s = settings["test"]["iso3s"] if args.test else None
    df_iso3s = get_iso3_data(sel_iso3s, engine)

    logger.info(f"Creating stack of COGs from {start} to {end}...")
    ds = stack_cogs(start, end, dataset, args.mode)

    # Loop through each country
    logger.info(f"Calculating raster stats for {len(df_iso3s)} ISO3s...")
    for idx, row in df_iso3s.iterrows():
        iso3 = df_iso3s.loc[idx, "iso_3"]
        shp_url = df_iso3s.loc[idx, "o_shp"]
        max_adm = df_iso3s.loc[idx, "max_adm_level"]
        logger.info(f"Processing data for {iso3}...")

        with tempfile.TemporaryDirectory() as td:
            load_shp(shp_url, td, iso3)
            gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm0.shp")
            try:
                ds_clipped = prep_raster(ds, gdf)
            except Exception as e:
                logger.error(f"Error preparing raster for {iso3}: {e}")
                stack_trace = traceback.format_exc()
                insert_qa_table(iso3, None, dataset, e, stack_trace, engine)
                continue

            # Loop through each adm
            for adm_level in list(range(0, max_adm + 1)):
                try:
                    gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{adm_level}.shp")
                    logger.info(f"Computing stats for adm{adm_level}...")
                    fast_zonal_stats_runner(
                        ds_clipped,
                        gdf,
                        adm_level,
                        iso3,
                        save_to_database=False,
                        engine=engine,
                        dataset=dataset,
                    )
                except Exception as e:
                    logger.error(
                        f"Error calculating stats for {iso3} at {adm_level}: {e}"
                    )
                    stack_trace = traceback.format_exc()
                    insert_qa_table(iso3, adm_level, dataset, e, stack_trace, engine)
                    continue

    logger.info("Done calculating and saving stats.")

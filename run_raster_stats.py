import logging
import sys
import tempfile
import traceback
from multiprocessing import Pool, current_process

import coloredlogs
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from src.config.settings import LOG_LEVEL, UPSAMPLED_RESOLUTION, parse_pipeline_config
from src.utils.cog_utils import stack_cogs
from src.utils.database_utils import (
    create_dataset_table,
    create_qa_table,
    db_engine_url,
    insert_qa_table,
    postgres_upsert,
)
from src.utils.general_utils import split_date_range
from src.utils.inputs import cli_args
from src.utils.iso3_utils import create_iso3_df, get_iso3_data, load_shp_from_azure
from src.utils.metadata_utils import process_polygon_metadata
from src.utils.raster_utils import fast_zonal_stats_runner, prep_raster

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def setup_logger(name, level=logging.INFO):
    """Function to setup a logger that prints to console"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    coloredlogs.install(level=level, logger=logger)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def process_chunk(start, end, dataset, mode, df_iso3s, engine_url):
    process_name = current_process().name
    logger = setup_logger(f"{process_name}: {dataset}_{start}")
    logger.info(f"Starting processing for {dataset} from {start} to {end}")

    engine = create_engine(engine_url)
    ds = stack_cogs(start, end, dataset, mode)

    try:
        for _, row in df_iso3s.iterrows():
            iso3 = row["iso3"]
            max_adm = row["max_adm_level"]
            logger.info(f"Processing data for {iso3}...")

            with tempfile.TemporaryDirectory() as td:
                load_shp_from_azure(iso3, td, mode)
                gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm0.shp")
                try:
                    ds_clipped = prep_raster(ds, gdf, logger=logger)
                except Exception as e:
                    logger.error(f"Error preparing raster for {iso3}: {e}")
                    stack_trace = traceback.format_exc()
                    insert_qa_table(iso3, None, dataset, e, stack_trace, engine)
                    continue

                try:
                    all_results = []
                    for adm_level in range(max_adm + 1):
                        gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{adm_level}.shp")
                        logger.debug(f"Computing stats for adm{adm_level}...")
                        df_results = fast_zonal_stats_runner(
                            ds_clipped,
                            gdf,
                            adm_level,
                            iso3,
                            save_to_database=False,
                            engine=None,
                            dataset=dataset,
                            logger=logger,
                        )
                        if df_results is not None:
                            all_results.append(df_results)
                    df_all_results = pd.concat(all_results, ignore_index=True)
                    logger.debug(f"Writing {len(df_all_results)} rows to database...")
                    df_all_results.to_sql(
                        f"{dataset}",
                        con=engine,
                        if_exists="append",
                        index=False,
                        method=postgres_upsert,
                    )
                except Exception as e:
                    logger.error(f"Error calculating stats for {iso3}: {e}")
                    stack_trace = traceback.format_exc()
                    insert_qa_table(iso3, adm_level, dataset, e, stack_trace, engine)
                    continue
            # Clear memory
            del ds_clipped

    finally:
        engine.dispose()


if __name__ == "__main__":
    args = cli_args()

    engine_url = db_engine_url(args.mode)
    engine = create_engine(engine_url)

    if args.update_metadata:
        logger.info("Updating metadata in Postgres database...")
        create_iso3_df(engine)
        process_polygon_metadata(
            engine,
            args.mode,
            upsampled_resolution=UPSAMPLED_RESOLUTION,
            sel_iso3s=None,
        )
        sys.exit(0)

    dataset = args.dataset
    logger.info(f"Updating data for {dataset}...")

    create_qa_table(engine)
    start, end, is_forecast, sel_iso3s, extra_dims = parse_pipeline_config(
        dataset, args.test, args.update_stats, args.mode
    )
    create_dataset_table(dataset, engine, is_forecast, extra_dims)

    df_iso3s = get_iso3_data(sel_iso3s, engine)
    date_ranges = split_date_range(start, end)

    if len(date_ranges) > 1:
        num_processes = 2
        logger.info(
            f"Processing {len(date_ranges)} chunks with {num_processes} processes"
        )

        process_args = [
            (start, end, dataset, args.mode, df_iso3s, engine_url)
            for start, end in date_ranges
        ]

        with Pool(num_processes) as pool:
            pool.starmap(process_chunk, process_args)

    else:
        logger.info("Processing entire date range in a single chunk")
        process_chunk(start, end, dataset, args.mode, df_iso3s, engine_url)

    logger.info("Done calculating and saving stats.")

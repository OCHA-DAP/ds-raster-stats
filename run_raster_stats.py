import logging
import sys
import traceback
from multiprocessing import Pool, current_process

import coloredlogs
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from src.config.settings import (
    LOG_LEVEL,
    NUM_PROCESSES,
    UPSAMPLED_RESOLUTION,
    config_pipeline,
)
from src.utils.cog_utils import stack_cogs
from src.utils.database_utils import (
    create_dataset_table,
    create_qa_table,
    db_engine_url,
    insert_qa_table,
    postgres_upsert,
)
from src.utils.inputs import cli_args
from src.utils.iso3_utils import create_iso3_df, get_iso3_data, load_shp_cached
from src.utils.metadata_utils import process_polygon_metadata
from src.utils.zonal_utils import (
    clip_raster,
    validate_stats_df,
    zonal_stats_runner,
)

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


def process_chunk(
    dates, dataset, mode, df_iso3s, engine_url, chunksize, forecast
):
    process_name = current_process().name
    logger = setup_logger(f"{process_name}: {dataset}_{dates[0]}")
    logger.info(
        f"""
        Starting processing for {len(dates)} dates for {dataset}
        between {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}
        """
    )

    engine = create_engine(engine_url)
    ds = stack_cogs(dates, dataset, mode)

    try:
        for _, row in df_iso3s.iterrows():
            iso3 = row["iso3"]
            max_adm = row["max_adm_level"]

            # Coverage check for specific datasets
            if dataset in df_iso3s.keys():
                if not row[dataset]:
                    logger.info(f"Skipping {iso3}...")
                    continue
            logger.info(f"Processing data for {iso3}...")

            shp_dir = load_shp_cached(iso3, mode)
            gdf = gpd.read_file(f"{shp_dir}/{iso3.lower()}_adm0.shp")
            try:
                ds_clipped = clip_raster(ds, gdf, logger=logger)
            except Exception as e:
                logger.error(f"Error preparing raster for {iso3}: {e}")
                stack_trace = traceback.format_exc()
                insert_qa_table(iso3, None, dataset, e, stack_trace, engine)
                continue

            try:
                all_results = []
                for adm_level in range(max_adm + 1):
                    gdf = gpd.read_file(
                        f"{shp_dir}/{iso3.lower()}_adm{adm_level}.shp"
                    )
                    logger.debug(f"Computing stats for adm{adm_level}...")
                    df_results = zonal_stats_runner(
                        ds_clipped,
                        gdf,
                        adm_level,
                        iso3,
                        logger=logger,
                    )
                    if df_results is not None:
                        all_results.append(df_results)
                if not all_results:
                    continue
                df_all_results = pd.concat(all_results, ignore_index=True)
                validate_stats_df(df_all_results, forecast)
                logger.debug(
                    f"Writing {len(df_all_results)} rows to database..."
                )
                df_all_results.to_sql(
                    f"{dataset}",
                    con=engine,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    method=postgres_upsert,
                )
            except Exception as e:
                logger.error(f"Error calculating stats for {iso3}: {e}")
                stack_trace = traceback.format_exc()
                insert_qa_table(
                    iso3, adm_level, dataset, e, stack_trace, engine
                )
                continue
            # Clear memory
            del ds_clipped

    finally:
        engine.dispose()


def build_tasks(date_chunks, df_iso3s, num_processes):
    """
    Build (dates, df_iso3s) work items for the process pool.

    Date chunks are the primary unit of parallelism. When there are
    fewer chunks than workers (e.g. the daily/monthly update runs,
    which have a single date), countries are also split across workers
    so the run still parallelizes.
    """
    if not date_chunks or not len(df_iso3s):
        return []
    n_groups = min(
        -(-num_processes // len(date_chunks)),  # ceil division
        len(df_iso3s),
    )
    iso3_groups = [
        df_iso3s.iloc[i::n_groups].reset_index(drop=True)
        for i in range(n_groups)
    ]
    tasks = []
    for i, dates in enumerate(date_chunks):
        for group in iso3_groups:
            # Rotate the country order per chunk so concurrent workers
            # start on different countries -- this spreads out boundary
            # downloads and weight-matrix builds instead of having
            # every worker compute the same country at the same time
            offset = (i * max(1, len(group) // num_processes)) % max(
                1, len(group)
            )
            rotated = pd.concat(
                [group.iloc[offset:], group.iloc[:offset]]
            ).reset_index(drop=True)
            tasks.append((dates, rotated))
    return tasks


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
    logger.info("Determining pipeline configuration...")

    create_qa_table(engine)
    config = config_pipeline(
        dataset,
        args.test,
        args.update_stats,
        args.mode,
        args.backfill,
        engine,
    )
    create_dataset_table(
        dataset, engine, config["forecast"], config["extra_dims"]
    )
    df_iso3s = get_iso3_data(config["sel_iso3s"], engine)
    date_chunks = config["date_chunks"]

    num_processes = args.num_processes or NUM_PROCESSES
    tasks = build_tasks(date_chunks, df_iso3s, num_processes)
    logger.info(
        f"Processing {len(date_chunks)} date chunks "
        f"({len(tasks)} tasks) with {num_processes} processes"
    )

    process_args = [
        (
            dates,
            dataset,
            args.mode,
            df_iso3s_group,
            engine_url,
            args.chunksize,
            config["forecast"],
        )
        for dates, df_iso3s_group in tasks
    ]

    with Pool(num_processes) as pool:
        pool.starmap(process_chunk, process_args)

    logger.info("Done calculating and saving stats.")

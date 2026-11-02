import gc
import logging
import os
import sys
import tempfile
import traceback
from collections import deque
from multiprocessing import Pool, current_process

import coloredlogs
import dask
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from src.config.settings import (
    LOG_LEVEL,
    UPSAMPLED_RESOLUTION,
    config_pipeline,
)
from src.utils.cog_utils import (
    delete_downloads,
    download_cogs,
    get_cogs_list,
    stack_cogs,
)
from src.utils.database_utils import (
    create_dataset_table,
    create_qa_table,
    db_engine_url,
    insert_qa_table,
    postgres_upsert,
)
from src.utils.inputs import cli_args
from src.utils.iso3_utils import (
    create_iso3_df,
    get_iso3_data,
    load_shp_from_azure,
)
from src.utils.metadata_utils import process_polygon_metadata
from src.utils.raster_utils import (
    fast_zonal_stats_runner,
    prep_raster,
    validate_stats,
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


def process_chunk(cogs, dataset, mode, df_iso3s, engine_url, chunksize, td):
    process_name = current_process().name
    logger = setup_logger(f"{process_name}: {dataset}")
    engine = create_engine(engine_url, poolclass=NullPool)
    dask.config.set(scheduler="synchronous")

    logger.info(f"Stacking {len(cogs)} cogs...")
    with dask.config.set(scheduler="synchronous"):
        ds = stack_cogs(cogs, dataset, mode).load()

    try:
        for _, row in df_iso3s.iterrows():
            iso3 = row["iso3"]
            logger.info(f"Processing data for {iso3}...")
            max_adm = row["max_adm_level"]

            # Coverage check for specific datasets
            if dataset in df_iso3s.keys():
                if not row[dataset]:
                    logger.info(f"Skipping {iso3}...")
                    continue
            try:
                gdf = gpd.read_parquet(f"{td}/{iso3.lower()}_adm0.parquet")

                if dataset != "chirps":
                    logger.info(f"Clipping and upsampling for {iso3}...")
                    ds_clipped = prep_raster(ds, gdf, logger=logger)
                else:
                    logger.info(f"Clipping raster for {iso3}...")
                    ds_clipped = prep_raster(
                        ds, gdf, logger=logger, upsample=False
                    )
            except Exception as e:
                logger.error(f"Error preparing raster for {iso3}: {e}")
                stack_trace = traceback.format_exc()
                insert_qa_table(iso3, None, dataset, e, stack_trace, engine)
                engine.dispose()
                # Nothing to compute stats on, so move to the next iso3
                continue
            try:
                all_results = []
                for adm_level in range(max_adm + 1):
                    gdf = gpd.read_parquet(
                        f"{td}/{iso3.lower()}_adm{adm_level}.parquet"
                    )
                    logger.info(f"Computing stats for adm{adm_level}...")
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
                if dataset == "chirps":
                    n_before = len(df_all_results)
                    df_all_results = df_all_results[
                        df_all_results["mean"] != 0
                    ]
                    logger.info(
                        f"Dropped {n_before - len(df_all_results)} zero-precipitation row(s) for {iso3}."
                    )
                validate_stats(iso3, df_all_results)
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

            # Clear memory
            del ds_clipped
            gc.collect()
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
        dataset,
        engine,
        config["forecast"],
        config["extra_dims"],
        config["frequency"],
    )
    df_iso3s = get_iso3_data(config["sel_iso3s"], engine)
    date_chunks = config["date_chunks"]

    num_processes = args.num_processes or os.cpu_count() or 2
    logger.info(f"Using {num_processes} processes")

    with tempfile.TemporaryDirectory() as tmp_dir:
        for iso3 in df_iso3s["iso3"].tolist():
            # Check if it exists already in shapes directory
            if os.path.exists(
                os.path.join(config["shapes_dir"], f"{iso3.lower()}_adm0.shp")
            ):
                logger.debug(f"Skipping shp download for: {iso3}...")
            else:
                logger.debug(f"Loading shp data for iso: {iso3}...")
                load_shp_from_azure(iso3, args.mode, config["shapes_dir"])

        # Only ship the columns process_chunk reads to each worker
        iso3_cols = ["iso3", "max_adm_level"]
        if dataset in df_iso3s.columns:
            iso3_cols.append(dataset)
        df_iso3s_chunk = df_iso3s[iso3_cols]

        cogs_per_chunk = config["cog_chunksize"]
        downloads_dir = config["downloads_dir"]

        # Downloads are only deleted on Databricks. Locally they're kept, so
        # later runs reuse them instead of downloading them again
        on_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        cleanup = args.pre_download and on_databricks
        if args.pre_download:
            logger.info(
                f"Downloading cogs to {downloads_dir} "
                f"({'deleted' if cleanup else 'kept'} once processed)."
            )
        else:
            logger.info("Reading cogs straight from the source.")

        # A date chunk's downloads, deleted once its last task finishes
        chunk_downloads = {}

        def task_done(chunk_id):
            downloads = chunk_downloads[chunk_id]
            downloads["tasks_left"] -= 1
            if downloads["tasks_left"] == 0:
                del chunk_downloads[chunk_id]
                if cleanup:
                    delete_downloads(downloads["paths"], downloads_dir)

        # Each date chunk's cogs are split into tasks of cog_chunksize cogs.
        # Up to num_processes tasks run at once while the next chunk is
        # downloaded if needed
        pool = Pool(num_processes)
        pending = deque()
        with pool:
            for i, date_chunk in enumerate(date_chunks, start=1):
                logger.info(
                    f"Date chunk {i}/{len(date_chunks)}: "
                    f"{pd.Timestamp(date_chunk[0]):%Y-%m-%d} to "
                    f"{pd.Timestamp(date_chunk[-1]):%Y-%m-%d}"
                )

                logger.info("Retrieving list of cogs...")
                cogs_list = list(get_cogs_list(dataset, date_chunk, args.mode))
                logger.info("Done retrieving list of cogs.")

                if args.pre_download:
                    logger.info(f"Downloading {len(cogs_list)} cogs...")
                    cogs_list = download_cogs(
                        dataset, cogs_list, downloads_dir
                    )
                    logger.info("Done downloading cogs.")

                cogs_chunks = [
                    cogs_list[j : j + cogs_per_chunk]
                    for j in range(0, len(cogs_list), cogs_per_chunk)
                ]
                logger.info(
                    f"Split {len(cogs_list)} cogs into {len(cogs_chunks)} "
                    f"tasks of up to {cogs_per_chunk} cogs per date chunk"
                )

                process_args = [
                    (
                        cogs_chunk,
                        dataset,
                        args.mode,
                        df_iso3s_chunk,
                        engine_url,
                        args.chunksize,
                        config["shapes_dir"],
                    )
                    for cogs_chunk in cogs_chunks
                ]
                if not process_args:
                    continue

                chunk_downloads[i] = {
                    "paths": cogs_list if args.pre_download else [],
                    "tasks_left": len(process_args),
                }

                for task_args in process_args:
                    # Cap the tasks in flight, so memory stays at
                    # num_processes stacks and downloads don't run far ahead
                    if len(pending) >= num_processes:
                        result, chunk_id = pending.popleft()
                        result.get()
                        task_done(chunk_id)
                    pending.append(
                        (pool.apply_async(process_chunk, task_args), i)
                    )

            # get() re-raises any error from a task
            while pending:
                result, chunk_id = pending.popleft()
                result.get()
                task_done(chunk_id)

    logger.info("Done calculating and saving stats.")

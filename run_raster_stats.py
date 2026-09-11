import gc
import logging
import os
import sys
import tempfile
import traceback
from multiprocessing import Pool, current_process

import coloredlogs
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from src.config.settings import (
    LOG_LEVEL,
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
from src.utils.iso3_utils import (
    create_iso3_df,
    get_iso3_data,
    load_shp_from_azure,
)
from src.utils.metadata_utils import process_polygon_metadata
from src.utils.raster_utils import fast_zonal_stats_runner, upsample_raster

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
    dates, dataset, mode, df_iso3s, engine_url, chunksize, iso3, td
):
    try:
        from pyspark import TaskContext

        tc = TaskContext.get()
    except ImportError:
        tc = None
    if tc:
        task_label = f"partition-{tc.partitionId()}"
    else:
        task_label = current_process().name
    logger = setup_logger(f"{task_label}: {dataset}_{dates[0]}")

    engine = create_engine(engine_url)

    logger.info(f"Processing data for {iso3}...")
    gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm0.shp")

    ds = stack_cogs(dates, dataset, mode, gdf)

    df_row = df_iso3s[df_iso3s["iso3"] == iso3].iloc[0]
    max_adm = df_row["max_adm_level"]

    # Coverage check for specific datasets
    if dataset in df_iso3s.keys():
        if iso3 not in df_row[dataset]:
            logger.info(f"Skipping {iso3}...")
            return

    try:
        if dataset != "chirps":
            logger.info(f"Clipping and upsampling {iso3}...")
            ds_clipped = upsample_raster(ds, logger=logger)
        else:
            ds_clipped = ds
    except Exception as e:
        logger.error(f"Error preparing raster for {iso3}: {e}")
        stack_trace = traceback.format_exc()
        insert_qa_table(iso3, None, dataset, e, stack_trace, engine)
        engine.dispose()
        return
    try:
        all_results = []
        for adm_level in range(max_adm + 1):
            gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{adm_level}.shp")
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
            df_all_results = df_all_results[df_all_results["mean"] != 0]
            logger.info(
                f"Dropped {n_before - len(df_all_results)} "
                "zero-precipitation row(s) for chirps..."
            )
        logger.debug(f"Writing {len(df_all_results)} rows to database...")
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
        insert_qa_table(iso3, adm_level, dataset, e, stack_trace, engine)

    # Clear memory
    del ds_clipped
    del ds
    gc.collect()
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
        dataset, engine, config["forecast"], config["extra_dims"]
    )
    df_iso3s = get_iso3_data(config["sel_iso3s"], engine)
    date_chunks = config["date_chunks"]

    spark = None
    if args.no_spark:
        num_processes = args.num_processes or os.cpu_count() or 2
        logger.info(
            f"Processing {len(date_chunks)} date chunks with {num_processes} "
            "processes (Spark disabled)"
        )
    else:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder.appName("raster-stats")
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            builder = builder.master("local[*]")
        spark = builder.getOrCreate()

        num_processes = (
            args.num_processes or spark.sparkContext.defaultParallelism
        )
        logger.info(
            f"Processing {len(date_chunks)} date chunks with {num_processes} "
            "Spark task slots"
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        for iso3 in df_iso3s["iso3"].tolist():
            # Check if it exists already in shapes directory
            if os.path.exists(
                os.path.join(config["shapes_dir"], f"{iso3.lower()}_adm0.shp")
            ):
                logger.debug(f"Skipping shp download for: {iso3}...")
            else:
                logger.debug(f"Loading shp data for iso: {iso3}...")
                load_shp_from_azure(iso3, config["shapes_dir"], args.mode)

        process_args = [
            (
                dates,
                dataset,
                args.mode,
                df_iso3s,
                engine_url,
                args.chunksize,
                iso3,
                config["shapes_dir"],
            )
            for dates in date_chunks
            for iso3 in df_iso3s["iso3"].tolist()
        ]

        if process_args:
            if spark is None:
                with Pool(num_processes) as pool:
                    pool.starmap(process_chunk, process_args)
            else:
                rdd = spark.sparkContext.parallelize(
                    process_args, numSlices=len(process_args)
                )
                rdd.foreach(lambda t: process_chunk(*t))

    logger.info("Done calculating and saving stats.")

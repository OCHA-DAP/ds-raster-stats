import logging
import tempfile
import time
import traceback

import coloredlogs
import geopandas as gpd

from config import DATASETS, LOG_LEVEL, MAX_ADM
from src.cod_utils import get_metadata, load_shp
from src.cog_utils import stack_cogs
from src.database_utils import (
    create_dataset_table,
    create_qa_table,
    db_engine,
    insert_qa_table,
    postgres_upsert,
)
from src.inputs import cli_args
from src.raster_utils import compute_zonal_statistics, prep_raster

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def unpack_dataset_params(dataset, test):
    df_iso3s = get_metadata()
    if test:
        logger.info("Running pipeline in TEST mode. Processing a subset of all data.")
        start = DATASETS[dataset]["dev_run"]["start_date"]
        end = DATASETS[dataset]["dev_run"]["end_date"]
        iso3s = DATASETS[dataset]["dev_run"]["iso3s"]
        df_iso3s = df_iso3s[df_iso3s.iso_3.isin(iso3s)]
    else:
        start = DATASETS[dataset]["start_date"]
        end = DATASETS[dataset]["end_date"]
    return start, end, df_iso3s


if __name__ == "__main__":
    args = cli_args()
    datasets = [args.dataset] if args.dataset else list(DATASETS.keys())

    # Set up database
    engine = db_engine(args.mode)
    create_qa_table(engine)

    # Loop through each dataset
    for dataset in datasets:
        logger.info(f"Updating data for {dataset}...")
        full_start_time = time.time()
        start, end, df_iso3s = unpack_dataset_params(dataset, args.test)
        create_dataset_table(dataset, engine)

        # Get all COGs for the dataset and time period of interest
        logger.debug(f"Creating stack of COGs from {start} to {end}...")
        start_time = time.time()
        ds = stack_cogs(start, end, dataset, args.mode)
        elapsed_time = time.time() - start_time
        logger.debug(f"Finished processing COGs in {elapsed_time:.4f} seconds.")

        # Loop through each country
        logger.info(f"Calculating raster stats for {len(df_iso3s)} ISO3s...")
        for idx, row in df_iso3s.iterrows():
            iso3 = df_iso3s.loc[idx, "iso_3"]
            shp_url = df_iso3s.loc[idx, "o_shp"]
            src_max_adm = df_iso3s.loc[idx, "src_lvl"]
            max_adm = min(MAX_ADM, src_max_adm)
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

                # Loop through each admin level in each country
                for adm_level in list(range(0, max_adm + 1)):
                    try:
                        start_time = time.time()
                        gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{adm_level}.shp")
                        gdf["geometry"] = gdf["geometry"].simplify(
                            tolerance=0.001, preserve_topology=True
                        )
                        df_all_stats = compute_zonal_statistics(
                            ds_clipped, gdf, f"ADM{adm_level}_PCODE", adm_level, iso3
                        )

                        elapsed_time = time.time() - start_time
                        logger.debug(
                            f"- {elapsed_time:.4f}s: Raster stats calculated for admin{adm_level}."
                        )
                        start_time = time.time()
                        df_all_stats.to_sql(
                            dataset,
                            con=engine,
                            if_exists="append",
                            index=False,
                            method=postgres_upsert,
                        )
                        elapsed_time = time.time() - start_time
                        logger.debug(
                            f"- {elapsed_time:.4f}s: Wrote out {len(df_all_stats)} rows to db."
                        )
                    except Exception as e:
                        logger.error(
                            f"Error calculating stats for {iso3} at {adm_level}: {e}"
                        )
                        stack_trace = traceback.format_exc()
                        insert_qa_table(
                            iso3, adm_level, dataset, e, stack_trace, engine
                        )

        elapsed_time = time.time() - full_start_time
        logger.info(f"- {elapsed_time:.4f}s: Done calculations.")

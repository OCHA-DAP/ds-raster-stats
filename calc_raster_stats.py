import logging
import tempfile
import time
from pathlib import Path

import coloredlogs
import geopandas as gpd

from config import DATASETS, LOG_LEVEL, MAX_ADM
from src.cod_utils import get_metadata, load_shp
from src.cog_utils import stack_cogs
from src.database_utils import (
    create_dataset_table,
    create_error_table,
    db_engine,
    postgres_upsert,
    write_error_table,
)
from src.inputs import cli_args
from src.raster_utils import compute_zonal_statistics, prep_raster

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


if __name__ == "__main__":
    args = cli_args()
    df_iso3s = get_metadata()
    output_dir = Path("test_outputs") / "tabular"
    datasets = [args.dataset] if args.dataset else list(DATASETS.keys())
    engine = db_engine(args.mode)
    create_error_table(engine)

    for dataset in datasets:
        full_start_time = time.time()
        start_time = time.time()
        logger.info(f"Updating data for {dataset}...")
        create_dataset_table(dataset, engine)
        if args.test:
            logger.info(
                "Running pipeline in TEST mode. Processing a subset of all data."
            )
            start = DATASETS[dataset]["dev_run"]["start_date"]
            end = DATASETS[dataset]["dev_run"]["end_date"]
            iso3s = DATASETS[dataset]["dev_run"]["iso3s"]
            # df_iso3s = df_iso3s[df_iso3s.iso_3.isin(iso3s)]
        else:
            start = DATASETS[dataset]["start_date"]
            end = DATASETS[dataset]["end_date"]

        logger.debug(f"Creating stack of COGs from {start} to {end}...")
        ds = stack_cogs(start, end, dataset, args.mode)
        elapsed_time = time.time() - start_time
        logger.debug(f"Finished processing COGs in {elapsed_time:.4f} seconds.")

        logger.info(f"Calculating raster stats for {len(df_iso3s)} ISO3s...")

        # --- Start by looping through each country
        for idx, row in df_iso3s.iterrows():
            iso3 = df_iso3s.loc[idx, "iso_3"]
            shp_url = df_iso3s.loc[idx, "o_shp"]
            src_max_adm = df_iso3s.loc[idx, "src_lvl"]

            logger.info(f"Processing data for {iso3}...")
            if args.mode == "local":
                country_dir = output_dir / iso3
                country_dir.mkdir(exist_ok=True, parents=True)
            else:
                country_dir = iso3

            # Go up to MAX_ADM, unless the source data doesn't have it
            max_adm = min(MAX_ADM, src_max_adm)

            with tempfile.TemporaryDirectory() as td:
                load_shp(shp_url, td, iso3)
                # Prep the raster
                gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm0.shp")
                try:
                    ds_clipped = prep_raster(ds, gdf)
                except Exception as e:
                    logger.error(f"Error preparing raster for {iso3}:")
                    logger.error(e)
                    write_error_table(iso3, None, dataset, e, engine)

                # --- Now for each admin level in each country
                for adm_level in list(range(0, max_adm + 1)):
                    # Don't need it to really be a path if writing to cloud
                    if args.mode == "local":
                        adm_dir = country_dir / f"adm{adm_level}"
                        adm_dir.mkdir(exist_ok=True, parents=True)
                    else:
                        adm_dir = f"{country_dir}/adm{adm_level}"
                    try:
                        start_time = time.time()
                        gdf = gpd.read_file(f"{td}/{iso3.lower()}_adm{adm_level}.shp")
                        df_all_stats = compute_zonal_statistics(
                            ds_clipped, gdf, f"ADM{adm_level}_PCODE", adm_level
                        )
                        df_all_stats["iso3"] = iso3
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
                            f"Error calculating stats for {iso3} at {adm_level}:"
                        )
                        logger.error(e)
                        write_error_table(iso3, adm_level, dataset, e, engine)

        elapsed_time = time.time() - full_start_time
        logger.info(f"- {elapsed_time:.4f}s: Done calculations.")

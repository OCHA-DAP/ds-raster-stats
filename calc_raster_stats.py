import logging
import os
import tempfile
import time
from pathlib import Path

import coloredlogs
import geopandas as gpd
from dotenv import load_dotenv

from config import DATASETS
from src.cloud_utils import write_output_stats
from src.cod_utils import get_metadata, load_shp
from src.cog_utils import stack_cogs
from src.inputs import cli_args
from src.raster_utils import compute_zonal_statistics, upsample_raster

MAX_ADM = 2
LOG_LEVEL = "DEBUG"

load_dotenv()
logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


if __name__ == "__main__":
    args = cli_args()
    df_iso3s = get_metadata()
    output_dir = Path("test_outputs") / "tabular"
    datasets = [args.dataset if args.dataset else list(DATASETS.keys())]

    for dataset in datasets:
        logger.info(f"Updating data for {dataset}...")
        if args.test:
            logger.info(
                "Running pipeline in TEST mode. Processing a subset of all data."
            )
            start = DATASETS[dataset]["dev_run"]["start_date"]
            end = DATASETS[dataset]["dev_run"]["end_date"]
            iso3s = DATASETS[dataset]["dev_run"]["iso3s"]
            df_iso3s = df_iso3s[df_iso3s.iso_3.isin(iso3s)]
        else:
            start = DATASETS[dataset]["start_date"]
            end = DATASETS[dataset]["end_date"]

        logger.debug(f"Creating stack of COGs from {start} to {end}...")
        start_time = time.time()
        ds = stack_cogs(start, end, dataset, args.mode)
        ds_upsampled = upsample_raster(ds)
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
                # --- Now for each admin level in each country
                for adm_level in list(range(0, max_adm + 1)):
                    # Don't need it to really be a path if writing to cloud
                    if args.mode == "local":
                        adm_dir = country_dir / f"adm{adm_level}"
                        adm_dir.mkdir(exist_ok=True, parents=True)
                    else:
                        adm_dir = f"{country_dir}/adm{adm_level}"

                    start_time = time.time()

                    gdf = gpd.read_file(f"{td}/{iso3}_adm{adm_level}.shp")

                    df_all_stats = compute_zonal_statistics(
                        ds_upsampled, gdf, f"ADM{adm_level}_PCODE", adm_level
                    )

                    elapsed_time = time.time() - start_time
                    logger.debug(
                        f"Raster stats calculated for admin{adm_level} in {elapsed_time:.4f} seconds"
                    )

                    output_file = os.path.join(
                        adm_dir, f"{dataset}_raster_stats.parquet"
                    )
                    write_output_stats(df_all_stats, output_file, args.mode)

        logger.info("... Done calculations.")

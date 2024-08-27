import logging
import os
import tempfile
from pathlib import Path

import coloredlogs
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

from src.cloud_utils import stack_cogs, write_output_stats
from src.cod_utils import get_metadata, load_shp
from src.raster_utils import compute_zonal_statistics, upsample_raster

LAST_RUN = "2024-07-05"  # Or can be a date
DATASET = "era5"
START = "2020-01-01"
END = "2022-12-01"
MAX_ADM = 2
MODE = "dev"
LOG_LEVEL = "DEBUG"

load_dotenv()
logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


if __name__ == "__main__":
    output_dir = Path("test_outputs") / "tabular"

    logger.info(f"Updating data for {DATASET} from {START} to {END}")

    df = get_metadata()
    # Hard code this to "dev" for now since we don't have the right
    # data locally or in prod
    ds = stack_cogs(START, END, DATASET, "dev")
    ds_upsampled = upsample_raster(ds)

    if LAST_RUN:
        df_update = df[df.src_update >= LAST_RUN]
    else:
        df_update = df

    logger.info(
        f"Data last updated {LAST_RUN}. Recalculating raster stats for {len(df_update)} ISO3s."
    )

    # --- Start by looping through each country
    for idx, row in df_update.iterrows():
        iso3 = df_update.loc[idx, "iso_3"]
        shp_url = df_update.loc[idx, "o_shp"]
        src_max_adm = df_update.loc[idx, "src_lvl"]

        logger.info(f"Processing data for {iso3}...")
        if MODE == "local":
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
                if MODE == "local":
                    adm_dir = country_dir / f"adm{adm_level}"
                    adm_dir.mkdir(exist_ok=True, parents=True)
                else:
                    adm_dir = f"{country_dir}/adm{adm_level}"

                logger.debug(f"Computing for admin{adm_level}")

                gdf = gpd.read_file(f"{td}/{iso3}_adm{adm_level}.shp")

                stats = []
                # --- Each date in the source data
                for date in ds_upsampled.date.values:
                    da_upsampled = ds_upsampled.sel(date=date)
                    df_stats = compute_zonal_statistics(
                        da_upsampled,
                        gdf,
                        f"ADM{adm_level}_PCODE",
                        date=str(da_upsampled.date.values),
                    )
                    stats.append(df_stats)
                df_all_stats = pd.concat(stats, ignore_index=True)

                output_file = os.path.join(adm_dir, f"{DATASET}_raster_stats.parquet")
                write_output_stats(df_all_stats, output_file, MODE)

    logger.info("... Done calculations.")

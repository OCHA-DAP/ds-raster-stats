import logging
import os
from datetime import date, timedelta

import coloredlogs
import pandas as pd
import yaml
from dotenv import load_dotenv

from src.utils.general_utils import (
    get_missing_dates,
    get_most_recent_date,
    parse_extra_dims,
)

load_dotenv()


UPSAMPLED_RESOLUTION = 0.05
LOG_LEVEL = "DEBUG"
AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")
AZURE_DB_PW_PROD = os.getenv("AZURE_DB_PW_PROD")
DATABASES = {
    "local": "sqlite:///chd-rasterstats-local.db",
    "dev": f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_DEV}@chd-rasterstats-dev.postgres.database.azure.com/postgres",  # noqa
    "prod": f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_PROD}@chd-rasterstats-prod.postgres.database.azure.com/postgres",  # noqa
}


logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def load_pipeline_config(pipeline_name):
    config_path = os.path.join(
        os.path.dirname(__file__), f"{pipeline_name}.yml"
    )
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    return config


def config_pipeline(dataset, test, update, mode, backfill, engine):
    """
    Configure pipeline parameters based on dataset configuration and runtime flags.
    Also logs an overall summary of the pipeline run.

    Parameters
    ----------
    dataset : str
        Name of the dataset to process
    test : bool
        If True, use test configuration parameters
    update : bool
        If True, start from most recent date
    mode : str
        Pipeline execution mode
    backfill : bool
        If True, include missing dates in processing
    engine : SQLEngine
        Database connection for retrieving missing dates

    Returns
    -------
    dict
        Dictionary containing
            dates : list of list of datetime.date
                Chunked list of dates to process
            forecast : dict
                Forecast configuration parameters
            sel_iso3s : list or None
                Selected ISO3 country codes, if any
            extra_dims : dict
                Additional dimension parameters
    """
    config = load_pipeline_config(dataset)
    config_section = config["test"] if test else config

    output_config = {}
    output_config["forecast"] = config["forecast"]
    output_config["extra_dims"] = parse_extra_dims(config)
    output_config["sel_iso3s"] = config_section.get("iso3s")

    start_date = config_section["start_date"]
    end_date = config_section.get("end_date")
    frequency = config["frequency"]

    # Now work on getting the dates to process
    if not end_date:
        end_date = date.today() - timedelta(days=1)
    missing_dates = None
    if backfill:
        missing_dates = get_missing_dates(
            engine,
            dataset,
            start_date,
            end_date,
            frequency,
            config["forecast"],
        )

    # TODO: Updating by getting the most recent COG is a bit of a shortcut...
    if update:
        start_date = get_most_recent_date(mode, config["blob_prefix"])
        end_date = None

    dates = generate_date_series(
        start_date, end_date, frequency, missing_dates
    )
    output_config["date_chunks"] = dates

    # Configuration report
    logger.info("=" * 50)
    logger.info("Pipeline Configuration Summary:")
    logger.info("=" * 50)
    logger.info(f"Dataset: {dataset.upper()}")
    logger.info(f"Mode: {mode}")
    if update:
        logger.info(
            f"Run: Updating latest stats -- {start_date.strftime('%Y-%m-%d')}"
        )
    else:
        logger.info(
            f"Run: Archival update from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )
    logger.info(f"Total Date Chunks: {len(dates)}")
    logger.info(f"Total Dates: {sum(len(chunk) for chunk in dates)}")
    logger.info(f"Checked for missing dates: {backfill}")
    if backfill:
        logger.info(f"{len(missing_dates)} missing dates found")
        for date_ in missing_dates:
            logger.info(f" - {date_.strftime('%Y-%m-%d')}")
    if output_config["sel_iso3s"]:
        sel_iso3s = output_config["sel_iso3s"]
        logger.info(f"Filtering for ISO3 codes: {sel_iso3s}")

    logger.info("=" * 50)

    return output_config


def generate_date_series(
    start_date, end_date, frequency="D", missing_dates=None, chunk_size=100
):
    """Generate a sorted list of dates partitioned into chunks.

    Parameters
    ----------
    start_date : str or datetime
        Start date in 'YYYY-MM-DD' format if string
    end_date : str or datetime
        End date in 'YYYY-MM-DD' format if string, or None for single date
    frequency : str, default='D'
        Date frequency, either 'D' for daily or 'M' for monthly
    missing_dates : list, optional
        Additional dates to include in the series
    chunk_size : int, default=100
        Maximum number of dates per chunk

    Returns
    -------
    list of list of datetime.date
        List of date chunks, where each chunk contains up to chunk_size dates,
        sorted in ascending order with duplicates removed
    """
    if not end_date:
        dates = [start_date]
    else:
        dates = list(
            pd.date_range(
                start_date,
                end_date,
                freq="MS" if frequency == "M" else frequency,
            )
        )
    if missing_dates:
        dates.extend(missing_dates)
    dates = sorted(list(set(dates)))
    return [
        dates[i : i + chunk_size] for i in range(0, len(dates), chunk_size)
    ]

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
    config = load_pipeline_config(dataset)
    if test:
        start_date = config["test"]["start_date"]
        end_date = config["test"]["end_date"]
        sel_iso3s = config["test"]["iso3s"]
    else:
        start_date = config["start_date"]
        end_date = config["end_date"]
        sel_iso3s = None

    forecast = config["forecast"]
    extra_dims = parse_extra_dims(config)

    frequency = config["frequency"]
    if not end_date:
        end_date = date.today() - timedelta(days=1)

    missing_dates = None
    if backfill:
        missing_dates = get_missing_dates(
            engine, dataset, start_date, end_date, frequency
        )
        logger.info(f"Filling in {len(missing_dates)} missing dates:")
        for date_ in missing_dates:
            logger.info(f" - {date_.strftime('%Y-%m-%d')}")

    # TODO: Updating by getting the most recent COG is a bit of a shortcut...
    if update:
        start_date = get_most_recent_date(mode, config["blob_prefix"])
        end_date = None

    dates = generate_date_series(
        start_date, end_date, frequency, missing_dates
    )
    return dates, forecast, sel_iso3s, extra_dims, frequency


def generate_date_series(
    start_date, end_date, frequency="D", missing_dates=None, chunk_size=100
):
    """
    Generate a sorted list of dates between start and end dates, incorporating missing dates,
    partitioned into chunks of specified size.

    Parameters:
    start_date (str or datetime): Start date in 'YYYY-MM-DD' format if string
    end_date (str or datetime): End date in 'YYYY-MM-DD' format if string
    frequency (str): 'D' for daily or 'M' for monthly
    missing_dates (list): Optional list of dates to include, in 'YYYY-MM-DD' format if strings
    chunk_size (int): Maximum number of dates per partition

    Returns:
    list of lists: List of date chunks, where each chunk is a list of datetime.date objects
    """
    if not end_date:
        dates = [start_date]
    else:
        dates = pd.date_range(
            start_date, end_date, freq="MS" if frequency == "M" else frequency
        )
    if missing_dates:
        dates.extend(missing_dates)
    dates = sorted(list(set(dates)))
    return [
        dates[i : i + chunk_size] for i in range(0, len(dates), chunk_size)
    ]

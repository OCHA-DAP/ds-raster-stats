import re
from datetime import datetime
from typing import List

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import VARCHAR, Integer

from src.utils.cloud_utils import get_container_client


def add_months_to_date(date_string, months):
    """
    Add or subtract a number of months to/from a given date string.

    Parameters
    ----------
    date_string : str
        The input date in 'YYYY-MM-DD' format.
    months : int
        The number of months to add (positive) or subtract (negative).

    Returns
    -------
    str
        The resulting date after adding/subtracting months, in 'YYYY-MM-DD' format.

    """
    try:
        start_date = datetime.strptime(date_string, "%Y-%m-%d").date()
        result_date = start_date + relativedelta(months=months)
        return result_date.strftime("%Y-%m-%d")
    except ValueError as e:
        raise ValueError(
            "Invalid date format. Please use 'YYYY-MM-DD'."
        ) from e


# TODO: Might not scale well as we get more files in the blob
def get_most_recent_date(mode, name_prefix):
    """
    Find files with the most recent date in their filename from Azure blob storage.

    This function searches through Azure blob storage for files that start with the
    given prefix and match the date pattern for the specified dataset. It returns
    all files that match the most recent date found.

    Parameters
    ----------
    mode : str
        The mode in which the database is being accessed (e.g., 'local', 'dev').
    name_prefix : str
        The prefix of the filename before the date portion.
        For example, 'seas5/monthly/processed/precip_em_i'.

    Returns
    -------
    list of str
        Names of all files that match the most recent date. Empty list if no
        matching files are found.
    """
    container_client = get_container_client(mode, "raster")
    blobs = container_client.list_blobs(name_starts_with=name_prefix)
    file_dates = {}

    for blob in blobs:
        try:
            date = parse_date(blob.name)
            file_dates[blob.name] = date
        except (ValueError, IndexError) as e:
            print(f"Skipping {blob.name}: {str(e)}")
            continue

    if not file_dates:
        return []

    most_recent_date = max(file_dates.values())

    return most_recent_date


def parse_date(filename):
    """
    Parses the date based on a COG filename.
    """
    res = re.search("([0-9]{4}-[0-9]{2}-[0-9]{2})", filename)
    return pd.to_datetime(res[0])


def parse_extra_dims(config):
    parsed_extra_dims = {}

    if "extra_dims" in config.keys():
        extra_dims = config.get("extra_dims")
        for extra_dim in extra_dims:
            dim = next(iter(extra_dim))
            if extra_dim[dim] == "str":
                parsed_extra_dims[dim] = VARCHAR
            else:
                parsed_extra_dims[dim] = Integer

    return parsed_extra_dims


def get_expected_dates(
    start_date: str, end_date: str, frequency: str
) -> pd.DatetimeIndex:
    """
    Generate a complete list of expected dates between start and end dates.

    Parameters
    ----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    frequency : str
        Frequency of dates, either 'D' for daily or 'M' for monthly

    Returns
    -------
    pd.DatetimeIndex
        Complete list of expected dates
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    if frequency == "M":
        # For monthly data, always use first day of month
        dates = pd.date_range(
            start=start.replace(day=1), end=end.replace(day=1), freq="MS"
        )
    elif frequency == "D":
        dates = pd.date_range(start=start, end=end, freq="D")
    else:
        raise ValueError("Frequency must be either 'D' or 'M'")

    return dates


def get_missing_dates(
    engine,
    dataset: str,
    start_date: str,
    end_date: str,
    frequency: str,
    forecast: bool,
) -> List[datetime]:
    """
    Find missing dates in the database by comparing against expected dates.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database connection engine
    dataset : str
        Name of the dataset table in database
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    frequency : str
        Frequency of dates, either 'D' for daily or 'M' for monthly
    forecast : bool
        Whether or not the dataset is a forecast

    Returns
    -------
    List[datetime]
        List of missing dates that need to be processed
    """
    # Get all expected dates
    expected_dates = get_expected_dates(start_date, end_date, frequency)

    date_column = "issued_date" if forecast else "valid_date"

    # Query existing dates from database
    query = (
        f"SELECT DISTINCT {date_column} FROM {dataset} ORDER BY {date_column}"
    )
    existing_dates = pd.read_sql_query(query, engine)
    existing_dates[date_column] = pd.to_datetime(existing_dates[date_column])

    # Find missing dates
    missing_dates = expected_dates[
        ~expected_dates.isin(existing_dates[date_column])
    ]
    return missing_dates.tolist()

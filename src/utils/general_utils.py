import re
from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.utils.cloud_utils import get_container_client


def split_date_range(start_date, end_date):
    """
    Split the date range into yearly chunks if the range is greater than a year.

    Parameters
    ----------
    start_date (str): Start date in 'YYYY-MM-DD' format
    end_date (str): End date in 'YYYY-MM-DD' format

    Returns
    -------
    list of tuples: Each tuple contains the start and end date for a chunk
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # If the date range is less than or equal to a year, return it as a single chunk
    if (end - start).days <= 365:
        return [(start_date, end_date)]

    date_ranges = []
    while start < end:
        year_end = min(datetime(start.year, 12, 31), end)
        date_ranges.append((start.strftime("%Y-%m-%d"), year_end.strftime("%Y-%m-%d")))
        start = year_end + timedelta(days=1)

    return date_ranges


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
        raise ValueError("Invalid date format. Please use 'YYYY-MM-DD'.") from e


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

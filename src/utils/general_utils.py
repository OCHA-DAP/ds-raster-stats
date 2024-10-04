from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta


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

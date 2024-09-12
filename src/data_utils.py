import numpy as np
import pandas as pd


def features_to_dataframe(features, adm_level, start_date, end_date, increment):
    # TODO: No band prefix if there is only 1 band
    stats = ["mean", "median", "min", "max", "count", "stdev"]
    total_cols = len(stats) + 2

    # Count the number of features and bands
    features_count = len(features)  # total number of admin regions
    all_stats = [
        key for key in features[0]["properties"].keys() if key.startswith("band_")
    ]
    dates = list({"_".join(item.split("_")[:2]) for item in all_stats})
    bands_count = len(dates)
    dates = map_bands_to_dates(dates, start_date, end_date, increment)

    # Pre-allocate a numpy array with shape (features * bands, total_cols)
    rows = np.empty((features_count * bands_count, total_cols), dtype=object)

    row_idx = 0
    for feature in features:
        pcode = feature["properties"][f"ADM{adm_level}_PCODE"]  # Get ADM2_PCODE
        for band_num in range(1, bands_count + 1):
            band_prefix = f"band_{band_num}"
            # Extract values for the current band
            mean = feature["properties"].get(f"{band_prefix}_mean", np.nan)
            median = feature["properties"].get(f"{band_prefix}_median", np.nan)
            min_val = feature["properties"].get(f"{band_prefix}_min", np.nan)
            max_val = feature["properties"].get(f"{band_prefix}_max", np.nan)
            count = feature["properties"].get(f"{band_prefix}_count", np.nan)
            stdev = feature["properties"].get(f"{band_prefix}_stdev", np.nan)

            # Assign values to the row
            rows[row_idx] = [
                pcode,
                dates[band_num - 1],  # Band label
                mean,  # mean
                median,  # median
                min_val,  # min
                max_val,  # max
                count,  # count
                stdev,  # stdev
            ]
            row_idx += 1

    # Convert the pre-filled numpy array into a pandas DataFrame
    df = pd.DataFrame(
        rows,
        columns=["pcode", "valid_date", "mean", "median", "min", "max", "count", "std"],
    )
    return df


def map_bands_to_dates(bands, start_date, end_date, increment):
    # Convert start and end dates to pandas Timestamps
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Create a date range based on the increment type
    if increment == "daily":
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
    elif increment == "monthly":
        dates = pd.date_range(
            start=start_date, end=end_date, freq="MS"
        )  # 'MS' is Month Start
    formatted_dates = [date.strftime("%Y-%m-%d") for date in dates]

    # Map the bands to the corresponding dates
    if len(bands) > len(dates):
        raise ValueError("More bands than dates available")

    return formatted_dates

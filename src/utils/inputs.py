import argparse


def cli_args():
    """
    Sets the CLI arguments for running the raster stats data pipeline
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "dataset",
        help="Dataset for which to calculate raster stats",
        choices=["seas5", "era5", "imerg", "floodscan", "baseline"],
        default=None,
        nargs="?",
    )
    parser.add_argument(
        "--mode",
        "-m",
        help="Run the pipeline in 'local', 'dev', or 'prod' mode.",
        type=str,
        choices=["local", "dev", "prod"],
        default="local",
    )
    parser.add_argument(
        "--test",
        help="""Processes a smaller subset of the source data. Use to test the pipeline.""",
        action="store_true",
    )
    parser.add_argument(
        "--update-stats",
        help="""Calculate stats against the latest COG for a given dataset.""",
        action="store_true",
    )
    parser.add_argument(
        "--update-metadata",
        help="Update the iso3 and polygon metadata tables.",
        action="store_true",
    )
    parser.add_argument(
        "--chunksize",
        help="Limit the SQL insert batches to an specific chunksize.",
        type=int,
        default=100000,
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Whether to check and backfill for any missing dates",
    )
    return parser.parse_args()

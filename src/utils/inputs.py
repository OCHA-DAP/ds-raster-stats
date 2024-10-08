import argparse


def cli_args():
    """
    Sets the CLI arguments for running the raster stats data pipeline
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "dataset",
        help="Dataset for which to calculate raster stats",
        choices=["seas5", "era5", "imerg"],
        default=None,
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
        "--build-iso3",
        help="""Builds the `iso3` table in Postgres""",
        action="store_true",
    )
    return parser.parse_args()

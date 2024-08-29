import argparse


def cli_args():
    """
    Sets the CLI arguments for running the raster stats data pipeline
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        "-m",
        help="Run the pipeline in 'local', 'dev', or 'prod' mode.",
        type=str,
        choices=["local", "dev", "prod"],
        default="local",
    )
    parser.add_argument(
        "--dataset",
        "-d",
        help="""Calculate stats for only the indicated dataset.
        Must be on of the options in `config.py`.
        """,
        type=str,
        choices=["seas5", "era5", "imerg"],
        default=None,
    )
    parser.add_argument(
        "--update",
        help="""Will only update the raster stats from the latest source files.""",
        action="store_true",
    )
    parser.add_argument(
        "--test",
        help="""Processes a smaller subset of the source data. Use to test the pipeline.""",
        action="store_true",
    )
    return parser.parse_args()

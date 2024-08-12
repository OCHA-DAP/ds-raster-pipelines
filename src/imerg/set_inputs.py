import argparse
from datetime import datetime

import pandas as pd


def cli_args():
    """
    Sets the CLI arguments for running the IMERG data pipeline
    """
    today = datetime.today()
    yesterday = datetime.today() - pd.DateOffset(days=1)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--create-auth-files",
        "-caf",
        help="Create authorization files for accessing IMERG datasets",
        action="store_true",
    )
    parser.add_argument(
        "--mode",
        "-m",
        help="Run the pipeline in 'local', 'dev', or 'prod' mode.",
        type=str,
        choices=["local", "dev", "prod"],
        default="dev",
    )
    parser.add_argument(
        "--start-date",
        "-s",
        help="""Start date to retrieve and process archival IMERG data. Format: '%Y-%m-%d'
        Minimum: 1st of June 2000.""",
        default=yesterday.strftime("%Y-%m-%d"),
        type=str,
    )
    parser.add_argument(
        "--end-date",
        "-e",
        help="""End year to retrieve and process archival IMERG data. Format: '%Y-%m-%d'
        Maximum: current date.""",
        default=today.strftime("%Y-%m-%d"),
        type=str,
    )
    parser.add_argument(
        "--run",
        "-r",
        help="E for early run, L for late run.",
        type=str,
        choices=["E", "L"],
        default="L",
    )
    parser.add_argument(
        "--version",
        "-v",
        help="IMERG version (7 is technically 07B) or 6",
        type=int,
        choices=[7, 6],
        default=7,
    )
    parser.add_argument(
        "--save-raw",
        "-sr",
        help="""Will save the unprocessed file to specified location.""",
        action="store_true",
    )
    parser.add_argument(
        "--skip-existing",
        "-se",
        help="""Will skip any already downloaded files within the given range located in the container.""",
        action="store_true",
    )

    return parser.parse_args()

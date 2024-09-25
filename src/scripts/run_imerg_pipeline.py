import argparse
from datetime import datetime

import pandas as pd

from src.config.settings import IMERG_SETTINGS
from src.pipelines.imerg_pipeline import IMERGPipeline


def parse_arguments(base_parser):
    today = datetime.today()
    yesterday = today - pd.DateOffset(days=1)
    parser = argparse.ArgumentParser(parents=[base_parser])
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
    parser.add_argument("--update", action="store_true", help="Run in update mode")
    parser.add_argument(
        "--run",
        "-r",
        help="'early' for early run, 'late' for late run.",
        type=str,
        choices=["early", "late"],
        default="late",
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
        "--create-auth-files",
        "-caf",
        help="Create authorization files for accessing IMERG datasets",
        action="store_true",
    )
    return parser.parse_args()


def main(base_parser):
    args = parse_arguments(base_parser)
    settings = IMERG_SETTINGS.copy()
    settings["mode"] = args.mode
    settings["is_update"] = args.update
    settings["start_date"] = args.start_date
    settings["end_date"] = args.end_date
    settings["log_level"] = args.log_level
    settings["use_cache"] = args.use_cache
    settings["version"] = args.version
    settings["run"] = args.run
    settings["create_auth_files"] = args.create_auth_files

    pipeline = IMERGPipeline(**settings)
    pipeline.run_pipeline()

import argparse
from datetime import datetime

import pandas as pd

from src.config.settings import load_pipeline_config
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
    settings = load_pipeline_config("imerg")
    settings.update(
        {
            "mode": args.mode,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "log_level": args.log_level,
            "use_cache": args.use_cache,
            "version": args.version,
            "run": args.run,
            "create_auth_files": args.create_auth_files,
        }
    )

    pipeline = IMERGPipeline(**settings)
    # pipeline.run_pipeline()
    pipeline.print_coverage_report()

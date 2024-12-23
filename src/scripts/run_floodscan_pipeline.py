import argparse
from datetime import datetime

import pandas as pd

from src.config.settings import load_pipeline_config
from src.pipelines.floodscan_pipeline import FloodScanPipeline
from src.utils.date_utils import DATE_FORMAT


def parse_arguments(base_parser):
    today = datetime.today()
    yesterday = today - pd.DateOffset(days=1)
    parser = argparse.ArgumentParser(parents=[base_parser])
    parser.add_argument(
        "--start-date",
        "-s",
        help=f"""Start date to retrieve and process archival FloodScan data.
        Format: {DATE_FORMAT}""",
        default=yesterday.strftime(DATE_FORMAT),
        type=str,
    )
    parser.add_argument(
        "--end-date",
        "-e",
        help=f"""End year to retrieve and process archival FloodScan data.
        Format: {DATE_FORMAT}""",
        default=yesterday.strftime(DATE_FORMAT),
        type=str,
    )
    parser.add_argument(
        "--version",
        "-v",
        help="Version (only 5 available currently)",
        type=int,
        choices=[5],
        default=5,
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Whether to check and backfill for any missing dates (only 2024 onwards)",
    )
    parser.add_argument("--update", action="store_true", help="Run in update mode")
    return parser.parse_args()


def main(base_parser):
    args = parse_arguments(base_parser)
    settings = load_pipeline_config("floodscan")
    settings.update(
        {
            "mode": args.mode,
            "is_update": args.update,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "version": args.version,
            "log_level": args.log_level,
            "use_cache": args.use_cache,
        }
    )

    pipeline = FloodScanPipeline(**settings)
    # pipeline.run_pipeline()
    pipeline.print_coverage_report()

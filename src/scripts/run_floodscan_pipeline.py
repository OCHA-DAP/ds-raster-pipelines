import argparse
from datetime import datetime

import pandas as pd
from src.config.settings import load_pipeline_config
from src.pipelines.floodscan_pipeline import FloodScanPipeline


def parse_arguments(base_parser):
    today = datetime.today()
    yesterday = today - pd.DateOffset(days=1)
    parser = argparse.ArgumentParser(parents=[base_parser])
    parser.add_argument(
        "--start-date",
        "-s",
        help="""Start date to retrieve and process archival FloodScan data. 
        Format: '%Y%m%d""",
        default=yesterday.strftime("%Y%m%d"),
        type=str,
    )
    parser.add_argument(
        "--end-date",
        "-e",
        help="""End year to retrieve and process archival FloodScan data. 
        Format: '%Y%m%d""",
        default=yesterday.strftime("%Y%m%d"),
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
    parser.add_argument("--update", action="store_true", help="Run in update mode")
    parser.add_argument("--historical-run", action="store_true", help="Run in full historical mode")
    return parser.parse_args()


def main(base_parser):
    args = parse_arguments(base_parser)
    settings = load_pipeline_config("floodscan")
    settings.update(
        {
            "mode": args.mode,
            "is_update": args.update,
            "is_full_historical_run": args.historical_run,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "version": args.version,
            "log_level": args.log_level,
            "use_cache": args.use_cache,
        }
    )

    pipeline = FloodScanPipeline(**settings)
    pipeline.run_pipeline()

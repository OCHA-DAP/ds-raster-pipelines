import argparse

from src.config.settings import load_pipeline_config
from src.pipelines.seas5_pipeline import SEAS5Pipeline


def parse_arguments(base_parser):
    parser = argparse.ArgumentParser(parents=[base_parser])
    parser.add_argument(
        "--start-year", type=int, required=False, help="Start year for data processing"
    )
    parser.add_argument(
        "--end-year", type=int, required=False, help="End year for data processing"
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
    settings = load_pipeline_config("seas5")
    settings.update(
        {
            "mode": args.mode,
            "is_update": args.update,
            "backfill": args.backfill,
            "start_year": args.start_year,
            "end_year": args.end_year,
            "log_level": args.log_level,
            "use_cache": args.use_cache,
        }
    )

    pipeline = SEAS5Pipeline(**settings)
    pipeline.run_pipeline()

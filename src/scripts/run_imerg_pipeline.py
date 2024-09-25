import argparse

from src.config.settings import IMERG_SETTINGS
from src.pipelines.imerg_pipeline import IMERGPipeline


def parse_arguments(base_parser):
    parser = argparse.ArgumentParser(parents=[base_parser])
    parser.add_argument(
        "--start", type=int, required=False, help="Start year for data processing"
    )
    parser.add_argument(
        "--end", type=int, required=False, help="End year for data processing"
    )
    parser.add_argument("--update", action="store_true", help="Run in update mode")
    return parser.parse_args()


def main(base_parser):
    args = parse_arguments(base_parser)
    settings = IMERG_SETTINGS.copy()
    settings["mode"] = args.mode
    settings["is_update"] = args.update
    settings["start_year"] = args.start
    settings["end_year"] = args.end
    settings["log_level"] = args.log_level
    settings["use_cache"] = args.use_cache

    pipeline = IMERGPipeline(**settings)
    pipeline.run_pipeline()

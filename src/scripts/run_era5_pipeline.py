import argparse

from src.config.settings import ERA5_SETTINGS
from src.pipelines.era5_pipeline import ERA5Pipeline


def parse_arguments():
    parser = argparse.ArgumentParser(description="Run ERA5 data pipeline")
    parser.add_argument(
        "--mode",
        choices=["local", "dev", "prod"],
        default="local",
        help="Mode to run the pipeline in",
    )
    parser.add_argument("--update", action="store_true", help="Run in update mode")
    parser.add_argument("--start", type=int, help="Start year for historical run")
    parser.add_argument("--end", type=int, help="End year for historical run")
    return parser.parse_args()


def main():
    args = parse_arguments()
    settings = ERA5_SETTINGS.copy()
    settings["mode"] = args.mode
    settings["is_update"] = args.update
    settings["start_year"] = args.start
    settings["end_year"] = args.end

    pipeline = ERA5Pipeline(**settings)
    pipeline.run_pipeline()

import argparse
import sys

from src.scripts.run_era5_pipeline import main as run_era5
from src.scripts.run_imerg_pipeline import main as run_imerg
from src.scripts.run_seas5_pipeline import main as run_seas5


def create_base_parser():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--mode",
        choices=["local", "dev", "prod"],
        default="local",
        help="Mode to run the pipeline in",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Whether to check for existing raw data",
    )
    return parser


def main():
    base_parser = create_base_parser()
    main_parser = argparse.ArgumentParser()
    main_parser.add_argument(
        "pipeline",
        choices=["era5", "seas5", "imerg", "floodscan"],
        help="Pipeline to run",
    )

    args, remaining_args = main_parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining_args

    if args.pipeline == "era5":
        run_era5(base_parser)
    elif args.pipeline == "seas5":
        run_seas5(base_parser)
    elif args.pipeline == "imerg":
        run_imerg(base_parser)
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")


if __name__ == "__main__":
    main()

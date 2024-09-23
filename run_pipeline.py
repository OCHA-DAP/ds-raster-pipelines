import argparse
import sys
from pathlib import Path

from src.scripts.run_era5_pipeline import main as run_era5

project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))


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
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")


if __name__ == "__main__":
    main()

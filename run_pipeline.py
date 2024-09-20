import argparse
import sys
from pathlib import Path

from src.scripts.run_era5_pipeline import main as run_era5

# Add the project root to the Python path
# TODO: Is this needed?
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))


def main():
    parser = argparse.ArgumentParser(description="Run data pipelines")
    parser.add_argument(
        "pipeline",
        choices=["era5", "seas5", "imerg", "floodscan"],
        help="Pipeline to run",
    )
    args, remaining_args = parser.parse_known_args()

    if args.pipeline == "era5":
        pipeline_func = run_era5
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")

    # Run the selected pipeline, passing any remaining arguments
    # TODO: Doesn't seem to be totally working?
    sys.argv = [sys.argv[0]] + remaining_args
    pipeline_func()


if __name__ == "__main__":
    main()

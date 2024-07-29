import argparse
import tempfile
from pathlib import Path

from constants import BBOX_GLOBAL, BBOX_TEST
from src.seas5.download_archive_mars_tprate import *


def check_range(value):
    ivalue = int(value)
    if ivalue < 1981 or ivalue > 2022:
        raise argparse.ArgumentTypeError(
            f"Value {value} is outside the valid range (1981-2022)"
        )
    return ivalue


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        "-s",
        help="Start year to retrieve and process archival SEAS5 data. Must be between 1981 and 2022 (default: 1981).",
        default=1981,
        type=check_range,
    )
    parser.add_argument(
        "--end",
        "-e",
        help="End year to retrieve and process archival SEAS5 data. Must be between 1981 and 2022 (default: 2022).",
        default=2022,
        type=check_range,
    )
    parser.add_argument(
        "--test",
        "-t",
        help="Run the pipeline in test mode. Will save a subset of outputs locally to 'test_outputs/'.",
        action="store_true",
    )

    args = parser.parse_args()

    print(f"Running SEAS5 update from {args.start} to {args.end}")

    if args.test:
        print("Running in 'test' mode: Saving a subset of data locally.")
        bbox = BBOX_TEST
        output_dir = Path("test_outputs")
        output_dir.mkdir(exist_ok=True)
        for year in range(args.start, args.end + 1):
            tp_raw = download_archive(year, bbox, output_dir)
            process_archive(tp_raw, output_dir)
    else:
        with tempfile.TemporaryDirectory() as td:
            bbox = BBOX_GLOBAL
            for year in range(args.start, args.end):
                tp_raw = download_archive(year, bbox, td)
                process_archive(tp_raw, td)

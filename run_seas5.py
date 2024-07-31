import argparse
import logging
import tempfile
from pathlib import Path

import src.seas5.mars_tprate as mars_tprate
from constants import BBOX_GLOBAL, BBOX_TEST

logger = logging.getLogger(__name__)


def check_range(value):
    ivalue = int(value)
    if ivalue < 1981 or ivalue > 2022:
        raise argparse.ArgumentTypeError(
            f"Value {value} is outside the valid range (1981-2022)"
        )
    return ivalue


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

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

    logger.info(f"Running SEAS5 update from {args.start} to {args.end}")

    if args.test:
        logger.info("Running in 'test' mode: Saving a subset of data locally.")
        bbox = BBOX_TEST
        output_dir = Path("test_outputs")
        output_dir.mkdir(exist_ok=True)
        for year in range(args.start, args.end + 1):
            tp_raw = mars_tprate.download_archive(year, bbox, output_dir)
            mars_tprate.process_archive(tp_raw, output_dir)
    else:
        with tempfile.TemporaryDirectory() as td:
            bbox = BBOX_GLOBAL
            for year in range(args.start, args.end + 1):
                tp_raw = mars_tprate.download_archive(year, bbox, td)
                mars_tprate.process_archive(tp_raw, td)

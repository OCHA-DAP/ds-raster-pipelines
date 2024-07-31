import argparse
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import src.seas5.aws_tprate as aws_tprate
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
        help="""Start year to retrieve and process archival SEAS5 data.
        Must be between 1981 and 2022 (default: 1981). Only applies for `--source mars`""",
        default=1981,
        type=check_range,
    )
    parser.add_argument(
        "--end",
        "-e",
        help="""End year to retrieve and process archival SEAS5 data.
        Must be between 1981 and 2022 (default: 2022). Only applies for `--source mars`""",
        default=2022,
        type=check_range,
    )
    parser.add_argument(
        "--mode",
        "-m",
        help="Run the pipeline in 'local', 'dev', or 'prod' mode.",
        type=str,
        choices=["local", "dev", "prod"],
        default="local",
    )
    parser.add_argument(
        "--source",
        "-src",
        help="Data download source",
        type=str,
        choices=["mars", "aws"],
    )
    parser.add_argument(
        "--backfill-aws",
        help="""Will backfill all previous months of AWS data.
        If not flagged, only data from the current month will be processed.""",
        action="store_true",
    )

    args = parser.parse_args()

    if args.source == "mars":
        logger.info(f"Retrieving SEAS5 archive from {args.start} to {args.end}...")

        if args.mode == "local":
            logger.info("Running in 'local' mode: Saving a subset of data locally.")
            bbox = BBOX_TEST
            output_dir = Path("test_outputs")
            output_dir.mkdir(exist_ok=True)
            for year in range(args.start, args.end + 1):
                tp_raw = mars_tprate.download_archive(year, bbox, output_dir)
                mars_tprate.process_archive(tp_raw, output_dir)
        else:
            logger.info(
                f"Running in '{args.mode}' mode. Saving data to {args.mode} Azure storage."
            )
            with tempfile.TemporaryDirectory() as td:
                bbox = BBOX_GLOBAL
                for year in range(args.start, args.end + 1):
                    tp_raw = mars_tprate.download_archive(year, bbox, td, args.mode)
                    mars_tprate.process_archive(tp_raw, td, args.mode)

    elif args.source == "aws":
        cur_month = int(datetime.now().strftime("%m"))
        months = list(range(1, cur_month + 1)) if args.backfill_aws else [cur_month]
        logger.info(f"Retrieving SEAS5 updates from AWS bucket for months: {months}...")

        if args.mode == "local":
            logger.info("Running in 'local' mode: Saving data locally.")
            output_dir = Path("test_outputs")
            output_dir.mkdir(exist_ok=True)
            for month in months:
                aws_tprate.run_update(month, output_dir, args.mode)

        else:
            logger.info(
                f"Running in '{args.mode}' mode. Saving data to {args.mode} Azure storage."
            )
            with tempfile.TemporaryDirectory() as td:
                for month in months:
                    aws_tprate.run_update(month, td, args.mode)

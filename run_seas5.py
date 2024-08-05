import logging
import tempfile
from datetime import datetime
from pathlib import Path

import src.seas5.aws_tprate as aws_tprate
import src.seas5.mars_tprate as mars_tprate
from constants import BBOX_GLOBAL, BBOX_TEST
from src.seas5.set_inputs import cli_args

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.WARNING)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    args = cli_args()

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

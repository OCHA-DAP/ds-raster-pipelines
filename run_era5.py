import logging
import tempfile
from pathlib import Path

import src.era5.cds_tp as cds_tp
from src.era5 import cli_args

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    args = cli_args()

    if args.source == "mars":
        logger.info(f"Retrieving SEAS5 archive from {args.start} to {args.end}...")

        if args.mode == "local":
            logger.info("Running in 'local' mode: Saving data locally.")
            output_dir = Path("test_outputs")
            output_dir.mkdir(exist_ok=True)
            for year in range(args.start, args.end + 1):
                tp_raw = cds_tp.download_archive(year, output_dir)
                cds_tp.process_archive(tp_raw, output_dir)
        else:
            logger.info(
                f"Running in '{args.mode}' mode. Saving data to {args.mode} Azure storage."
            )
            with tempfile.TemporaryDirectory() as td:
                for year in range(args.start, args.end + 1):
                    tp_raw = cds_tp.download_archive(year, td, args.mode)
                    cds_tp.process_archive(tp_raw, td, args.mode)

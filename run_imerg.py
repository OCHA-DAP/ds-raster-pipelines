import logging
import tempfile
from datetime import datetime

from src.imerg import imerg
from src.imerg.create_auth_files import create_auth_files
from src.imerg.set_inputs import cli_args
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    args = cli_args()

    if args.create_auth_files:
        create_auth_files()

    logger.info(f"Retrieving IMERG archive from {args.start_date} to {args.end_date}...")

    if args.mode == "local":
        logger.info("Running in 'local' mode: Saving a subset of data locally.")
        output_dir = Path("test_outputs")
        output_dir.mkdir(exist_ok=True)

        for date in pd.date_range(
                datetime.strptime(args.start_date, '%Y-%m-%d'), datetime.strptime(args.end_date, '%Y-%m-%d') - pd.DateOffset(days=1)
        ):
            tp_raw = imerg.download(date=date,
                                    run=args.run,
                                    version=args.version,
                                    save_raw=args.save_raw,
                                    output_dir=output_dir,
                                    mode=args.mode)
    else:
        logger.info(
            f"Running in '{args.mode}' mode. Saving data to {args.mode} Azure storage."
        )
        with tempfile.TemporaryDirectory() as td:
            for date in pd.date_range(
                    datetime.strptime(args.start_date, '%Y-%m-%d'),
                    datetime.strptime(args.end_date, '%Y-%m-%d') - pd.DateOffset(days=1)
            ):
                tp_raw = imerg.download(date=date,
                                        run=args.run,
                                        version=args.version,
                                        save_raw=args.save_raw,
                                        output_dir=td,
                                        mode=args.mode)

    logger.info("Finished running pipeline.")
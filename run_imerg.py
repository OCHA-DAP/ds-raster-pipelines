import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.imerg import imerg
from src.imerg.create_auth_files import create_auth_files
from src.imerg.set_inputs import cli_args

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    args = cli_args()
    run_type = "late" if args.run == "L" else "early"

    if args.create_auth_files:
        create_auth_files()

    logger.info(
        f"Retrieving IMERG {run_type} archive from {args.start_date} to {args.end_date}..."
    )

    if args.mode == "local":
        logger.info("Running in 'local' mode: Saving a subset of data locally.")
        output_dir = Path("test_outputs")
        output_dir.mkdir(exist_ok=True)

        for date in pd.date_range(
            datetime.strptime(args.start_date, "%Y-%m-%d"),
            datetime.strptime(args.end_date, "%Y-%m-%d") - pd.DateOffset(days=1),
        ):
            tp_raw = imerg.download(
                date=date,
                run=args.run,
                version=args.version,
                save_raw=args.save_raw,
                output_dir=output_dir,
                mode=args.mode,
            )
            if tp_raw:
                tp_processed = imerg.process_nc4(
                    date=date,
                    run=args.run,
                    version=args.version,
                    output_dir=tp_raw,
                    mode=args.mode,
                )
    else:
        logger.info(
            f"Running in '{args.mode}' mode. Saving data to {args.mode} Azure storage."
        )
        with tempfile.TemporaryDirectory() as td:
            for date in pd.date_range(
                datetime.strptime(args.start_date, "%Y-%m-%d"),
                datetime.strptime(args.end_date, "%Y-%m-%d") - pd.DateOffset(days=1),
            ):
                tp_raw = imerg.download(
                    date=date,
                    run=args.run,
                    version=args.version,
                    save_raw=args.save_raw,
                    output_dir=td,
                    mode=args.mode,
                )
                if tp_raw:
                    tp_processed = imerg.process_nc4(
                        date=date,
                        run=args.run,
                        version=args.version,
                        path_raw=tp_raw,
                        output_dir=td,
                        mode=args.mode,
                    )

    logger.info("Finished running pipeline.")

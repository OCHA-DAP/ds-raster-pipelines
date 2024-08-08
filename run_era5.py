import logging
import tempfile
from pathlib import Path

import src.era5.cds_tp as cds_tp
from src.era5.set_inputs import cli_args

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.WARNING)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    args = cli_args()

    if args.mode == "local":
        logger.info("Running in 'local' mode: Saving data locally.")
        output_dir = Path("test_outputs")
        output_dir.mkdir(exist_ok=True)
        cds_tp.run_update(args.update, args.start, args.end, output_dir, args.mode)

    else:
        logger.info(
            f"Running in '{args.mode}' mode. Saving data to {args.mode} Azure storage."
        )
        with tempfile.TemporaryDirectory() as td:
            cds_tp.run_update(args.update, args.start, args.end, td, args.mode)

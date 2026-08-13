# TEMPORARY (2026-08-12): repair affine 3.x on the Databricks job
# clusters. affine 3.0.0 (released 2026-08-08) is an attrs slots-class
# that needs a modern attrs to make functools.cached_property work
# without an instance __dict__; the clusters' preinstalled attrs is too
# old for that, so every Affine.__getitem__ raises TypeError ("No
# '__dict__' attribute on 'Affine' instance") — the Run IMERG job has
# failed daily since 2026-08-08 because of this. The jobs' library
# lists leave affine floating, so the workaround lives here: when the
# probe shows affine is broken, downgrade its cached_property members
# to plain (uncached) properties. No-op when affine is healthy. Remove
# once the job configs pin affine==2.4.0.
import functools

import affine


def _repair_affine() -> None:
    try:
        affine.Affine.identity()[0]
    except TypeError:
        for _name, _member in list(vars(affine.Affine).items()):
            if isinstance(_member, functools.cached_property):
                setattr(affine.Affine, _name, property(_member.func))
        affine.Affine.identity()[0]  # fail loudly if still broken


_repair_affine()

import argparse  # noqa: E402
import sys  # noqa: E402

from src.scripts.run_era5_pipeline import main as run_era5  # noqa: E402
from src.scripts.run_floodscan_pipeline import main as run_floodscan  # noqa: E402
from src.scripts.run_imerg_pipeline import main as run_imerg  # noqa: E402
from src.scripts.run_seas5_pipeline import main as run_seas5  # noqa: E402


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
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Whether to check for existing raw data",
    )
    return parser


def main():
    base_parser = create_base_parser()
    main_parser = argparse.ArgumentParser()
    main_parser.add_argument(
        "pipeline",
        choices=["era5", "floodscan", "imerg", "seas5"],
        help="Pipeline to run",
    )

    args, remaining_args = main_parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining_args

    if args.pipeline == "era5":
        run_era5(base_parser)
    elif args.pipeline == "floodscan":
        run_floodscan(base_parser)
    elif args.pipeline == "imerg":
        run_imerg(base_parser)
    elif args.pipeline == "seas5":
        run_seas5(base_parser)
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")


if __name__ == "__main__":
    main()

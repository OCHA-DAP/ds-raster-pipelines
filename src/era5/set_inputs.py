import argparse


def check_range(value):
    """
    Checks that an input year is within the acceptable range
    of values for SEAS5 MARS retrieval
    """
    ivalue = int(value)
    if ivalue < 1981 or ivalue > 2023:
        raise argparse.ArgumentTypeError(
            f"Value {value} is outside the valid range (1981-2023)"
        )
    return ivalue


def cli_args():
    """
    Sets the CLI arguments for running the SEAS5 data pipeline
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        "-m",
        help="Run the pipeline in 'local', 'dev', or 'prod' mode.",
        type=str,
        choices=["local", "dev", "prod"],
        default="local",
    )
    parser.add_argument(
        "--start",
        "-s",
        help="""Start year to retrieve and process archival ERA5 data.
        Must be between 1981 and 2023 (default: 1981).""",
        default=1981,
        type=check_range,
    )
    parser.add_argument(
        "--end",
        "-e",
        help="""End year to retrieve and process archival ERA5 data.
        Must be between 1981 and 2023 (default: 2023).""",
        default=2023,
        type=check_range,
    )

    return parser.parse_args()

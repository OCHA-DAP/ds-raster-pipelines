import argparse


def check_year_range(value):
    """
    Checks that an input year is within the acceptable range
    of values for ERA5 CDS retrieval
    """
    ivalue = int(value)
    if ivalue < 1981 or ivalue > 2024:
        raise argparse.ArgumentTypeError(
            f"Year {value} is outside the valid range (1981-2024)"
        )
    return ivalue


def check_month_range(value):
    """
    Checks that an input month is within the acceptable range
    of values for ERA5 CDS retrieval
    """
    ivalue = int(value)
    if ivalue < 1 or ivalue > 12:
        raise argparse.ArgumentTypeError(
            f"Month {value} is outside the valid range (1-12)"
        )
    return ivalue


def cli_args():
    """
    Sets the CLI arguments for running the ERA5 data pipeline
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
        Must be between 1981 and 2024 (default: 1981).""",
        default=1981,
        type=check_year_range,
    )
    parser.add_argument(
        "--end",
        "-e",
        help="""End year to retrieve and process archival ERA5 data.
        Must be between 1981 and 2024 (default: 2024).""",
        default=2024,
        type=check_year_range,
    )
    parser.add_argument(
        "--month",
        help="""Month from which to retrieve ERA5 data.
        Must be between 1 and 12 (default: None).
        Retrieves all data from the year if not specified.""",
        default=None,
        type=check_year_range,
    )

    return parser.parse_args()

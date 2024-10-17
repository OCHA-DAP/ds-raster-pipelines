import argparse
import re
from datetime import timedelta, datetime

DATE_FORMAT = "%Y-%m-%d"

def create_date_range(start, end, min_accepted=None, max_accepted=None):
    """
    Method to create a range of dates between two dates.

    Args:
        start (datetime.datetime): Range start date.
        end (datetime.datetime): Range end date.
        min_accepted (datetime.datetime): Optional parameter to set a threshold for minimum value accepted.
        max_accepted (datetime.datetime): Optional parameter to set a threshold for maximum value accepted.

    Returns:
        dates (list): List with the range dates
    """

    if start > end:
        raise argparse.ArgumentTypeError(
            f"End date {end.date()} is before the start date {start.date()}"
        )
    if min_accepted:
        if min(start, end) < min_accepted:
            raise argparse.ArgumentTypeError(
                f"Start date {start.date()} is before the minimum accepted date {min_accepted.date()}"
            )
    if max_accepted:
        if max(start, end) > max_accepted:
            raise argparse.ArgumentTypeError(
                f"End date {end.date()} is before the minimum accepted date {max_accepted.date()}"
            )

    date_range = []
    current_date = start
    while current_date <= end:
        date_range.append(current_date)
        current_date += timedelta(days=1)

    if len(date_range) == 0:
        raise argparse.ArgumentTypeError(
            f"Date range cannot be empty."
        )

    return date_range


def get_datetime_from_filename(filename):
    try:
        return datetime.strptime(re.search("([0-9]{4}-[0-9]{2}-[0-9]{2})", filename)[0], "%Y-%m-%d")
    except Exception as err:
        raise argparse.ArgumentError(f"Cannot get datetime from {filename}: {err}")
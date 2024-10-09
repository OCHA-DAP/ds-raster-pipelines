import argparse
import logging

import coloredlogs
import numpy as np
from datetime import timedelta

logger = logging.getLogger(__name__)
coloredlogs.install(
    level="DEBUG",
    logger=logger,
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def change_longitude_range(
    ds,
    lon_coord: str,
):
    """
    If longitude ranges from 0 to 360,
    change it to range from -180 to 180.
    Args:
        ds (xarray dataset): dataset that should be transformed
        lon_coord: name of the longitude coordinate

    Returns:
        ds_lon (xarray dataset): dataset with transformed longitude
        coordinates
    """
    ds_lon = ds.assign_coords(
        {lon_coord: (((ds[lon_coord] + 180) % 360) - 180)}
    ).sortby(lon_coord)
    return ds_lon


def round_lat_lon(ds_in, lat_coord, lon_coord):
    ds = ds_in.copy()
    ds[lat_coord] = np.round(ds[lat_coord].values, 4)
    ds[lon_coord] = np.round(ds[lon_coord].values, 4)
    return ds


def invert_lat_lon(
    ds,
    lon_coord="x",
    lat_coord="y",
):
    """
    This function checks for inversion of latitude and longitude
    and changes them if needed.

    We expect lon to go from -180 to 180, and lat to go from 90 to -90.

    Function largely copied from
    https://github.com/perrygeo/python-rasterstats/issues/218
    Args:
        ds (xarray dataset): dataset containing the variables
        and coordinates

    Returns:
        da (xarray dataset): dataset containing the variables
        and flipped coordinates
    """
    lat_start = ds[lat_coord][0].item()
    lat_end = ds[lat_coord][-1].item()
    lon_start = ds[lon_coord][0].item()
    lon_end = ds[lon_coord][-1].item()
    if lat_start < lat_end:
        logger.warning("Dataset was north down, latitude coordinates have been flipped")
        ds = ds.reindex({lat_coord: ds[lat_coord][::-1]})
    if lon_start > lon_end:
        logger.error("Inverted longitude still needs to be implemented..")

    return ds


def create_date_range(start, end, min_accepted=None, max_accepted=None):
    """
    Method to create a range of dates between two dates.
    TODO change the parameters to datetime?
    Args:
        start_date (datetime.datetime): Range start date.
        end_date (datetime.datetime): Range end date.
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

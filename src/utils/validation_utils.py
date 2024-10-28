import logging

import coloredlogs
import numpy as np
import xarray

from src.utils.date_utils import get_datetime_from_filename

logger = logging.getLogger(__name__)
coloredlogs.install(
    level="DEBUG",
    logger=logger,
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def validate_dataset(
    da,
    filename,
    lat_var="y",
    lon_var="x",
    lat_range=(90, -90),
    lon_range=(-180, 180),
    num_attrs=15,
) -> bool:
    """Validate the dataset meets expected criteria."""
    date, date_type = get_datetime_from_filename(filename, return_type=True)
    date_type = "valid" if date_type == "v" else "issued"
    if (
        da[lat_var][0].item() > lat_range[0]
        or da[lat_var][-1].item() < lat_range[1]  # noqa
        or da[lon_var][0].item() < lon_range[0]  # noqa
        or da[lon_var][-1].item() > lon_range[1]  # noqa
    ):
        logger.error("Coordinate range is not as expected")
        return False
    if str(da.rio.crs) != "EPSG:4326":
        logger.error(f"CRS is not as expected: {da.rio.crs}")
        return False
    if type(da) == xarray.core.dataset.Dataset:
        if any(type(da.dtypes[key]) != np.dtypes.Float32DType for key in da.dtypes):
            logger.error(f"Incorrect data type: {da.dtypes}")
            return False
    else:
        if da.dtype != np.float32:
            logger.error(f"Incorrect data type: {da.dtype}")
            return False
    if len(da.attrs) != num_attrs:
        logger.error(
            f"Data does not have correct number of metadata fields: {len(da.attrs)}"
        )
        return False

    # Monthly datasets will have an empty `date` attr,
    # but will have the 1st of the month in the filename
    da_date = da.attrs[f"date_{date_type}"] if da.attrs[f"date_{date_type}"] else 1
    da_month = da.attrs[f"month_{date_type}"]
    da_year = da.attrs[f"year_{date_type}"]
    if date.day != da_date or date.month != da_month or date.year != da_year:
        logger.error(
            f"Date does not match filename {filename}: day: {da_date}"
            f"month: {da_month} and year: {da_year}."
        )
        return False
    base_attrs = [
        "averaging_period",
        "date_issued",
        "date_valid",
        "download_date",
        "grid_resolution",
        "leadtime",
        "leadtime_units",
        "month_issued",
        "month_valid",
        "product",
        "source",
        "units",
        "version",
        "year_issued",
        "year_valid",
    ]
    if set(list(da.attrs.keys())) != set(base_attrs):
        logger.error(
            f"Data does not have correct metadata fields: {list(da.attrs.keys())}"
        )
        return False
    return True

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

    # Check if the filename has an `issued` or `valid` date
    date, date_type = get_datetime_from_filename(filename, return_type=True)
    date_type = "issued" if date_type == "i" else "valid"

    # -- Coordinate range should make sense
    if (
        da[lat_var][0].item() > lat_range[0]
        or da[lat_var][-1].item() < lat_range[1]  # noqa
        or da[lon_var][0].item() < lon_range[0]  # noqa
        or da[lon_var][-1].item() > lon_range[1]  # noqa
    ):
        logger.error("Coordinate range is not as expected")
        return False

    # -- CRS should be as expected
    if str(da.rio.crs) != "EPSG:4326":
        logger.error(f"CRS is not as expected: {da.rio.crs}")
        return False

    # -- Data types should be float32
    if type(da) == xarray.core.dataset.Dataset:
        if any(type(da.dtypes[key]) != np.dtypes.Float32DType for key in da.dtypes):
            logger.error(f"Incorrect data type: {da.dtypes}")
            return False
    else:
        if da.dtype != np.float32:
            logger.error(f"Incorrect data type: {da.dtype}")
            return False

    # -- All standard attributes should be present
    if len(da.attrs) != num_attrs:
        logger.error(
            f"Data does not have correct number of metadata fields: {len(da.attrs)}"
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

    # -- The date in the filename should match the corresponding date metadata
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

    # This is to make sure the other date_type fields are null
    inv_type = "issued" if date_type == "valid" else "valid"
    if (
        da.attrs[f"date_{inv_type}"]
        or da.attrs[f"month_{inv_type}"]
        or da.attrs[f"year_{inv_type}"]
    ):
        logger.error(
            f"All the '_{inv_type}' fields should be null for date type {date_type}."
        )
        return False

    if da.attrs["leadtime"]:
        return validate_metadata_leadtime(da.attrs)

    return True


def validate_metadata_leadtime(metadata):
    """
    Validates that the leadtime in metadata correctly represents the time difference
    between issued_date and valid_date based on the specified leadtime_units. Note
    that only leadtimes in months or days are supported.

    Parameters:
    -----------
    metadata : dict
        Dictionary containing forecast metadata with the following keys:
        - date_issued: Full issued date string or None
        - date_valid: Full valid date string or None
        - year_issued: Year when forecast was issued
        - month_issued: Month when forecast was issued
        - year_valid: Year for which forecast is valid
        - month_valid: Month for which forecast is valid
        - leadtime: Expected time difference between issued and valid dates
        - leadtime_units: Units of the leadtime (currently supports 'months' and 'days')

    Returns:
    --------
    bool
        True if the relationship is valid

    Raises:
    -------
    ValueError
        If the calculated leadtime doesn't match the expected leadtime,
        or if required fields are missing, or if leadtime_units are unsupported
    """
    # Construct issued date
    date_issued = 1 if metadata["date_issued"] is None else metadata["date_issued"]
    full_issued_date = np.datetime64(
        f"{metadata['year_issued']}-{metadata['month_issued']:02d}-{date_issued:02d}"
    )

    # Construct valid date
    date_valid = 1 if metadata["date_valid"] is None else metadata["date_valid"]
    full_valid_date = np.datetime64(
        f"{metadata['year_valid']}-{metadata['month_valid']:02d}-{date_valid:02d}"
    )

    # Calculate the difference based on leadtime_units
    if metadata["leadtime_units"] == "months":
        issued_month = full_issued_date.astype("datetime64[M]")
        valid_month = full_valid_date.astype("datetime64[M]")
        calculated_leadtime = (valid_month - issued_month).astype(int)

    elif metadata["leadtime_units"] == "days":
        calculated_leadtime = (
            (full_valid_date - full_issued_date).astype("timedelta64[D]").astype(int)
        )

    else:
        raise ValueError(f"Unsupported leadtime_units: {metadata['leadtime_units']}")

    is_valid = calculated_leadtime == metadata["leadtime"]
    if not is_valid:
        raise ValueError(
            f"Leadtime mismatch: Expected {metadata['leadtime']} {metadata['leadtime_units']}, "
            f"but calculated {calculated_leadtime} {metadata['leadtime_units']}. "
            f"Issued date: {full_issued_date}, Valid date: {full_valid_date}"
        )

    return True

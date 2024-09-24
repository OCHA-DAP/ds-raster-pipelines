import logging

import coloredlogs
import numpy as np

logger = logging.getLogger(__name__)
coloredlogs.install(
    level="DEBUG",
    logger=logger,
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def validate_dataset(
    da,
    lat_var="y",
    lon_var="x",
    lat_range=(90, 90),
    lon_range=(-180, 180),
    num_attrs=15,
) -> bool:
    """Validate the dataset meets expected criteria."""

    if (
        da[lat_var][0].item() < lat_range[0]
        or da[lat_var][-1].item() > lat_range[1]  # noqa
        or da[lon_var][0].item() < lon_range[0]  # noqa
        or da[lon_var][-1].item() > lon_range[1]  # noqa
    ):
        logger.error("Coordinate range is not as expected")
        return False
    if str(da.rio.crs) != "EPSG:4326":
        logger.error(f"CRS is not as expected: {da.rio.crs}")
        return False
    if np.isnan(da.values).any():
        logger.error("Data contains null values")
        return False
    if da.dtype != np.float32:
        logger.error(f"Incorrect data type: {da.dtype}")
        return False
    if len(da.attrs) != num_attrs:
        logger.error(
            f"Data does not have correct number of metadata fields: {len(da.attrs)}"
        )
        return False
    return True

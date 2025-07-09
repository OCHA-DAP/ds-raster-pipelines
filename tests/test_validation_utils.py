import logging

import numpy as np
import pytest
import rioxarray  # noqa: F401
import xarray as xr

from src.pipelines import seas5_pipeline
from src.pipelines.pipeline import Pipeline
from src.utils.validation_utils import validate_dataset, validate_metadata_leadtime

LOGGER = logging.getLogger(__name__)


SAMPLE_DATA = np.array(
    [
        [1, 2, 3, 4],
        [5, 6, 7, 8],
        [9, 10, 11, 12],
        [13, 14, 15, 16],
    ],
    dtype=np.float32,
)


def sample_xarray_dataset(attrs):
    da = xr.Dataset(
        {"data": (["y", "x"], SAMPLE_DATA)},
        coords={"x": np.linspace(-1, 1, 4), "y": np.linspace(1, -1, 4)},
    )

    da.attrs = Pipeline._set_metadata(None, {})
    for key in attrs.keys():
        da.attrs[key] = attrs[key]

    da.rio.write_crs("EPSG:4326", inplace=True)
    return da


def sample_xarray_dataarray(attrs):
    da = xr.DataArray(
        SAMPLE_DATA,
        dims=["y", "x"],
        coords={
            "x": np.linspace(-1, 1, 4),
            "y": np.linspace(1, -1, 4),
        },
    )
    da.attrs = Pipeline._set_metadata(None, {})
    for key in attrs.keys():
        da.attrs[key] = attrs[key]

    da.rio.write_crs("EPSG:4326", inplace=True)
    return da


@pytest.mark.parametrize(
    "test_id, metadata",
    [
        (
            "zero_month",
            {
                "year_issued": 2025,
                "month_issued": 3,
                "date_issued": None,
                "year_valid": 2025,
                "month_valid": 3,
                "date_valid": None,
                "leadtime": 0,
                "leadtime_units": "months",
            },
        ),
        (
            "three_month",
            {
                "year_issued": 2025,
                "date_issued": None,
                "month_issued": 3,
                "year_valid": 2025,
                "month_valid": 6,
                "date_valid": None,
                "leadtime": 3,
                "leadtime_units": "months",
            },
        ),
        (
            "day-based",
            {
                "year_issued": 2025,
                "date_issued": 15,
                "month_issued": 3,
                "year_valid": 2025,
                "month_valid": 3,
                "date_valid": 31,
                "leadtime": 16,
                "leadtime_units": "days",
            },
        ),
        (
            "year_boundary",
            {
                "year_issued": 2025,
                "month_issued": 11,
                "date_issued": None,
                "year_valid": 2026,
                "month_valid": 2,
                "date_valid": None,
                "leadtime": 3,
                "leadtime_units": "months",
            },
        ),
    ],
)
def test_valid_metadata(test_id, metadata):
    """Test cases that should pass validation"""
    assert validate_metadata_leadtime(metadata) is True


# Test cases that should raise specific exceptions
@pytest.mark.parametrize(
    "test_id, metadata, error_type, error_message",
    [
        (
            "leadtime_mismatch",
            {
                "year_issued": 2025,
                "month_issued": 3,
                "year_valid": 2025,
                "month_valid": 6,
                "date_valid": None,
                "date_issued": None,
                "leadtime": 2,  # Should be 3
                "leadtime_units": "months",
            },
            ValueError,
            "Leadtime mismatch",
        ),
        (
            "unsupported_units",
            {
                "year_issued": 2025,
                "month_issued": 3,
                "year_valid": 2025,
                "month_valid": 9,
                "date_valid": None,
                "date_issued": None,
                "leadtime": 2,
                "leadtime_units": "quarters",  # Unsupported
            },
            ValueError,
            "Unsupported leadtime_units",
        ),
    ],
)
def test_invalid_metadata(test_id, metadata, error_type, error_message):
    """Test cases that should fail validation with specific errors"""
    with pytest.raises(error_type) as excinfo:
        validate_metadata_leadtime(metadata)
    assert error_message in str(excinfo.value)


@pytest.mark.parametrize(
    "test_id, attrs, filename, error_message",
    [
        (
            "IMERG_month_mismatch",
            {
                "year_issued": 2025,
                "month_issued": 1,
                "year_valid": 2025,
                "month_valid": 9,
                "date_valid": 1,
                "date_issued": None,
            },
            "imerg-daily-late-2025-01-01.tif",
            "Date does not match filename imerg-daily-late-2025-01-01.tif: day: 1"
            "month: 9 and year: 2025.",
        ),
        (
            "SEAS5_day_mismatch",
            {
                "year_issued": 2025,
                "month_issued": 3,
                "year_valid": 2025,
                "month_valid": 6,
                "date_valid": None,
                "date_issued": None,
            },
            "precip_em_i2025-01-01_lt0.tif",
            "Date does not match filename precip_em_i2025-01-01_lt0.tif: day: 1"
            "month: 3 and year: 2025.",
        ),
        (
            "ERA5_year_mismatch",
            {
                "year_issued": 2025,
                "month_issued": 1,
                "year_valid": 2024,
                "month_valid": 1,
                "date_valid": 1,
                "date_issued": None,
            },
            "precip_reanalysis_v2025-05-01.tif",
            "Date does not match filename precip_reanalysis_v2025-05-01.tif: day: 1"
            "month: 1 and year: 2024.",
        ),
        (
            "FloodScan_missing_day_mismatch",
            {
                "year_issued": 2025,
                "month_issued": 1,
                "year_valid": 2025,
                "month_valid": 1,
                "date_valid": None,
                "date_issued": None,
            },
            "aer_area_300s_v2025-01-02_v05r01.tif",
            "Date does not match filename aer_area_300s_v2025-01-02_v05r01.tif: day: 1"
            "month: 1 and year: 2025.",
        ),
        (
            "FloodScan_with_non_null_fields",
            {
                "year_issued": 2025,  # Should be null
                "month_issued": None,
                "year_valid": 2025,
                "month_valid": 1,
                "date_valid": None,
                "date_issued": None,
            },
            "aer_area_300s_v2025-01-01_v05r01.tif",
            "All the '_issued' fields should be null for date type valid.",
        ),
        (
            "SEAS5_with_null_fields",
            {
                "year_issued": 2025,
                "month_issued": 1,
                "year_valid": 2025,
                "month_valid": 6,
                "date_valid": None,  # Should not be null
                "date_issued": None,  # Should not be null
            },
            "precip_em_i2025-01-01_lt0.tif",
            "All the '_valid' fields should be null for date type issued.",
        ),
        (
            "ERA5_with_non_null_fields",
            {
                "year_issued": 2025,
                "month_issued": 1,
                "year_valid": 2025,
                "month_valid": 1,
                "date_valid": 1,
                "date_issued": None,
            },
            "precip_reanalysis_v2025-01-01.tif",
            "All the '_issued' fields should be null for date type valid.",
        ),
    ],
)
def test_validate_dataset_throws_error(test_id, attrs, filename, error_message, caplog):
    """Test cases that should fail validation with specific errors"""
    da = sample_xarray_dataarray(attrs)
    with caplog.at_level(logging.ERROR):
        assert validate_dataset(da, filename) is False
    assert error_message in str(caplog.text)


@pytest.mark.parametrize(
    "test_id, fc_month, issued_month, year, filename, time",
    [
        (
            "SEAS5_test1",
            6,
            3,
            2025,
            "precip_em_i2025-03-01_lt3.tif",
            np.datetime64("2025-03-01T00:00:00.000000000"),
        ),
        (
            "SEAS5_test2",
            1,
            9,
            2024,
            "precip_em_i2024-09-01_lt4.tif",
            np.datetime64("2024-09-01T00:00:00.000000000"),
        ),
    ],
)
def test_valid_seas5_filenames(test_id, fc_month, issued_month, year, filename, time):
    """Test cases that should fail validation with specific errors"""
    ds, seas5_run_pipeline = setup_seas5_pipeline(fc_month, issued_month, time, year)

    assert (
        seas5_run_pipeline.process_after_2024(
            ds_mean=ds, fc_month=fc_month, issued_month=issued_month, year=year
        )[1]
        == filename
    )


@pytest.mark.parametrize(
    "test_id, fc_month, issued_month, year, time, error_message",
    [
        (
            "SEAS5_test_wrong_year",
            6,
            3,
            2025,
            np.datetime64("2024-03-01T00:00:00.000000000"),
            "Date mismatch: 2025-03-01 does not match dataset time 2024-03-01T00:00:00.000000000",
        ),
        (
            "SEAS5_test_wrong_issued_date",
            1,
            1,
            2024,
            np.datetime64("2024-09-01T00:00:00.000000000"),
            "Date mismatch: 2024-01-01 does not match dataset time 2024-09-01T00:00:00.000000000",
        ),
    ],
)
def test_date_mismatch_seas5(
    test_id, fc_month, issued_month, year, time, error_message
):
    """Test cases that should fail validation with specific errors"""
    ds, seas5_run_pipeline = setup_seas5_pipeline(fc_month, issued_month, time, year)
    with pytest.raises(ValueError) as excinfo:
        seas5_run_pipeline.process_after_2024(
            ds_mean=ds, fc_month=fc_month, issued_month=issued_month, year=year
        )
    assert error_message in str(excinfo.value)


def setup_seas5_pipeline(fc_month, issued_month, time, year):
    attrs = {
        "year_issued": year,
        "month_issued": issued_month,
        "year_valid": year,
        "month_valid": fc_month,
        "leadtime": issued_month,
        "leadtime_units": "months",
    }
    ds = sample_xarray_dataset(attrs)
    ds["time"] = time
    seas5_run_pipeline = seas5_pipeline.SEAS5Pipeline(
        mode="local",
        is_update=False,
        start_year=2020,
        end_year=2021,
        log_level="INFO",
        container_name="test-container",
        raw_path="test-raw",
        processed_path="test-processed",
        use_cache=False,
        backfill=False,
        metadata={},
        coverage={},
        server=None,
        aws_bucket_name="aws-bucket",
        bbox={
            "dev": [60, 29, 75, 38],
            "local": [60, 29, 75, 38],
            "prod": [-180, -90, 180, 90],
        },
    )
    return ds, seas5_run_pipeline

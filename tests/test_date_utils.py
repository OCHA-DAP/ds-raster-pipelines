import argparse
from datetime import datetime

import pytest
from typing_extensions import assert_type

from src.utils.date_utils import create_date_range, get_datetime_from_filename


@pytest.mark.parametrize(
    "start, end, min_accepted, max_accepted, expected_len",
    [
        (
            datetime(year=1998, month=1, day=1),
            datetime(year=2024, month=1, day=1),
            None,
            None,
            9497,
        ),
        (
            datetime(year=2004, month=1, day=1),
            datetime(year=2024, month=1, day=1),
            datetime(year=2004, month=1, day=1),
            None,
            7306,
        ),
        (
            datetime(year=2004, month=1, day=1),
            datetime(year=2024, month=1, day=1),
            datetime(year=2004, month=1, day=1),
            datetime(year=2024, month=1, day=1),
            7306,
        ),
    ],
)
def test_create_date_range_returns_correct_length(
    start, end, min_accepted, max_accepted, expected_len
):
    assert (
        len(create_date_range(start, end, min_accepted, max_accepted)) == expected_len
    )


@pytest.mark.parametrize(
    "start, end, min_accepted, max_accepted",
    [
        (
            datetime(year=2004, month=1, day=1),
            datetime(year=2024, month=2, day=12),
            None,
            datetime(year=2014, month=1, day=1),
        ),
        (
            datetime(year=2004, month=1, day=1),
            datetime(year=2024, month=2, day=12),
            datetime(year=2024, month=1, day=1),
            None,
        ),
    ],
)
def test_create_date_range_raises_exception(start, end, min_accepted, max_accepted):
    try:
        create_date_range(start, end, min_accepted, max_accepted)
    except argparse.ArgumentTypeError as err:
        assert_type(err, argparse.ArgumentTypeError)


@pytest.mark.parametrize(
    "filename, expected",
    [
        (
            "aer_mfed_area_300s_20231129_v05r01.tif",
            (datetime(year=2023, month=11, day=29)),
        ),
        (
            "aer_mfed_area_300s_2023-11-29_v05r01.tif",
            (datetime(year=2023, month=11, day=29), "_"),
        ),
        (
            "precip_reanalysis_v2020-06-01.tif",
            (datetime(year=2020, month=6, day=1), "v"),
        ),
        (
            "imerg-v7-imerg-daily-late-2024-01-02.tif",
            (datetime(year=2024, month=1, day=2), "-"),
        ),
        (
            "daily_precip_em_i1990-12-01_lt6.tif",
            (datetime(year=1990, month=12, day=1), "i"),
        ),
        (
            "aer_floodscan_mfed_area_flooded_fraction_africa_90days_2024-02-26.zip",
            (datetime(year=2024, month=2, day=26), "_"),
        ),
    ],
)
def test_get_datetime_from_filename(filename, expected):
    assert get_datetime_from_filename(filename, return_type=True) == expected

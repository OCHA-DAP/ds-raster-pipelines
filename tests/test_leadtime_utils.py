import pytest

from src.utils.leadtime_utils import (
    leadtime_months,
    to_fc_month,
    to_fc_year,
    to_leadtime,
)


@pytest.mark.parametrize(
    "pub_month, fc_month, expected",
    [
        (1, 3, 2),
        (12, 1, 1),
        (6, 6, 0),
        (1, 12, 11),
        (7, 2, 7),
    ],
)
def test_to_leadtime(pub_month, fc_month, expected):
    assert to_leadtime(pub_month, fc_month) == expected


@pytest.mark.parametrize(
    "start_month, leadtime, expected",
    [
        (1, 3, [1, 2, 3]),
        (12, 3, [12, 1, 2]),
        (6, 12, [6, 7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5]),
        (1, 1, [1]),
        (7, 6, [7, 8, 9, 10, 11, 12]),
    ],
)
def test_leadtime_months(start_month, leadtime, expected):
    assert leadtime_months(start_month, leadtime) == expected


@pytest.mark.parametrize(
    "pub_month, leadtime, expected",
    [
        (1, 0, 1),
        (12, 1, 1),
        (6, 6, 12),
        (1, 12, 1),
        (7, 8, 3),
    ],
)
def test_to_fc_month(pub_month, leadtime, expected):
    assert to_fc_month(pub_month, leadtime) == expected


@pytest.mark.parametrize(
    "pub_month, pub_year, leadtime, expected",
    [
        (1, 2020, 0, 2020),
        (12, 2020, 1, 2021),
        (6, 2020, 6, 2020),
        (1, 2020, 12, 2021),
        (7, 2020, 18, 2022),
    ],
)
def test_to_fc_year(pub_month, pub_year, leadtime, expected):
    assert to_fc_year(pub_month, pub_year, leadtime) == expected

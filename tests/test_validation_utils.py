import pytest

from src.utils.validation_utils import validate_metadata_leadtime


@pytest.mark.parametrize(
    "test_id, metadata",
    [
        (
            "zero_month",
            {
                "year_issued": 2025,
                "month_issued": 3,
                "date_issued": "None",
                "year_valid": 2025,
                "month_valid": 3,
                "date_valid": "None",
                "leadtime": 0,
                "leadtime_units": "months",
            },
        ),
        (
            "three_month",
            {
                "year_issued": 2025,
                "date_issued": "None",
                "month_issued": 3,
                "year_valid": 2025,
                "month_valid": 6,
                "date_valid": "None",
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
                "date_issued": "None",
                "year_valid": 2026,
                "month_valid": 2,
                "date_valid": "None",
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
                "date_valid": "None",
                "date_issued": "None",
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
                "date_valid": "None",
                "date_issued": "None",
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

from unittest.mock import call, patch

import pytest

from src.pipelines.era5_pipeline import ERA5Pipeline


@pytest.fixture
def pipeline(monkeypatch):
    monkeypatch.setenv("CDSAPI_KEY", "dummy-key")
    monkeypatch.setenv("CDSAPI_URL", "dummy-url")
    return ERA5Pipeline(
        mode="local",
        is_update=False,
        start_year=2020,
        end_year=2021,
        log_level="INFO",
        container_name="test-container",
        raw_path="test-raw",
        processed_path="test-processed",
        use_cache=False,
        metadata={},
    )


def test_generate_raw_filename(pipeline):
    assert (
        pipeline._generate_raw_filename(2020)
        == "tp_reanalysis_monthly_2020_all.grib"  # noqa
    )
    assert (
        pipeline._generate_raw_filename(2020, 6)
        == "tp_reanalysis_monthly_2020_06.grib"  # noqa
    )


def test_generate_processed_filename(pipeline):
    assert (
        pipeline._generate_processed_filename("2020-06-01")
        == "precip_reanalysis_v2020-06-01.tif"  # noqa
    )


@patch("src.pipelines.era5_pipeline.ERA5Pipeline.get_raw_data")
@patch("src.pipelines.era5_pipeline.ERA5Pipeline.process_data")
def test_run_pipeline_full(mock_process_data, mock_get_raw_data, pipeline):
    pipeline.run_pipeline()
    # Check if get_raw_data and process_data are called 2 times (2 years before 2024)
    assert mock_get_raw_data.call_count == 2
    assert mock_process_data.call_count == 2
    # Check if get_raw_data is called with correct arguments
    expected_calls = [call(year=year) for year in range(2020, 2022)]
    actual_calls = mock_get_raw_data.call_args_list
    assert actual_calls == expected_calls


@patch("src.pipelines.era5_pipeline.ERA5Pipeline.get_raw_data")
@patch("src.pipelines.era5_pipeline.ERA5Pipeline.process_data")
def test_run_pipeline_update(mock_process_data, mock_get_raw_data, pipeline):
    pipeline.is_update = True
    pipeline.run_pipeline()
    # In update mode, it should only process the last month
    assert mock_get_raw_data.call_count == 1
    assert mock_process_data.call_count == 1

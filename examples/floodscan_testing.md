---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.3
  kernelspec:
    display_name: venv
    language: python
    name: python3
---

## Floodscan Pipeline testing

This notebook runs through some basic testing for data updates with the `FloodscanPipeline`. All outputs are saved locally. Running this notebook will empty any local processed Flooscan COGs.

```python
%load_ext autoreload
%autoreload 2
```

Load Floodscan config and dependencies

```python
from datetime import datetime, timedelta
from src.pipelines.floodscan_pipeline import FloodScanPipeline
from dotenv import load_dotenv
from src.config.settings import load_pipeline_config
from src.utils.azure_utils import blob_client, download_from_azure
import os
import glob

load_dotenv()

settings = load_pipeline_config("floodscan")
```

Set up an instance of the class. Note: Starting in `local` mode

```python
settings.update(
    {
        "mode": "local",
        "is_update": True,
        "backfill": True,
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "version": 5,
        "log_level": "INFO",
        "use_cache": False, # NOTE: Bug if use_cache: True
    }
)

floodscan = FloodScanPipeline(**settings)
```

Set up expected local directory with raw files

```python
blob = blob_client("dev")

sfed_local_file_path = floodscan.local_raw_dir / floodscan.sfed_historical
mfed_local_file_path = floodscan.local_raw_dir / floodscan.mfed_historical

# Download the pre-2024 files
download_from_azure(
    blob_service_client=blob,
    container_name=floodscan.container_name,
    blob_path=floodscan.raw_path / floodscan.sfed_historical,
    local_file_path=sfed_local_file_path,
)

download_from_azure(
    blob_service_client=blob,
    container_name=floodscan.container_name,
    blob_path=floodscan.raw_path / floodscan.mfed_historical,
    local_file_path=mfed_local_file_path,
)

# Download the zip files
sfed_dates = [datetime(2024,2, 26), datetime(2024,2, 28)]
mfed_dates = [datetime(2024,2, 26), datetime(2024,2, 27), datetime(2024,2, 28)]

for date in sfed_dates:
    raw_filename = floodscan._generate_raw_filename(date, "SFED")
    download_from_azure(
        blob_service_client=blob,
        container_name=floodscan.container_name,
        blob_path=floodscan.raw_path / raw_filename,
        local_file_path=floodscan.local_raw_dir / raw_filename,
    )

for date in mfed_dates:
    raw_filename = floodscan._generate_raw_filename(date, "MFED")
    download_from_azure(
        blob_service_client=blob,
        container_name=floodscan.container_name,
        blob_path=floodscan.raw_path / raw_filename,
        local_file_path=floodscan.local_raw_dir / raw_filename,
    )
```

```python
def check_and_clean_directory(valid_dates, just_clean=False):
    raw_dir = floodscan.local_processed_dir
    expected_filenames = {f'aer_area_300s_v{date.strftime('%Y-%m-%d')}_v05r01.tif' for date in valid_dates}
    all_files = set(os.path.basename(f) for f in glob.glob(os.path.join(raw_dir, '*.tif')))

    if not just_clean:
        # Check if files match exactly with expected files
        if all_files != expected_filenames:
            extra_files = all_files - expected_filenames
            missing_files = expected_filenames - all_files

            error_msg = []
            if extra_files:
                error_msg.append(f"Unexpected files found: {extra_files}")
            if missing_files:
                error_msg.append(f"Missing expected files: {missing_files}")

            raise AssertionError(" | ".join(error_msg))

    # If we get here, the assertion passed, now remove all files
    for file in all_files:
        full_path = os.path.join(raw_dir, file)
        os.remove(full_path)
        print(f"Removed: {full_path}")
```

```python
check_and_clean_directory([], just_clean=True)
```

## Testing basic functionality

**Test 1**: Run latest update

```python
floodscan.process_latest_update()

# Check and cleanup
yesterday = (datetime.now() - timedelta(days=1)).date()
check_and_clean_directory([yesterday])
```

**Test 2**: Process single historical date where we have both SFED and MFED raw files present

```python
test_date = [datetime(2024,2, 26)]
floodscan.process_historical_dates(test_date)

check_and_clean_directory(test_date)
```

**Test 3**: Process single historical date where only MFED raw file present, but has raw file from within the preceeding 90 days

```python
test_date = [datetime(2024,2, 27)]
floodscan.process_historical_dates(test_date)

check_and_clean_directory(test_date)
```

**Test 4**: Processing single historical date where no raw file present, and no raw file from within the preceeding 90 days

```python
test_date = [datetime(2024,3, 27)]
floodscan.process_historical_dates(test_date)

check_and_clean_directory([]) # No data updates! Should this error instead?
```

**Test 5**: Process single historical date from before 2024 (so pulling from NetCDF file)

```python
test_date = [datetime(2023,1, 27)]
floodscan.process_historical_dates(test_date)

check_and_clean_directory(test_date)
```

**Test 6**: Process multiple historical dates, which have a mix of the above conditions

```python
test_dates = [
    datetime(2023, 1, 25),  # Pre 2024
    datetime(2024, 1, 25),  # Raw file in preceeding 90 days
    datetime(2024, 2, 25),  # Raw file in preceeding 90 days
    datetime(2024, 2, 26),  # All there
    datetime(2024, 2, 27),  # Only mfed
    datetime(2024, 2, 28),  # All there
]
floodscan.process_historical_dates(test_dates)

check_and_clean_directory(test_dates)
```

**Test 7**: Print coverage report

```python
floodscan.print_coverage_report()
```

## End to end tests

**Test 1**: Basic daily update, without any backfilling

```python
settings.update({"is_update": True, "backfill": False})
floodscan = FloodScanPipeline(**settings)
floodscan.run_pipeline()
```

**Test 2**: Basic daily update, with backfilling

```python
settings.update({"is_update": True, "backfill": True})
floodscan = FloodScanPipeline(**settings)
floodscan.run_pipeline()
```

**Test 3**: Test historical update, between two dates in 2024

```python
settings.update({"start_date": "2024-02-26","end_date": "2024-02-28", "backfill": False, "is_update": False})
floodscan = FloodScanPipeline(**settings)
floodscan.run_pipeline()
```

**Test 4**: Test historical update, between two dates in 2023

```python
settings.update({"start_date": "2023-02-26","end_date": "2023-02-28"})
floodscan = FloodScanPipeline(**settings)
floodscan.run_pipeline()
```

**Test 5**: Test historical update, between two dates that span 2023 and 2024

```python
settings.update({"start_date": "2023-12-26","end_date": "2024-01-05", "is_update": False})
floodscan = FloodScanPipeline(**settings)
floodscan.run_pipeline()
```

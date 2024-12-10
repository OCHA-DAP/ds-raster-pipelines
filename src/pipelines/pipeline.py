import logging
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import coloredlogs
import pandas as pd
import xarray
from azure.storage.blob import StandardBlobTier

from ..utils.azure_utils import blob_client, download_from_azure, upload_file_by_mode
from ..utils.date_utils import get_datetime_from_filename
from ..utils.validation_utils import validate_dataset


class Pipeline(ABC):
    def __init__(
        self,
        container_name,
        raw_path,
        processed_path,
        log_level,
        metadata,
        coverage,
        mode="local",
        use_cache=False,
    ):
        self.container_name = container_name
        self.raw_path = Path(raw_path)
        self.processed_path = Path(processed_path)
        self.mode = mode
        self.use_cache = use_cache
        self.metadata = self._set_metadata(metadata)
        self.coverage = self._set_coverage(coverage)
        self.logger = self._setup_logger(log_level)

        if self.mode == "local":
            self.base_dir = Path("test_local")
        else:
            self.temp_dir = tempfile.mkdtemp()
            self.base_dir = Path(self.temp_dir)

        self.local_raw_dir = self.base_dir / self.raw_path
        self.local_processed_dir = self.base_dir / self.processed_path

        self.local_raw_dir.mkdir(parents=True, exist_ok=True)
        self.local_processed_dir.mkdir(parents=True, exist_ok=True)

        if self.mode != "local":
            self.blob_service_client = blob_client(self.mode)

    @abstractmethod
    def query_api(self, **kwargs):
        pass

    @abstractmethod
    def process_data(self, raw_file_path):
        pass

    @abstractmethod
    def run_pipeline(self, **kwargs):
        pass

    @abstractmethod
    def _generate_raw_filename(self, **kwargs):
        pass

    @abstractmethod
    def _generate_processed_filename(self, **kwargs):
        pass

    def _set_metadata(self, metadata):
        standard_metadata = {
            "units": None,
            "averaging_period": None,
            "grid_resolution": None,
            "year_valid": None,
            "year_issued": None,
            "month_valid": None,
            "month_issued": None,
            "date_valid": None,
            "date_issued": None,
            "leadtime": None,
            "leadtime_units": None,
            "source": None,
            "version": None,
            "product": None,
            "download_date": datetime.today().strftime("%Y-%m-%d"),
        }
        standard_metadata.update(metadata)
        return standard_metadata

    def _set_coverage(self, coverage: dict) -> dict:
        default_config = {"start_date": None, "end_date": None, "frequency": "M"}
        if coverage:
            default_config.update(coverage)

        valid_frequencies = ["D", "M", "Y"]
        if default_config["frequency"] not in valid_frequencies:
            raise ValueError(f"Frequency must be one of {valid_frequencies}")
        if default_config["start_date"]:
            try:
                pd.to_datetime(default_config["start_date"])
            except ValueError:
                raise ValueError("Invalid start_date format. Use YYYY-MM-DD")
        if default_config["end_date"]:
            try:
                pd.to_datetime(default_config["end_date"])
            except ValueError:
                raise ValueError("Invalid end_date format. Use YYYY-MM-DD")

        return default_config

    def _setup_logger(self, log_level):
        logger = logging.getLogger(self.__class__.__name__)
        coloredlogs.install(
            level=log_level,
            logger=logger,
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        return logger

    def get_raw_data(self, **kwargs):
        if self.use_cache:
            return self._get_cached_raw_data(**kwargs)
        else:
            return self.query_api(**kwargs)

    def _get_cached_raw_data(self, **kwargs):
        filename = self._generate_raw_filename(**kwargs)
        if self.mode == "local":
            file_path = self.local_raw_dir / filename
            if file_path.exists():
                self.logger.info(f"Using cached raw data: {file_path}")
                return filename
        else:
            blob_path = self.raw_path / filename
            local_file_path = self.local_raw_dir / filename
            if download_from_azure(
                self.blob_service_client,
                self.container_name,
                blob_path,
                local_file_path,
            ):
                self.logger.info(f"Using cached raw data from cloud: {blob_path}")
                return local_file_path

        self.logger.info("No cached data found. Querying API...")
        return self.query_api(**kwargs)

    def _get_existing_dates(self):
        """Get list of dates from existing processed files."""
        if self.mode == "local":
            path = self.local_processed_dir
            files = list(path.glob("*.tif"))
        else:
            blobs = self.blob_service_client.get_container_client(
                self.container_name
            ).list_blobs(name_starts_with=str(self.processed_path))
            files = [blob.name for blob in blobs if blob.name.endswith(".tif")]

        dates = []
        for file in files:
            file_name = Path(file).name
            file_date = get_datetime_from_filename(file_name)
            dates.append(file_date)

        return sorted(set(dates))

    def check_coverage(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
    ) -> Tuple[List[pd.Timestamp], float]:
        """
        Check coverage of pipeline outputs.

        Args:
            start_date: Override start_date from config if provided
            end_date: Override end_date from config if provided
            frequency: Override frequency from config if provided

        Returns:
            Tuple of (missing dates, coverage percentage)
        """
        # Use config values if not overridden
        start_date = start_date or self.coverage["start_date"]
        frequency = frequency or self.coverage["frequency"]

        if not start_date:
            raise ValueError(
                "start_date must be provided either in config or as parameter"
            )

        if end_date is None:
            end_date = self.coverage["end_date"] or datetime.now().strftime("%Y-%m-%d")

        expected_dates = pd.date_range(
            start=start_date, end=end_date, freq="MS" if frequency == "M" else frequency
        )
        existing_dates = self._get_existing_dates()

        missing_dates = [date for date in expected_dates if date not in existing_dates]
        coverage_pct = (len(existing_dates) / len(expected_dates)) * 100

        return missing_dates, coverage_pct

    def print_coverage_report(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
    ) -> None:
        """Print a formatted coverage report."""
        missing_dates, coverage_pct = self.check_coverage(
            start_date, end_date, frequency
        )

        self.logger.info(f"Coverage Report for {self.__class__.__name__}")
        self.logger.info("=" * 50)
        self.logger.info(f"Mode: {self.mode}")
        self.logger.info(f"Storage Path: {self.processed_path}")
        self.logger.info(f"Coverage: {coverage_pct:.1f}%")

        if missing_dates:
            self.logger.info("Missing Dates:")
            for date in missing_dates:
                self.logger.info(f" - {date.strftime('%Y-%m-%d')}")
        else:
            self.logger.info("No missing dates found!")

    def get_raw_data_from_blob(self, filename, folder=None):
        blob_path = self.raw_path / filename
        if folder:
            blob_path = self.raw_path / folder / filename
        local_file_path = self.local_raw_dir / filename
        if download_from_azure(
            self.blob_service_client,
            self.container_name,
            blob_path,
            local_file_path,
        ):
            self.logger.info(f"Downloading raw data from cloud: {blob_path}")

    def save_raw_data(self, filename, folder=None):
        if self.mode != "local":
            local_path = self.local_raw_dir / filename
            blob_path = self.raw_path / filename
            if folder:
                blob_path = self.raw_path / folder / filename
            upload_file_by_mode(self.mode, self.container_name, local_path, blob_path)
        return

    def save_processed_data(self, ds, filename, folder=None):
        local_path = self.local_processed_dir / filename
        if type(ds) == xarray.core.dataset.Dataset:
            da = ds
        else:
            try:
                da = ds.to_dataarray()
            except AttributeError as e:
                da = ds
                self.logger.warning(f"Input data is already a DataArray: {e}")
        if len(da.attrs) != 15:
            da.attrs = self.metadata
        if not validate_dataset(da, filename):
            raise ValueError("Dataset failed validation")
        da.rio.to_raster(local_path, driver="COG")
        if self.mode != "local":
            local_path = self.local_processed_dir / filename
            blob_path = self.processed_path / filename
            if folder:
                blob_path = self.processed_path / folder / filename
            self.logger.info(f"Uploading processed data {local_path} to {blob_path}")
            upload_file_by_mode(
                self.mode,
                self.container_name,
                local_path,
                blob_path,
                StandardBlobTier.HOT,
                "image/tiff",
            )
        return

    def __del__(self):
        if hasattr(self, "temp_dir"):
            import shutil

            shutil.rmtree(self.temp_dir)
            self.logger.info(f"Cleaned up temporary directory: {self.temp_dir}")

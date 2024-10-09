import logging
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import coloredlogs
from azure.storage.blob import StandardBlobTier

from ..utils.azure_utils import blob_client, download_from_azure, upload_file_by_mode
from ..utils.validation_utils import validate_dataset


class Pipeline(ABC):
    def __init__(
        self,
        container_name,
        raw_path,
        processed_path,
        log_level,
        metadata,
        mode="local",
        use_cache=False,
    ):
        self.container_name = container_name
        self.raw_path = Path(raw_path)
        self.processed_path = Path(processed_path)
        self.mode = mode
        self.use_cache = use_cache
        self.metadata = self._set_metadata(metadata)
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
                blob_path = self.raw_path / folder /filename
            upload_file_by_mode(self.mode, self.container_name, local_path, blob_path)
        return

    def save_processed_data(self, ds, filename, folder=None):
        local_path = self.local_processed_dir / filename
        try:
            da = ds.to_dataarray()
        except AttributeError as e:
            da = ds
            self.logger.warning(f"Input data is already a DataArray: {e}")
        da.attrs = self.metadata
        #TODO add back! if not validate_dataset(da):
        #    raise ValueError("Dataset failed validation")
        da.rio.to_raster(local_path, driver="COG")
        if self.mode != "local":
            local_path = self.local_processed_dir / filename
            blob_path = self.processed_path / filename
            if folder:
                blob_path = self.processed_path / folder / filename
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

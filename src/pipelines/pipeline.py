import logging
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

import coloredlogs
from azure.storage.blob import StandardBlobTier

from ..utils.azure_utils import blob_client, download_from_azure, upload_file_by_mode


class Pipeline(ABC):
    def __init__(
        self,
        container_name,
        raw_path,
        processed_path,
        log_level,
        mode="local",
        use_cache=False,
    ):
        self.container_name = container_name
        self.raw_path = Path(raw_path)
        self.processed_path = Path(processed_path)
        self.mode = mode
        self.use_cache = use_cache
        self.logger = self.setup_logger(log_level)

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
    def _generate_raw_filename(self, **kwargs):
        pass

    @abstractmethod
    def _generate_processed_filename(self, **kwargs):
        pass

    def setup_logger(self, log_level):
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
                return file_path
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

    def save_raw_data(self, filename):
        if self.mode != "local":
            local_path = self.local_raw_dir / filename
            blob_path = self.raw_path / filename
            upload_file_by_mode(self.mode, self.container_name, local_path, blob_path)
        return

    def save_processed_data(self, da, filename):
        local_path = self.local_processed_dir / filename
        da.rio.to_raster(local_path, driver="COG")
        if self.mode != "local":
            local_path = self.local_processed_dir / filename
            blob_path = self.processed_path / filename
            upload_file_by_mode(
                self.mode,
                self.container_name,
                local_path,
                blob_path,
                StandardBlobTier.HOT,
                "image/tiff",
            )
        return

    def run_pipeline(self, **kwargs):
        raw_data = self.get_raw_data(**kwargs)
        processed_data = self.process_data(raw_data)
        filename = self._generate_processed_filename(**kwargs)
        self.save_processed_data(processed_data, filename)

    def __del__(self):
        if hasattr(self, "temp_dir"):
            import shutil

            shutil.rmtree(self.temp_dir)

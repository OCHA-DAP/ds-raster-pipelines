import os
import platform
import shutil
from datetime import datetime
from subprocess import Popen

import pandas as pd
import requests
import xarray as xr

from ..utils.raster_utils import invert_lat_lon
from .pipeline import Pipeline


class IMERGPipeline(Pipeline):
    def __init__(self, mode, is_update, start_date, end_date, log_level, **kwargs):
        raw_path = kwargs["raw_path"].format(run_type=kwargs["run"])
        processed_path = kwargs["processed_path"].format(run_type=kwargs["run"])

        super().__init__(
            container_name=kwargs["container_name"],
            raw_path=raw_path,
            processed_path=processed_path,
            log_level=log_level,
            mode=mode,
            metadata=kwargs["metadata"],
            use_cache=kwargs["use_cache"],
        )
        self.run_type = kwargs["run"]
        self.is_update = is_update
        self.start_date = start_date
        self.end_date = end_date
        self.imerg_username = os.getenv("IMERG_USERNAME")
        self.imerg_password = os.getenv("IMERG_PASSWORD")
        self.imerg_base_url = kwargs["base_url"]

        self.version = kwargs["version"]
        self.create_auth_files = kwargs["create_auth_files"]

    def _generate_raw_filename(self, date):
        return f"imerg-daily-{self.run_type}-{date.strftime('%Y-%m-%d')}.nc4"

    def _generate_processed_filename(self, date):
        return f"imerg-daily-{self.run_type}-{date.strftime('%Y-%m-%d')}.tif"

    def query_api(self, date):
        run_type = "L" if self.run_type == "late" else "E"
        version_letter = "B" if self.version == 7 else ""
        filename = self._generate_raw_filename(date)

        self.logger.info(f"Downloading data from {date}: {filename}")

        url = self.imerg_base_url.format(
            run=run_type,
            date=date,
            version=self.version,
            version_letter=version_letter,
        )
        try:
            result = requests.get(url)
            result.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.logger.error(f"Failed downloading: {err}")
            return None

        with open(self.local_raw_dir / filename, "wb") as f:
            f.write(result.content)

        self.save_raw_data(filename)
        return filename

    def process_data(self, raw_filename, date):
        filename = self._generate_processed_filename(date)
        raw_file_path = self.local_raw_dir / raw_filename

        with xr.open_dataset(raw_file_path) as ds:
            ds = ds.transpose("lat", "lon", "time", "nv")
            var_name = (
                "precipitationCal" if "precipitationCal" in ds else "precipitation"
            )
            da = ds[var_name]
            if not ds["time"].dtype == "<M8[ns]":
                da["time"] = pd.to_datetime(
                    [pd.Timestamp(t.strftime("%Y-%m-%d")) for t in da["time"].values]
                )
            da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
            self.metadata["date_valid"] = date.day
            self.metadata["month_valid"] = date.month
            self.metadata["year_valid"] = date.year
            da = invert_lat_lon(da)
            da = da.rio.write_crs("EPSG:4326", inplace=False)
            self.save_processed_data(da, filename)

    def _create_auth_files(self):
        # script to set credentials from
        # https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Generate%20Earthdata%20Prerequisite%20Files
        urs = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication
        homeDir = os.path.expanduser("~") + os.sep
        with open(homeDir + ".netrc", "w") as file:
            file.write(
                "machine {} login {} password {}".format(
                    urs, self.imerg_username, self.imerg_password
                )
            )
            file.close()
        with open(homeDir + ".urs_cookies", "w") as file:
            file.write("")
            file.close()
        with open(homeDir + ".dodsrc", "w") as file:
            file.write("HTTP.COOKIEJAR={}.urs_cookies\n".format(homeDir))
            file.write("HTTP.NETRC={}.netrc".format(homeDir))
            file.close()

        self.logger.info("Saved .netrc, .urs_cookies, and .dodsrc to:", homeDir)

        # Set appropriate permissions for Linux/macOS
        if platform.system() != "Windows":
            Popen("chmod og-rw ~/.netrc", shell=True)
        else:
            # Copy dodsrc to working directory in Windows
            shutil.copy2(homeDir + ".dodsrc", os.getcwd())
            self.logger.info("Copied .dodsrc to:", os.getcwd())

    def run_pipeline(self):
        self.logger.info(f"Running IMERG pipeline in {self.mode} mode...")
        self.logger.info(
            f"Retrieving IMERG data from {self.start_date} to {self.end_date}..."
        )
        if self.create_auth_files:
            self._create_auth_files()
        for date in pd.date_range(
            datetime.strptime(self.start_date, "%Y-%m-%d"),
            datetime.strptime(self.end_date, "%Y-%m-%d") - pd.DateOffset(days=1),
        ):
            self.logger.info(f"Getting data for {date}...")
            raw_filename = self.get_raw_data(date=date)
            self.process_data(raw_filename, date)
        self.logger.info("Completed IMERG update.")

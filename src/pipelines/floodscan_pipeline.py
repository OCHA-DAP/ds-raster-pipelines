import os
import re
import shutil
from datetime import datetime
from fileinput import filename
from zipfile import ZipFile

import pandas as pd
import requests
import xarray as xr

from ..utils.azure_utils import blob_client, download_from_azure
from ..utils.date_utils import (
    DATE_FORMAT,
    create_date_range,
    get_datetime_from_filename,
)
from ..utils.raster_utils import invert_lat_lon
from .pipeline import Pipeline

SFED = "SFED"
MFED = "MFED"


class FloodScanPipeline(Pipeline):
    def __init__(self, **kwargs):
        raw_path = kwargs["raw_path"]
        processed_path = kwargs["processed_path"]
        kwargs["metadata"]["version"] = kwargs["version"]

        super().__init__(
            container_name=kwargs["container_name"],
            raw_path=raw_path,
            processed_path=processed_path,
            log_level=kwargs["log_level"],
            mode=kwargs["mode"],
            metadata=kwargs["metadata"],
            use_cache=kwargs["use_cache"],
        )

        self.start_date = datetime.strptime(kwargs["start_date"], DATE_FORMAT)
        self.end_date = datetime.strptime(kwargs["end_date"], DATE_FORMAT)
        self.is_update = kwargs["is_update"]
        self.is_full_historical_run = kwargs["is_full_historical_run"]
        self.version = kwargs["version"]
        self.sfed_historical = kwargs["sfed_historical"]
        self.mfed_historical = kwargs["mfed_historical"]
        self.sfed_base_url = os.getenv("FLOODSCAN_SFED_URL")
        self.mfed_base_url = os.getenv("FLOODSCAN_MFED_URL")

    def _generate_raw_filename(self, date, type):
        return f"aer_floodscan_{type.lower()}_area_flooded_fraction_africa_90days_{date.strftime(DATE_FORMAT)}.zip"

    def _generate_processed_filename(self, date):
        return f"aer_area_300s_{date.strftime(DATE_FORMAT)}_v0{self.version}r01.tif"

    def get_date_geotiff_from_daily_90_days_file(self, date, filepath):
        with ZipFile(filepath, "r") as zipObj:
            listOfFileNames = zipObj.namelist()
            self.logger.info(
                f"Most recent geotiff in this file is: {max(listOfFileNames)}"
            )
            for fileName in listOfFileNames:
                if fileName.endswith(
                    f"{date.strftime(DATE_FORMAT)}_v0{self.version}r01.tif"
                ):
                    try:
                        full_path = zipObj.extract(fileName, os.path.dirname(filepath))
                        tif_filename = os.path.basename(
                            shutil.move(full_path, os.path.dirname(filepath))
                        )
                        return tif_filename
                    except Exception as e:
                        self.logger.info(f"Failed to extract {filename()}: {e}")

        raise ValueError(
            f"No filename match for the date: {date.strftime(DATE_FORMAT)}. "
        )

    def get_historical_nc_files(self):
        sfed_local_file_path = self.local_raw_dir / self.sfed_historical
        mfed_local_file_path = self.local_raw_dir / self.mfed_historical

        if sfed_local_file_path.exists() and mfed_local_file_path.exists():
            return sfed_local_file_path, mfed_local_file_path

        # Download historical netcdf files for 1998-2023
        try:
            if download_from_azure(
                blob_service_client=self.blob_service_client,
                container_name=self.container_name,
                blob_path=self.raw_path / self.sfed_historical,
                local_file_path=sfed_local_file_path,
            ) and download_from_azure(
                blob_service_client=self.blob_service_client,
                container_name=self.container_name,
                blob_path=self.raw_path / self.mfed_historical,
                local_file_path=mfed_local_file_path,
            ):
                return sfed_local_file_path, mfed_local_file_path

        except Exception as err:
            self.logger.error(f"Failed downloading: {err}")

        return None

    def _get_90_days_filenames_for_dates(self, dates):
        filenames = []

        if self.mode != "local":
            existing_files = [
                x.name
                for x in blob_client(self.mode)
                .get_container_client(self.container_name)
                .list_blobs(
                    name_starts_with=self.raw_path.as_posix() + "/aer_floodscan"
                )
            ]

        else:
            existing_files = os.listdir(self.local_raw_dir)

        for filename_ in existing_files:
            date_from_file = get_datetime_from_filename(filename_)
            if date_from_file in dates:
                filenames.append(
                    {
                        SFED: self._generate_raw_filename(date_from_file, SFED),
                        MFED: self._generate_raw_filename(date_from_file, MFED),
                    }
                )

        if not filenames:
            filename_ = existing_files[0]
            date_from_file = get_datetime_from_filename(filename_)
            filenames.append(
                {
                    SFED: self._generate_raw_filename(date_from_file, SFED),
                    MFED: self._generate_raw_filename(date_from_file, MFED),
                }
            )

        return filenames

    def get_historical_90days_zipped_files(self, dates):
        filename_list = self._get_90_days_filenames_for_dates(dates=dates)
        zipped_files_path = []

        for filename_ in filename_list:
            sfed_filename = filename_[SFED]
            mfed_filename = filename_[MFED]
            sfed_local_file_path = self.local_raw_dir / sfed_filename
            mfed_local_file_path = self.local_raw_dir / mfed_filename

            if self.mode != "local":
                try:
                    download_from_azure(
                        blob_service_client=self.blob_service_client,
                        container_name=self.container_name,
                        blob_path=self.raw_path / sfed_filename,
                        local_file_path=sfed_local_file_path,
                    )
                    download_from_azure(
                        blob_service_client=self.blob_service_client,
                        container_name=self.container_name,
                        blob_path=self.raw_path / mfed_filename,
                        local_file_path=mfed_local_file_path,
                    )

                    zipped_files_path.append(
                        {SFED: sfed_local_file_path, MFED: mfed_local_file_path}
                    )
                except Exception as err:
                    self.logger.error(f"Failed downloading: {err}")
            else:
                zipped_files_path.append(
                    {SFED: sfed_local_file_path, MFED: mfed_local_file_path}
                )

        return zipped_files_path

    def _unzip_90days_file(self, file_to_unzip, dates):
        unzipped_files = []
        try:
            with ZipFile(file_to_unzip, "r") as zipObj:
                for fileName in zipObj.namelist():
                    if os.path.basename(fileName):
                        date = get_datetime_from_filename(fileName)
                        if date in dates:
                            date_str = re.search("([0-9]{4}[0-9]{2}[0-9]{2})", fileName)
                            new_filename = os.path.basename(
                                fileName.replace(
                                    date_str[0], date.strftime(DATE_FORMAT)
                                )
                            )
                            try:
                                full_path = zipObj.extract(fileName, self.local_raw_dir)
                                new_full_path = os.path.join(
                                    os.path.dirname(full_path), new_filename
                                )
                                os.rename(full_path, new_full_path)
                                unzipped_files.append(
                                    os.path.basename(
                                        shutil.move(new_full_path, self.local_raw_dir)
                                    )
                                )
                            except Exception:
                                self.logger.warning(
                                    f"File already exists : {new_filename}"
                                )

            return unzipped_files
        except Exception as e:
            self.logger.error(f"Failed to extract {fileName}: {e}")

    def process_historical_data(self, filepath, dates, band_type):
        paths = {}

        self.logger.info(f"Processing historical data from {filepath}")
        with xr.open_dataset(filepath) as ds:
            ds = ds.transpose("time", "lat", "lon")
            if not ds["time"].dtype == "<M8[ns]":
                ds["time"] = pd.to_datetime(
                    [pd.Timestamp(t.strftime(DATE_FORMAT)) for t in ds["time"].values]
                )
            for date in dates:
                if date.year < 2024:
                    ds_sel = ds.sel({"time": date})
                    ds_sel = ds_sel.rename({band_type + "_AREA": band_type})
                    da = ds_sel[band_type]
                    self.metadata["units"] = "Flood Fraction"
                    self.metadata["grid_resolution"] = 0.08333
                    self.metadata[
                        "source"
                    ] = "Atmospheric and Environmental Research (AER) FloodScan"
                    self.metadata["product"] = "FloodScan"
                    self.metadata["averaging_period"] = "Daily"

                    da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
                    self.metadata["date_valid"] = date.day
                    self.metadata["year_valid"] = date.year
                    self.metadata["month_valid"] = date.month

                    da = invert_lat_lon(da)
                    da = da.rio.write_crs("EPSG:4326", inplace=False)

                    paths[date] = da

        return paths

    def process_historical_zipped_data(self, zipped_filepaths, dates):
        unzipped_sfed = []
        unzipped_mfed = []
        sfed_das = {}
        mfed_das = {}

        for filepath in zipped_filepaths:
            self.logger.info(
                f"Unzipping data from from {filepath[SFED]} and {filepath[MFED]} to {self.local_raw_dir}"
            )

            try:
                unzipped_sfed += self._unzip_90days_file(filepath[SFED], dates)
                unzipped_mfed += self._unzip_90days_file(filepath[MFED], dates)

                if len(dates) == len(unzipped_sfed):
                    break
            except Exception as err:
                self.logger.error(
                    f"Failed to extract {filepath[SFED]} or {filepath[MFED]}: {err}"
                )

        for file in list(zip(unzipped_sfed, unzipped_mfed)):
            date = get_datetime_from_filename(file[0])
            sfed_das[date] = self.process_data(file[0], band_type=SFED)
            mfed_das[date] = self.process_data(file[1], band_type=MFED)

        for date in dates:
            self.combine_bands(sfed_das, mfed_das, date=date)

    def query_api(self, date):
        today = datetime.today()
        yesterday = today - pd.DateOffset(days=1)

        sfed_raw_filename = self._generate_raw_filename(date, SFED)
        mfed_raw_filename = self._generate_raw_filename(date, MFED)

        # Gets the latest 90 days zip files for SFED and MFED
        if date.date() == yesterday.date():
            self.logger.info(
                f"Downloading data from {date}: {sfed_raw_filename} and {mfed_raw_filename}"
            )

            try:
                sfed_result = requests.get(self.sfed_base_url)
                mfed_result = requests.get(self.mfed_base_url)
                sfed_result.raise_for_status()
                mfed_result.raise_for_status()
            except requests.exceptions.HTTPError as err:
                self.logger.error(f"Failed downloading: {err}")
                return None

            sfed_filepath = self.local_raw_dir / sfed_raw_filename
            with open(sfed_filepath, "wb") as sfed:
                sfed.write(sfed_result.content)

            mfed_filepath = self.local_raw_dir / mfed_raw_filename
            with open(mfed_filepath, "wb") as mfed:
                mfed.write(mfed_result.content)

            # Saving the latest zipped files for SFED and MFED
            self.save_raw_data(sfed_raw_filename)
            self.save_raw_data(mfed_raw_filename)

            # Unzipping and getting geotiffs
            sfed_unzipped, mfed_unzipped = (
                self.get_date_geotiff_from_daily_90_days_file(date, sfed_filepath),
                self.get_date_geotiff_from_daily_90_days_file(date, mfed_filepath),
            )

            # Saving the latest zipped files for SFED and MFED
            self.save_raw_data(sfed_unzipped, SFED)
            self.save_raw_data(mfed_unzipped, MFED)

            return sfed_unzipped, mfed_unzipped

        else:
            self.logger.info(f"Downloading data from our blob for date {date}...")

            sfed_preprocessed_filename = self._generate_processed_filename(date, SFED)
            mfed_preprocessed_filename = self._generate_processed_filename(date, MFED)

            self.get_raw_data_from_blob(sfed_preprocessed_filename, folder=SFED)
            self.get_raw_data_from_blob(mfed_preprocessed_filename, folder=MFED)

            return sfed_preprocessed_filename, mfed_preprocessed_filename

    def process_data(self, filename, band_type, date=None):
        if not date:
            # Infer date from filename:
            date = get_datetime_from_filename(filename)

        raw_file_path = self.local_raw_dir / filename

        with xr.open_dataset(raw_file_path) as ds:
            ds = ds.transpose("band", "y", "x")
            ds_sel = ds.sel({"band": 1}, drop=True)
            ds_sel = ds_sel.rename({"band_data": band_type})
            da = ds_sel[band_type]
            self.metadata["units"] = "Flood Fraction"
            self.metadata["grid_resolution"] = 0.08333
            self.metadata[
                "source"
            ] = "Atmospheric and Environmental Research (AER) FloodScan"
            self.metadata["product"] = "FloodScan"
            self.metadata["averaging_period"] = "Daily"
            self.metadata["date_valid"] = date.day
            self.metadata["year_valid"] = date.year
            self.metadata["month_valid"] = date.month
            da = invert_lat_lon(da)
            da = da.rio.write_crs("EPSG:4326", inplace=False)

            return da

    def combine_bands(self, sfed, mfed, date):
        print("combining bands")

        if sfed and mfed:
            # try:
            da = xr.merge([sfed[date], mfed[date]])
            self.save_processed_data(da, self._generate_processed_filename(date))
            self.logger.info(f"Successfully combined SFED and MFED for: {date}")
            # except Exception:
            #     self.logger.error("Failed when combining sfed and mfed geotiffs.")

    def run_pipeline(self):
        yesterday = datetime.today() - pd.DateOffset(days=1)
        dates = create_date_range(
            self.start_date,
            self.end_date,
            min_accepted=datetime.strptime("1998-01-12", DATE_FORMAT),
            max_accepted=yesterday,
        )

        self.logger.info(f"Running FloodScan pipeline in {self.mode} mode...")

        # Run for the day before if the data is already available
        if self.is_update:
            self.logger.info("Retrieving FloodScan data from yesterday...")
            sfed, mfed = self.get_raw_data(date=yesterday)
            self.process_data(sfed, date=yesterday)
            self.process_data(mfed, date=yesterday)

        # Run using historical archived data
        elif self.is_full_historical_run:
            # If any of the dates are below 2024:
            if any(date.year < 2024 for date in dates):
                self.logger.info(
                    f"Retrieving historical FloodScan data from {min(dates).date()} until {max(dates).date()}..."
                )

                # Dates fall under netcdf archive
                sfed_path, mfed_path = self.get_historical_nc_files()

                sfed_das = self.process_historical_data(sfed_path, dates, SFED)
                mfed_das = self.process_historical_data(mfed_path, dates, MFED)

                for date in dates:
                    self.combine_bands(sfed_das, mfed_das, date=date)

            # If any of the dates are above 2023:
            if any(date.year >= 2024 for date in dates):
                filenames = self.get_historical_90days_zipped_files(dates=dates)
                filenames.reverse()
                self.process_historical_zipped_data(filenames, dates)

        # Run for a specific date range using geotiffs
        else:
            self.logger.info(
                f"Retrieving FloodScan data from {self.start_date.date()} to {self.end_date.date()}..."
            )
            for date in dates:
                sfed_path, mfed_path = self.get_raw_data(date=date)
                self.combine_bands(
                    sfed=self.process_data(sfed_path, date=date),
                    mfed=self.process_data(mfed_path, date=date),
                    date=date,
                )

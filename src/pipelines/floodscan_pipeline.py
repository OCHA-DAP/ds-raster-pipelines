import os
import re
import shutil
from datetime import datetime
from fileinput import filename
from zipfile import ZipFile

import pandas as pd
import requests
import xarray as xr

from ..utils.azure_utils import download_from_azure, blob_client
from ..utils.raster_utils import invert_lat_lon, create_date_range
from .pipeline import Pipeline


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

        self.start_date = datetime.strptime(kwargs["start_date"], "%Y%m%d")
        self.end_date = datetime.strptime(kwargs["end_date"], "%Y%m%d")
        self.is_update = kwargs["is_update"]
        self.is_full_historical_run = kwargs["is_full_historical_run"]
        self.version = kwargs["version"]
        self.sfed_historical = kwargs["sfed_historical"]
        self.mfed_historical = kwargs["mfed_historical"]
        self.sfed_base_url = os.getenv("FLOODSCAN_SFED_URL")
        self.mfed_base_url = os.getenv("FLOODSCAN_MFED_URL")


    def _generate_raw_filename(self, date, type):
        return f"aer_floodscan_{type}_area_flooded_fraction_africa_90days_{date.strftime('%Y%m%d')}.zip"

    def _generate_unzipped_filename(self, date, type):
        return f"aer_{type}_area_300s_{date.strftime('%Y%m%d')}_v0{self.version}r01.tif"

    def _generate_processed_filename(self, date):
        return f"aer_area_300s_{date.strftime('%Y%m%d')}_v0{self.version}r01.tif"

    def get_date_geotiff_from_daily_90_days_file(self, date, filepath):

        with ZipFile(filepath, 'r') as zipObj:
            listOfFileNames = zipObj.namelist()
            self.logger.info(f"Most recent geotiff in this file is: {max(listOfFileNames)}")
            for fileName in listOfFileNames:
                if fileName.endswith(f"{date.strftime('%Y%m%d')}_v0{self.version}r01.tif"):
                    try:
                        full_path = zipObj.extract(fileName, os.path.dirname(filepath))
                        tif_filename = os.path.basename(shutil.move(full_path, os.path.dirname(filepath)))
                        return tif_filename
                    except Exception as e:
                        self.logger.info(f"Failed to extract {filename()}: {e}")

        raise ValueError(f"No filename match for the date: {date.strftime('%Y%m%d')}. ")


    def get_historical_nc_files(self):

        sfed_local_file_path = self.local_raw_dir / self.sfed_historical
        mfed_local_file_path = self.local_raw_dir / self.mfed_historical

        if sfed_local_file_path.exists() and mfed_local_file_path.exists():
            return sfed_local_file_path, mfed_local_file_path

        # Download historical netcdf files for 1998-2023
        try:
            return (download_from_azure(
                    blob_service_client=self.blob_service_client,
                    container_name=self.container_name,
                    blob_path=self.raw_path / self.sfed_historical,
                    local_file_path=sfed_local_file_path),
                    download_from_azure(
                    blob_service_client=self.blob_service_client,
                    container_name=self.container_name,
                    blob_path=self.raw_path / self.mfed_historical,
                    local_file_path=mfed_local_file_path))
        except Exception as err:
            self.logger.error(f"Failed downloading: {err}")

        return None


    def _get_90_days_filenames_for_dates(self, dates):
        filenames = []

        existing_files = [
            x.name
            for x in blob_client(self.mode).get_container_client(self.container_name).list_blobs(
                name_starts_with=self.raw_path.as_posix()+"/aer_floodscan"
            )
        ]

        for filename in existing_files:
            date_from_file = self._get_datetime_from_filename(filename)
            filenames.append({"SFED" : self._generate_raw_filename(date_from_file, "sfed"),
                              "MFED" : self._generate_raw_filename(date_from_file, "mfed")})
            if date_from_file > max(dates):
                return filenames

        return filenames

    def get_historical_90days_zipped_files(self, dates):


        filename_list = self._get_90_days_filenames_for_dates(dates=dates)
        zipped_files_path = []

        for filename in filename_list:
            sfed_filename = filename["SFED"]
            mfed_filename = filename["MFED"]
            sfed_local_file_path = self.local_raw_dir / sfed_filename
            mfed_local_file_path = self.local_raw_dir / mfed_filename

            try:
                download_from_azure(
                    blob_service_client=self.blob_service_client,
                    container_name=self.container_name,
                    blob_path=self.raw_path / sfed_filename,
                    local_file_path=sfed_local_file_path)
                download_from_azure(
                    blob_service_client=self.blob_service_client,
                    container_name=self.container_name,
                    blob_path=self.raw_path / mfed_filename,
                    local_file_path=mfed_local_file_path)

                zipped_files_path.append({"SFED" : sfed_local_file_path, "MFED" : mfed_local_file_path})
            except Exception as err:
                self.logger.error(f"Failed downloading: {err}")

        return zipped_files_path

    def _get_datetime_from_filename(self, filename):
        try:
            return datetime.strptime(re.search("([0-9]{4}[0-9]{2}[0-9]{2})", filename)[0], "%Y%m%d")
        except Exception as err:
            self.logger.error(f"Cannot get datetime from {filename}: {err}")

    def _unzip_90days_file(self, file_to_unzip, dates):

        unzipped_files = []
        try:
            with ZipFile(file_to_unzip, 'r') as zipObj:
                for fileName in zipObj.namelist():
                    if os.path.basename(fileName):
                        date = self._get_datetime_from_filename(fileName)
                        if date in dates:
                                full_path = zipObj.extract(fileName, self.local_raw_dir)
                                unzipped_files.append(os.path.basename(shutil.move(full_path, self.local_raw_dir)))

            return unzipped_files
        except Exception as e:
            self.logger.error(f"Failed to extract {fileName}: {e}")


    def process_historical_data(self, filepath, dates, band_type):

        with xr.open_dataset(filepath, engine="netcdf4") as ds:
            ds = ds.transpose("time", "lat", "lon")
            if not ds["time"].dtype == "<M8[ns]":
                ds["time"] = pd.to_datetime(
                    [pd.Timestamp(t.strftime("%Y-%m-%d")) for t in ds["time"].values]
                )
            for date in dates:
                if date.year < 2024:
                    ds_sel = ds.sel({"time": date})
                    da = ds_sel[band_type+"_AREA"]
                    self.metadata["units"] = "Flood Fraction"
                    self.metadata["grid_resolution"] = 0.08333
                    self.metadata["source"] = "Atmospheric and Environmental Research (AER) FloodScan"
                    self.metadata["product"] = "FloodScan"
                    self.metadata["averaging_period"] = "Daily"

                    da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
                    self.metadata["date_valid"] = date.day
                    self.metadata["year_valid"] = date.year
                    self.metadata["month_valid"] = date.month

                    da = invert_lat_lon(da)
                    da = da.rio.write_crs("EPSG:4326", inplace=False)

                    filename = self._generate_processed_filename(date)

                    self.logger.info(
                        f"Saving processed data {filename}...")

                    self.save_processed_data(da, filename, band_type)

        return filename

    def process_historical_zipped_data(self, zipped_filepaths, dates):

        unzipped_sfed = []
        unzipped_mfed = []

        for filepath in zipped_filepaths:

            self.logger.info(f"Unzipping data from from {filepath['SFED']} and {filepath['MFED']} to {self.local_raw_dir}")

            try:
                unzipped_sfed += self._unzip_90days_file(filepath["SFED"], dates)
                unzipped_mfed += self._unzip_90days_file(filepath["MFED"], dates)
            except Exception as err:
                self.logger.error(f"Failed to extract {filepath['SFED']} or {filepath['MFED']}: {err}")


        for file in list(zip(unzipped_sfed, unzipped_mfed)):
            self.process_data(file[0], band_type="SFED")
            self.process_data(file[1], band_type="MFED")


    def query_api(self, date):

        today = datetime.today()
        yesterday = today - pd.DateOffset(days=1)

        sfed_raw_filename = self._generate_raw_filename(date, "sfed")
        mfed_raw_filename = self._generate_raw_filename(date, "mfed")

        # Gets the latest 90 days zip files for SFED and MFED
        if date.date() == yesterday.date():
            self.logger.info(f"Downloading data from {date}: {sfed_raw_filename} and {mfed_raw_filename}")

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
            sfed_unzipped, mfed_unzipped = (self.get_date_geotiff_from_daily_90_days_file(date, sfed_filepath),
             self.get_date_geotiff_from_daily_90_days_file(date, mfed_filepath))

            # Saving the latest zipped files for SFED and MFED
            self.save_raw_data(sfed_unzipped, "SFED")
            self.save_raw_data(mfed_unzipped, "MFED")

            return sfed_unzipped, mfed_unzipped

        else:
            self.logger.info(f"Downloading data from our blob for date {date}...")

            sfed_preprocessed_filename = self._generate_processed_filename(date)
            mfed_preprocessed_filename = self._generate_processed_filename(date)

            self.get_raw_data_from_blob(sfed_preprocessed_filename, folder="SFED")
            self.get_raw_data_from_blob(mfed_preprocessed_filename, folder="MFED")

            return sfed_preprocessed_filename, mfed_preprocessed_filename


    def process_data(self, filename, band_type, date=None):

        if not date:
            # Infer date from filename:
            date = self._get_datetime_from_filename(filename)

        raw_file_path = self.local_raw_dir /  self._generate_unzipped_filename(date, band_type)

        with xr.open_dataset(raw_file_path, engine="netcdf4") as ds:
            ds = ds.transpose("band", "y", "x")
            ds = ds.rename({"band_data": "SFED"})
            da = ds["SFED"]
            self.metadata["units"] = "Flood Fraction"
            self.metadata["grid_resolution"] = 0.08333
            self.metadata["source"] = "Atmospheric and Environmental Research (AER) FloodScan"
            self.metadata["product"] = "FloodScan"
            self.metadata["averaging_period"] = "Daily"
            self.metadata["year_valid"] = date.year
            self.metadata["month_valid"] = date.month
            da.rename({"band": band_type})
            da = invert_lat_lon(da)
            da = da.rio.write_crs("EPSG:4326", inplace=False)
            self.save_processed_data(da, filename, band_type)


    def run_pipeline(self):
        yesterday = datetime.today() - pd.DateOffset(days=1)
        dates = create_date_range(self.start_date,
                                  self.end_date,
                                  min_accepted=datetime.strptime("19980112", "%Y%m%d"),
                                  max_accepted=yesterday)

        self.logger.info(f"Running FloodScan pipeline in {self.mode} mode...")

        # Run for the day before if the data is already available
        if self.is_update:
            self.logger.info("Retrieving FloodScan data from yesterday...")
            sfed, mfed = self.get_raw_data(date=yesterday)
            self.process_data(sfed, date=yesterday)
            self.process_data(mfed, date=yesterday)

        # Run using historical archived data
        # todo change this to just pick up from date?
        elif self.is_full_historical_run:

            # If any of the dates are below 2024:
            if any(date.year < 2024 for date in dates):
                self.logger.info(
                f"Retrieving historical FloodScan data from {min(dates).date()} until {max(dates).date()}...")

                # Dates fall under netcdf archive
                sfed_path, mfed_path = self.get_historical_nc_files()
                self.process_historical_data(sfed_path, dates, "SFED")
                self.process_historical_data(mfed_path, dates, "MFED")

            # If any of the dates are above 2023:
            if any(date.year >= 2024 for date in dates):
                dates = create_date_range(datetime.strptime("20240101", "%Y%m%d"),
                                          self.end_date)

                filenames = self.get_historical_90days_zipped_files(dates=dates)
                self.process_historical_zipped_data(filenames, dates)

        # Run for a specific date range using geotiffs
        else:
            self.logger.info(
                f"Retrieving FloodScan data from {self.start_date.date()} to {self.end_date.date()}..."
            )
            for date in dates:
                sfed, mfed = self.get_raw_data(date=date)
                self.process_data(sfed, date=date)
                self.process_data(mfed, date=date)

"""
Structure:    
floodscan
        v5
            raw
                historical sfed nc 
                historical mfed nc 
                daily 90 sfed
                daily 90 mfed               
            processed
                 SFED
                    daily geotiffs
                MFED
                    daily geotiffs
"""


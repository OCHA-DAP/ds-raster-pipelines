import os
import shutil
from datetime import datetime
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
import xarray as xr
from ..utils.raster_utils import invert_lat_lon
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

        self.start_date = kwargs["start_date"]
        self.end_date = kwargs["end_date"]
        self.is_update = kwargs["is_update"]
        self.is_full_historical_run = kwargs["is_full_historical_run"]
        self.version = kwargs["version"]
        self.sfed_historical = kwargs["sfed_historical"]
        self.mfed_historical = kwargs["mfed_historical"]
        self.sfed_base_url = kwargs["sfed_base_url"]
        self.mfed_base_url = kwargs["mfed_base_url"]


    def _generate_raw_filename(self, date, type):
        return f"aer_floodscan_{type}_area_flooded_fraction_africa_90days_{date.strftime('%Y%m%d')}.zip"

    def _generate_processed_filename(self, date):
        return f"aer_area_300s_{date.strftime('%Y%m%d')}_v0{self.version}r01.tif"

    def get_date_geotiff_from_daily_90_days_file(self, date, filepath):

        with ZipFile(filepath, 'r') as zipObj:
            listOfFileNames = zipObj.namelist()
            self.logger.info(f"Most recent geotiff in this file is: {max(listOfFileNames)}")
            for fileName in listOfFileNames:
                if fileName.endswith(f"{date.strftime('%Y%m%d')}_v0{self.version}r01.tif"):
                    full_path = zipObj.extract(fileName, os.path.dirname(filepath))
                    tif_filename = os.path.basename(shutil.move(full_path, os.path.dirname(filepath)))
                    return tif_filename

        raise ValueError(f"No filename match for the date: {date.strftime('%Y%m%d')}. ")


    def get_historical_data(self):
        raise NotImplementedError()


    def process_historical_data(self):
        raise NotImplementedError()


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

            self.get_raw_data_from_blob(sfed_raw_filename)
            self.get_raw_data_from_blob(mfed_raw_filename)

            return (self.get_date_geotiff_from_daily_90_days_file(date, sfed_raw_filename),
                    self.get_date_geotiff_from_daily_90_days_file(date, mfed_raw_filename))


    def process_data(self, filenames, date):

        processed_filename = self._generate_processed_filename(date)

        sfed_filename, mfed_filename = filenames
        sfed_raw_file_path = self.local_raw_dir / sfed_filename
        mfed_raw_file_path = self.local_raw_dir / mfed_filename
        sfed_ds = xr.open_dataset(sfed_raw_file_path)
        mfed_ds = xr.open_dataset(mfed_raw_file_path)
        mfed_ds = mfed_ds.assign_coords(band=("band", np.array([2])))

        merged_ds = xr.concat(
            [
                sfed_ds,
                mfed_ds,
            ], dim='band'
        )
        merged_ds.band_data.attrs["long_name"] = ("SFED", "MFED")

        ds = merged_ds.transpose("band", "y", "x")
        da = ds["band_data"]
        self.metadata["date_valid"] = date.day
        self.metadata["month_valid"] = date.month
        self.metadata["year_valid"] = date.year
        da = invert_lat_lon(da)
        da = da.rio.write_crs("EPSG:4326", inplace=False)
        self.save_processed_data(da, processed_filename)


    def run_pipeline(self):
        today = datetime.today()
        yesterday = today - pd.DateOffset(days=1)

        self.logger.info(f"Running FloodScan pipeline in {self.mode} mode...")
        if self.is_update:
            self.logger.info("Retrieving FloodScan data from yesterday...")
            raw_filenames = self.get_raw_data(date=yesterday)
            self.process_data(raw_filenames, date=yesterday)
        elif self.is_full_historical_run:
            self.logger.info("Retrieving historical FloodScan data from 1998 until today...")
            raw_filenames = self.get_historical_data()
            self.process_historical_data(raw_filenames)
        else:
            self.logger.info(
                f"Retrieving FloodScan data from {self.start_date} to {self.end_date}..."
            )
            for date in range(self.start_date, self.end_date + 1):
                raw_filenames = self.get_raw_data(date=date)
                self.process_data(raw_filenames, date=date)



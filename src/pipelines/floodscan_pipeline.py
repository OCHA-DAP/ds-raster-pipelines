import os
import re
import shutil
from datetime import datetime, timedelta
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
            coverage=kwargs["coverage"],
            use_cache=kwargs["use_cache"],
        )

        self.backfill = kwargs["backfill"]
        self.start_date = datetime.strptime(kwargs["start_date"], DATE_FORMAT)
        self.end_date = datetime.strptime(kwargs["end_date"], DATE_FORMAT)
        self.is_update = kwargs["is_update"]
        self.version = kwargs["version"]
        self.sfed_historical = kwargs["sfed_historical"]
        self.mfed_historical = kwargs["mfed_historical"]
        self.sfed_base_url = os.getenv("FLOODSCAN_SFED_URL")
        self.mfed_base_url = os.getenv("FLOODSCAN_MFED_URL")

    def _generate_raw_filename(self, date, type):
        return f"aer_floodscan_{type.lower()}_area_flooded_fraction_africa_90days_{date.strftime(DATE_FORMAT)}.zip"

    def _generate_processed_filename(self, date):
        return f"aer_area_300s_v{date.strftime(DATE_FORMAT)}_v0{self.version}r01.tif"

    def get_most_recent_geotiff_from_daily_90_days_file(self, filepath):
        with ZipFile(filepath, "r") as zipobj:
            filenames = zipobj.namelist()
            latest = max(filenames)
            self.logger.info(f"Most recent geotiff in this file is: {latest}")
            try:
                full_path = zipobj.extract(latest, os.path.dirname(filepath))
                tif_filename = os.path.basename(
                    shutil.move(full_path, os.path.dirname(filepath))
                )
                return tif_filename
            except Exception as e:
                self.logger.info(f"Failed to extract {latest}: {e}")

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

    def _get_dates_with_pairs(self, filenames):
        pairs = {}
        for fname in filenames:
            date = get_datetime_from_filename(fname)
            file_type = "sfed" if "sfed" in fname else "mfed"
            pairs.setdefault(date, set()).add(file_type)
        return sorted([date for date, types in pairs.items() if len(types) == 2])

    def _get_90_days_filenames_for_dates(self, dates, max_days=5):
        # --- Check existing files
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
            existing_files = [
                f
                for f in os.listdir(self.local_raw_dir)
                if f.startswith("aer_floodscan")
            ]
        available_dates = self._get_dates_with_pairs(existing_files)
        print(f"The following dates have zipped data: {available_dates}")

        filenames = []
        for target in dates:
            # First check if exact date exists
            if target in available_dates:
                filenames.append(
                    {
                        SFED: self._generate_raw_filename(target, SFED),
                        MFED: self._generate_raw_filename(target, MFED),
                    }
                )
                continue

            # Check next 5 days
            found = False
            for days in range(1, max_days + 1):
                future_date = target + timedelta(days=days)
                if future_date in available_dates:
                    filenames.append(
                        {
                            SFED: self._generate_raw_filename(future_date, SFED),
                            MFED: self._generate_raw_filename(future_date, MFED),
                        }
                    )
                    found = True
                    break

            if not found:
                self.logger.warning(
                    f"No available data within {max_days} days of {target}"
                )

        # Now drop for any duplicates
        filenames = list({tuple(sorted(d.items())): d for d in filenames}.values())
        return filenames

    def get_historical_90days_zipped_files(self, dates):
        """
        Retrieves the appropriate zipped files, returning the full path to where they've been downloaded
        (Or where they're located if working locally)
        """
        filename_list = self._get_90_days_filenames_for_dates(dates=dates)
        zipped_files_path = []

        for zipped_filename in filename_list:
            sfed_filename = zipped_filename[SFED]
            mfed_filename = zipped_filename[MFED]
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
        with ZipFile(file_to_unzip, "r") as zipobj:
            for filename in zipobj.namelist():
                if os.path.basename(filename):
                    date = get_datetime_from_filename(filename)
                    if date in dates:
                        date_str = re.search("([0-9]{4}[0-9]{2}[0-9]{2})", filename)
                        new_filename = os.path.basename(
                            filename.replace(date_str[0], date.strftime(DATE_FORMAT))
                        )
                        try:
                            full_path = zipobj.extract(filename, self.local_raw_dir)
                            new_full_path = os.path.join(
                                os.path.dirname(full_path), new_filename
                            )
                            os.rename(full_path, new_full_path)
                            unzipped_files.append(
                                os.path.basename(
                                    shutil.move(new_full_path, self.local_raw_dir)
                                )
                            )
                        except FileExistsError:
                            self.logger.warning(f"File already exists: {new_filename}")
                        # except Exception as e:
                        #     self.logger.error(f" ** Failed to extract: {e}")

        return unzipped_files
        # except Exception as e:
        #     self.logger.error(f"-- Failed to extract: {e}")

    def process_historical_zipped_data(self, zipped_filepaths, dates):
        unzipped_sfed = []
        unzipped_mfed = []

        for filepath in zipped_filepaths:
            self.logger.debug(
                f"Unzipping data from from {filepath[SFED]} and {filepath[MFED]} to {self.local_raw_dir}"
            )

            # try:
            unzipped_sfed += self._unzip_90days_file(filepath[SFED], dates)
            unzipped_mfed += self._unzip_90days_file(filepath[MFED], dates)

            if len(dates) == len(unzipped_sfed):
                break
            # except Exception as err:
            #     print(err)
            #     self.logger.error(
            #         f"Failed to extract {filepath[SFED]} or {filepath[MFED]}: {err}"
            #     )

        unzipped_files = list(zip(unzipped_sfed, unzipped_mfed))

        for file in unzipped_files:
            date = get_datetime_from_filename(file[0])
            self.logger.info(f"Processing historical {SFED} data from {date}")
            sfed_da = self.process_data(file[0], band_type=SFED)

            self.logger.info(f"Processing historical {MFED} data from {date}")
            mfed_da = self.process_data(file[1], band_type=MFED)
            self._combine_bands(sfed_da, mfed_da, date=date)

        self._cleanup_local()

    def _cleanup_local(self):
        """Cleans up everything in the local directory that isn't a 90-day zip or a historical .nc file"""
        if self.mode == "local":
            for file in os.listdir(self.local_raw_dir):
                file_path = self.local_raw_dir / file
                if file_path.is_file() and not (
                    file.endswith(".zip") or file.endswith(".nc")
                ):
                    os.remove(file_path)
            for item in os.listdir(self.local_raw_dir):
                item_path = self.local_raw_dir / item
                if item_path.is_dir():
                    shutil.rmtree(item_path)

    def _update_name_if_necessary(self, raw_filename, band_type, latest_date):
        filename_date = get_datetime_from_filename(str(raw_filename))
        if filename_date != latest_date:
            new_filename = self.local_raw_dir / self._generate_raw_filename(
                latest_date, band_type
            )
            os.rename(raw_filename, new_filename)
            return new_filename
        else:
            return self.local_raw_dir / raw_filename

    def query_api(self, date):
        today = datetime.today()
        yesterday = today - pd.DateOffset(days=1)

        sfed_raw_filename = self._generate_raw_filename(date, SFED)
        mfed_raw_filename = self._generate_raw_filename(date, MFED)

        # Gets the latest 90 days zip files for SFED and MFED
        if date.date() == yesterday.date():
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

            sfed_unzipped, mfed_unzipped = (
                self.get_most_recent_geotiff_from_daily_90_days_file(sfed_filepath),
                self.get_most_recent_geotiff_from_daily_90_days_file(mfed_filepath),
            )
            latest_date = get_datetime_from_filename(sfed_unzipped)

            sfed_raw_path = self._update_name_if_necessary(
                sfed_filepath, SFED, latest_date
            )
            mfed_raw_path = self._update_name_if_necessary(
                mfed_filepath, MFED, latest_date
            )

            # Saving the latest zipped files for SFED and MFED
            self.save_raw_data(os.path.basename(sfed_raw_path))
            self.save_raw_data(os.path.basename(mfed_raw_path))

            return sfed_unzipped, mfed_unzipped, latest_date

    def process_historical_data(self, filepath, date, band_type):
        self.logger.info(f"Processing historical {band_type} data from {date}")

        with xr.open_dataset(filepath) as ds:
            ds = ds.transpose("time", "lat", "lon")
            if not ds["time"].dtype == "<M8[ns]":
                ds["time"] = pd.to_datetime(
                    [pd.Timestamp(t.strftime(DATE_FORMAT)) for t in ds["time"].values]
                )

            ds_sel = ds.sel({"time": date})
            ds_sel = ds_sel.rename({band_type + "_AREA": band_type})
            da = ds_sel[band_type]
            da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)

            self.metadata["date_valid"] = date.day
            self.metadata["year_valid"] = date.year
            self.metadata["month_valid"] = date.month

            da.attrs = self.metadata
            da = invert_lat_lon(da)
            da = da.rio.write_crs("EPSG:4326", inplace=False)

        return da

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
            self.metadata["date_valid"] = date.day
            self.metadata["year_valid"] = date.year
            self.metadata["month_valid"] = date.month
            da.attrs = self.metadata
            da = invert_lat_lon(da)
            da = da.rio.write_crs("EPSG:4326", inplace=False)

            return da

    def _combine_bands(self, sfed, mfed, date):
        if sfed is not None and mfed is not None:
            try:
                da = xr.merge([sfed, mfed])
                self.save_processed_data(da, self._generate_processed_filename(date))
                self.logger.debug(f"Successfully combined SFED and MFED for: {date}")
            except Exception as err:
                self.logger.error(
                    f"Failed when combining sfed and mfed geotiffs. {err}"
                )

    # ------------------ Processing runner functions

    def _process_pre_2024(self, dates):
        """Process any dates before 2024. These will pull from the large netcdf files."""
        self.logger.debug(f"Processing {len(dates)} dates from before 2024...")
        sfed_path, mfed_path = self.get_historical_nc_files()
        for date in dates:
            sfed_da = self.process_historical_data(sfed_path, date, SFED)
            mfed_da = self.process_historical_data(mfed_path, date, MFED)
            self._combine_bands(sfed_da, mfed_da, date=date)

    def _process_post_2024(self, dates):
        """Process any dates 2024 onwards. This will pull from the 90 days zipped files."""
        self.logger.debug(f"Processing {len(dates)} dates from 2024 onwards...")
        filenames = self.get_historical_90days_zipped_files(dates=dates)
        filenames.reverse()
        self.process_historical_zipped_data(filenames, dates)

    def process_historical_dates(self, dates):
        """
        Process data for a historical date range.
        Dates before 2024 will be handled differently
        """
        post_2024_dates = [date for date in dates if date.year >= 2024]
        pre_2024_dates = [date for date in dates if date.year < 2024]

        if post_2024_dates:
            self._process_post_2024(post_2024_dates)
        if pre_2024_dates:
            self._process_pre_2024(pre_2024_dates)

    def process_latest_update(self):
        """Retrieve yesterday's data"""
        self.logger.info("Retrieving Floodscan data from yesterday...")
        yesterday = datetime.today() - pd.DateOffset(days=1)
        sfed, mfed, latest_date = self.get_raw_data(date=yesterday)
        sfed_da = self.process_data(sfed, band_type=SFED)
        mdfed_da = self.process_data(mfed, band_type=MFED)
        self._combine_bands(sfed_da, mdfed_da, latest_date)
        return True

    def run_pipeline(self):
        self.logger.info(f"Running FloodScan pipeline in {self.mode} mode...")

        # Backfilling dates
        if self.backfill:
            self.logger.info("Checking for missing data and backfilling if needed...")
            missing_dates, _ = self.check_coverage()
            self.print_coverage_report()
            if missing_dates:
                self.process_historical_dates(missing_dates)

        # Run for the latest available date
        if self.is_update:
            self.process_latest_update()

        # Historical run
        else:
            yesterday = datetime.today() - pd.DateOffset(days=1)
            min_date = datetime.strptime(str(self.coverage["start_date"]), DATE_FORMAT)
            self.logger.info(
                f"Processing historical data from {self.start_date} to {self.end_date}..."
            )
            dates = create_date_range(
                self.start_date,
                self.end_date,
                min_accepted=min_date,
                max_accepted=yesterday,
            )
            self.process_historical_dates(dates)

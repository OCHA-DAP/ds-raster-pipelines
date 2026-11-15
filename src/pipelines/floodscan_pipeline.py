import os
import re
import shutil
from datetime import datetime, timedelta
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
import rioxarray as rxr
from rioxarray.merge import merge_datasets
import xarray as xr

from ..utils.azure_utils import blob_client, download_from_azure
from ..utils.date_utils import (
    DATE_FORMAT,
    create_date_range,
    get_datetime_from_filename,
)
from ..utils.raster_utils import invert_lat_lon
from .pipeline import Pipeline

REGIONS = ["africa", "asia-australia", "north-america", "south-america"]
REGION_SUFFIX = {"africa": "AF", "asia-australia": "AA", "north-america": "NA", "south-america": "SA"}

SFED = "SFED"
MFED = "MFED"

# Each 90-day zip covers the 90 days up to and including the date in its filename
DAYS_IN_90_DAYS_FILE = 90


def _as_date(value):
    """datetime -> date, so dates from different sources can be compared."""
    return value.date() if isinstance(value, datetime) else value


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

        self.start_date = datetime.strptime(kwargs["start_date"], DATE_FORMAT)
        self.end_date = datetime.strptime(kwargs["end_date"], DATE_FORMAT)
        self.is_update = kwargs["is_update"]
        self.backfill = kwargs["backfill"]
        self.baseline_update = kwargs["baseline_update"]
        self.version = kwargs["version"]
        self.sfed_historical = kwargs["sfed_historical"]
        self.mfed_historical = kwargs["mfed_historical"]

    def _generate_raw_filename(self, date, type, region):
        return f"aer_floodscan_{type.lower()}_area_flooded_fraction_{region}_90days_{date.strftime(DATE_FORMAT)}.zip"

    def _generate_processed_filename(self, date):
        return f"aer_area_300s_v{date.strftime(DATE_FORMAT)}_v0{self.version}r01.tif"

    def _generate_baseline_filename(self, date):
        return f"baseline_v{date.strftime(DATE_FORMAT)}_v0{self.version}r01.nc4"

    def get_geotiff_from_daily_90_days_file(self, filepath, date, region):
        with ZipFile(filepath, "r") as zipobj:
            filenames = zipobj.namelist()
            latest_date = get_datetime_from_filename(max(filenames))
            for file in filenames:
                if file.endswith(".tif"):
                    file_date = get_datetime_from_filename(file).date()
                    if file_date == date:
                        try:
                            full_path = zipobj.extract(file, os.path.dirname(filepath))
                            return os.path.relpath(full_path, self.local_raw_dir), latest_date
                        except Exception as e:
                            self.logger.info(f"Failed to extract {file}: {e}")
                        break
            self.logger.warning(f"Geotiff from {date} not present for {region}. Latest date available is {latest_date.date()}.")
            return None, latest_date

    def get_historical_end_date(self, region):
        end_dates = []
        for filename in (self.sfed_historical[region], self.mfed_historical[region]):
            match = re.search(r"_[0-9]{8}_([0-9]{8})_", filename)
            if not match:
                raise ValueError(f"Cannot get the end date from historical file {filename}")
            end_dates.append(datetime.strptime(match[1], "%Y%m%d"))
        return min(end_dates)

    def get_historical_nc_files(self, region):
        regional_sfed_historical = self.sfed_historical[region]
        regional_mfed_historical = self.mfed_historical[region]

        sfed_local_file_path = self.local_raw_dir / regional_sfed_historical
        mfed_local_file_path = self.local_raw_dir / regional_mfed_historical

        if sfed_local_file_path.exists() and mfed_local_file_path.exists():
            return sfed_local_file_path, mfed_local_file_path

        # Download historical netcdf files for 1998-2023
        try:
            if download_from_azure(
                blob_service_client=self.blob_service_client,
                container_name=self.container_name,
                blob_path=self.raw_path / regional_sfed_historical,
                local_file_path=sfed_local_file_path,
            ) and download_from_azure(
                blob_service_client=self.blob_service_client,
                container_name=self.container_name,
                blob_path=self.raw_path / regional_mfed_historical,
                local_file_path=mfed_local_file_path,
            ):
                return sfed_local_file_path, mfed_local_file_path

        except Exception as err:
            self.logger.error(f"Failed downloading: {err}")

        return None

    def _list_90_days_zips(self, band_type, region):
        prefix = f"aer_floodscan_{band_type.lower()}_area_flooded_fraction_{region}_90days_"
        if self.mode != "local":
            names = [
                os.path.basename(x.name)
                for x in blob_client(self.mode)
                .get_container_client(self.container_name)
                .list_blobs(name_starts_with=f"{self.raw_path.as_posix()}/{prefix}")
            ]
        else:
            names = os.listdir(self.local_raw_dir)
        # Skips anything else in the folder, e.g. extracted zip folders in local mode
        return [name for name in names if name.startswith(prefix) and name.endswith(".zip")]

    def _get_90_days_filenames_for_dates(self, dates, region):
        wanted = {_as_date(date) for date in dates}
        mfed_zips = set(self._list_90_days_zips(MFED, region))

        filenames = []
        for sfed_filename in sorted(self._list_90_days_zips(SFED, region)):
            zip_date = get_datetime_from_filename(sfed_filename).date()
            window_start = zip_date - timedelta(days=DAYS_IN_90_DAYS_FILE - 1)
            if not any(window_start <= date <= zip_date for date in wanted):
                continue
            mfed_filename = self._generate_raw_filename(zip_date, MFED, region)
            if mfed_filename not in mfed_zips:
                self.logger.warning(f"No matching {MFED} zip for {sfed_filename}, skipping it")
                continue
            filenames.append({SFED: sfed_filename, MFED: mfed_filename})

        if not filenames:
            self.logger.error(f"No archived 90-day zips for {region} cover {min(wanted)} to {max(wanted)}")
        return filenames

    def get_historical_90days_zipped_files(self, dates, region):
        filename_list = self._get_90_days_filenames_for_dates(dates=dates, region=region)
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
        wanted = {_as_date(date) for date in dates}
        unzipped_files = {}
        try:
            with ZipFile(file_to_unzip, "r") as zipobj:
                for filename in zipobj.namelist():
                    if not filename.endswith(".tif"):
                        continue
                    date = get_datetime_from_filename(os.path.basename(filename)).date()
                    if date in wanted:
                        full_path = zipobj.extract(filename, self.local_raw_dir)
                        unzipped_files[date] = os.path.relpath(full_path, self.local_raw_dir)
        except Exception as e:
            self.logger.error(f"Failed to extract {file_to_unzip}: {e}")
        return unzipped_files

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

    def get_historical_zipped_files_for_region(self, dates, region):
        wanted = {_as_date(date) for date in dates}
        files = {}

        for filepath in self.get_historical_90days_zipped_files(dates=dates, region=region):
            remaining = wanted - files.keys()
            if not remaining:
                break
            self.logger.info(f"Unzipping {filepath[SFED]} and {filepath[MFED]} to {self.local_raw_dir}")
            sfed_files = self._unzip_90days_file(filepath[SFED], remaining)
            mfed_files = self._unzip_90days_file(filepath[MFED], remaining)
            # Pair the bands by date, so SFED and MFED always come from the same day
            for date in sfed_files.keys() & mfed_files.keys():
                files[date] = (sfed_files[date], mfed_files[date])

        missing = wanted - files.keys()
        if missing:
            self.logger.warning(
                f"No 90-day zip data for {region} on {len(missing)} date(s) between {min(missing)} and {max(missing)}"
            )
        return files

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

    def _update_name_if_necessary(self, raw_filename, band_type, latest_date, region):
        filename_date = get_datetime_from_filename(str(raw_filename))
        if filename_date != latest_date:
            new_filename = self.local_raw_dir / self._generate_raw_filename(
                latest_date, band_type, region
            )
            os.rename(raw_filename, new_filename)
            return new_filename
        else:
            return self.local_raw_dir / raw_filename

    def query_api(self, date, region):
        yesterday = datetime.today() - pd.DateOffset(days=1)
        sfed_raw_filename = self._generate_raw_filename(yesterday, SFED, region)
        mfed_raw_filename = self._generate_raw_filename(yesterday, MFED, region)
        sfed_url, mfed_url = self.get_sfed_and_mfed_urls_for_region(region)
        if sfed_url is None or mfed_url is None:
            return None, None

        try:
            sfed_result = requests.get(sfed_url)
            mfed_result = requests.get(mfed_url)
            sfed_result.raise_for_status()
            mfed_result.raise_for_status()
        except requests.exceptions.RequestException as err:
            self.logger.error(f"Failed downloading: {err}")
            return None, None

        sfed_filepath = self.local_raw_dir / sfed_raw_filename
        with open(sfed_filepath, "wb") as sfed:
            sfed.write(sfed_result.content)

        mfed_filepath = self.local_raw_dir / mfed_raw_filename
        with open(mfed_filepath, "wb") as mfed:
            mfed.write(mfed_result.content)

        sfed_unzipped, sfed_latest_date = self.get_geotiff_from_daily_90_days_file(
            sfed_filepath, date, region
        )
        mfed_unzipped, mfed_latest_date = self.get_geotiff_from_daily_90_days_file(
            mfed_filepath, date, region
        )

        if not sfed_unzipped or not mfed_unzipped:
            return None, None

        sfed_raw_path = self._update_name_if_necessary(
            sfed_filepath, SFED, sfed_latest_date, region
        )
        mfed_raw_path = self._update_name_if_necessary(
            mfed_filepath, MFED, mfed_latest_date, region
        )

        # Saving the latest zipped files for SFED and MFED
        self.save_raw_data(os.path.basename(sfed_raw_path))
        self.save_raw_data(os.path.basename(mfed_raw_path))

        return sfed_unzipped, mfed_unzipped

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

    @staticmethod
    def _with_nan_nodata(ds):
        ds = ds.copy()
        for var in ds.data_vars:
            # NetCDF inputs already carry a _FillValue encoding, which clashes with the attribute
            ds[var].encoding.pop("_FillValue", None)
            ds[var] = ds[var].rio.write_nodata(np.nan, encoded=False)
        return ds

    def get_sfed_and_mfed_urls_for_region(self, region):
        urls = []
        for band in (SFED, MFED):
            var = f"FLOODSCAN_{band}_URL_{REGION_SUFFIX[region]}"
            url = os.getenv(var)
            if not url and region == "africa":
                self.logger.warning(f"{var} is not set, falling back to old env variable")
                var = f"FLOODSCAN_{band}_URL"
                url = os.getenv(var)
            if not url:
                self.logger.error(f"{var} is not set")
            urls.append(url)
        return tuple(urls)

    def _retrieve_datarray_for_date(self, date, sfed_filename, sfed_local_file_path):
        if self.mode == "local":
            if sfed_local_file_path.exists():
                self.logger.info(f"Using cached raw data: {sfed_local_file_path}")
                da_in = rxr.open_rasterio(sfed_local_file_path, chunks="auto")
            else:
                raise FileNotFoundError(
                    f"No SFED file locally at {sfed_local_file_path}"
                )
        else:
            try:
                sfed_file = download_from_azure(
                    blob_service_client=self.blob_service_client,
                    container_name=self.container_name,
                    blob_path=self.processed_path / sfed_filename,
                    local_file_path=sfed_local_file_path,
                )
                da_in = rxr.open_rasterio(sfed_file, chunks="auto")
            except Exception as err:
                self.logger.info(f"Failed to download SFED file for date {date}: {err}")
        da_in["date"] = date
        da_in = da_in.expand_dims(["date"])
        da_in = da_in.persist()
        return da_in

    def _calculate_baseline(self, date, merged_ds):
        merged_ds = merged_ds.rolling(date=11, center=True).mean()
        merged_ds = merged_ds.groupby("date.dayofyear").mean("date")
        filename = self._generate_baseline_filename(date)
        local_path = self.local_raw_dir / filename
        merged_ds.to_netcdf(local_path, format="NETCDF4")
        return filename

    def run_pipeline(self):
        yesterday = datetime.today() - pd.DateOffset(days=1)
        dates = create_date_range(
            self.start_date,
            self.end_date,
            min_accepted=datetime.strptime("1998-01-12", DATE_FORMAT),
            max_accepted=yesterday,
        )

        self.logger.info(f"Running FloodScan pipeline in {self.mode} mode...")

        historical_end_dates = {region: self.get_historical_end_date(region) for region in REGIONS}
        # This assumes that all missing dates will be in the last 90 days
        if self.backfill:
            self.logger.info("Checking for missing data and backfilling if needed...")
            missing_dates, _ = self.check_coverage()
            self.print_coverage_report()
            for date in missing_dates:
                das_to_merge = []
                merge_global = True
                for region in REGIONS:
                    sfed, mfed = self.get_raw_data(date=date.date(), region=region)
                    if not sfed or not mfed:
                        merge_global = False
                        continue
                    if region != "africa" and merge_global is False:
                        continue
                    sfed_da = self.process_data(sfed, band_type=SFED)
                    mfed_da = self.process_data(mfed, band_type=MFED)
                    combined_da = xr.merge([sfed_da, mfed_da])
                    das_to_merge.append(combined_da)
                    if region == "africa":
                        self.save_processed_data(combined_da, self._generate_processed_filename(date=date.date()))
                if not merge_global:
                    self.logger.warning(f"Some data missing for date: {date}")
                    continue
                merged = merge_datasets([self._with_nan_nodata(da) for da in das_to_merge], nodata=np.nan)
                self.save_processed_data(merged, self._generate_processed_filename(date=date.date()), folder="global")
                self._cleanup_local()

        # Run for the latest available date
        if self.is_update:
            self.logger.info("Retrieving FloodScan data from yesterday...")
            das_to_merge = []
            merge_global = True

            for region in REGIONS:
                sfed, mfed = self.get_raw_data(date=yesterday.date(), region=region)
                if sfed is None or mfed is None:
                    if region == "africa":
                        raise Exception("Failed retrieving data from yesterday for Africa.")
                    else:
                        self.logger.warning(f"Failed retrieving data from yesterday for {region}, skipping global")
                        merge_global = False
                        continue
                if region != "africa" and merge_global is False:
                    continue

                sfed_da = self.process_data(sfed, band_type=SFED)
                mfed_da = self.process_data(mfed, band_type=MFED)
                current_da = xr.merge([sfed_da, mfed_da])

                # Adding the datasets to merged in later
                das_to_merge.append(current_da)

                # Also saving the unmerged africa geotif
                if region == "africa":
                    self.save_processed_data(current_da, self._generate_processed_filename(date=yesterday.date()))

            if not merge_global:
                self.logger.warning(f"Not all regions downloaded correctly, skipping global file")
                self._cleanup_local()
                return True

            merged = merge_datasets([self._with_nan_nodata(da) for da in das_to_merge], nodata=np.nan)
            self.save_processed_data(merged, self._generate_processed_filename(date=yesterday.date()), folder="global")
            self._cleanup_local()
            return True

        elif self.baseline_update:
            dates = create_date_range(
                datetime.strptime(f"{self.baseline_update-10}-01-01", DATE_FORMAT),
                datetime.strptime(f"{self.baseline_update}-01-01", DATE_FORMAT),
            )

            self.logger.info(
                "Retrieving the past 10 years of COGs to calculate baseline..."
            )

            sfed_files = []
            for date in dates:
                sfed_filename = self._generate_processed_filename(date)
                #TODO change the local_processed_dir to include global once the full change to using it is complete
                sfed_local_file_path = self.local_processed_dir / sfed_filename
                da_in = self._retrieve_datarray_for_date(
                    date, sfed_filename, sfed_local_file_path
                )
                if da_in.any():
                    sfed_files.append(da_in.sel({"band": 1}, drop=True))

            self.logger.info("Merging datasets for the baseline...")
            merged_ds = xr.combine_nested(sfed_files, concat_dim="date")

            self.logger.info("Calculating baseline...")
            filename = self._calculate_baseline(date, merged_ds)

            self.logger.info("Uploading baseline file to storage account...")
            self.save_raw_data(filename)

            return True

        else:
            self.logger.info(
                f"Retrieving historical FloodScan data from {min(dates).date()} until {max(dates).date()}..."
            )

            # After a region's NetCDF archive ends, its data comes from the archived 90-day zips
            zipped_files = {}
            for region in REGIONS:
                zip_dates = [date for date in dates if date > historical_end_dates[region]]
                if zip_dates:
                    zipped_files[region] = self.get_historical_zipped_files_for_region(zip_dates, region)

            for date in dates:
                region_das = {}
                for region in REGIONS:
                    self.logger.info(f"Processing {region} for {date.date()}")
                    if date <= historical_end_dates[region]:
                        sfed_path, mfed_path = self.get_historical_nc_files(region)
                        sfed = self.process_historical_data(sfed_path, date, SFED)
                        mfed = self.process_historical_data(mfed_path, date, MFED)
                    else:
                        files = zipped_files[region].get(date.date())
                        if not files:
                            continue
                        sfed = self.process_data(files[0], band_type=SFED)
                        mfed = self.process_data(files[1], band_type=MFED)
                    region_das[region] = xr.merge([sfed, mfed])

                if not region_das:
                    self.logger.warning(f"No data found for date: {date.date()}")
                    continue

                # Also saving the unmerged africa geotif
                if "africa" in region_das:
                    self.save_processed_data(region_das["africa"], self._generate_processed_filename(date=date))

                missing = [region for region in REGIONS if region not in region_das]
                if missing:
                    self.logger.warning(f"Missing {', '.join(missing)} for {date.date()}, skipping global file")
                    continue
                merged = merge_datasets(
                    [self._with_nan_nodata(region_das[region]) for region in REGIONS], nodata=np.nan
                )
                self.save_processed_data(merged, self._generate_processed_filename(date=date), folder="global")

            self._cleanup_local()

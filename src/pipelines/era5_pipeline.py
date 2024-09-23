from datetime import datetime

import cdsapi
import pandas as pd
import xarray as xr
from dateutil.relativedelta import relativedelta

from ..config.settings import OUTPUT_METADATA
from ..utils import raster_utils
from .pipeline import Pipeline


class ERA5Pipeline(Pipeline):
    def __init__(self, mode, is_update, start_year, end_year, log_level, **kwargs):
        super().__init__(
            container_name=kwargs["container_name"],
            raw_path=kwargs["raw_path"],
            processed_path=kwargs["processed_path"],
            log_level=log_level,
        )
        self.mode = mode
        self.is_update = is_update
        self.start_year = start_year
        self.end_year = end_year
        self.client = cdsapi.Client()

    def _generate_raw_filename(self, year, month=None):
        fname_suffix = f"{month:02d}" if month else "all"
        return f"tp_reanalysis_monthly_{year}_{fname_suffix}.grib"

    def _generate_processed_filename(self, date):
        return f"precip_reanalysis_v{date}.tif"

    def query_api(self, year, month=None):
        filename = self._generate_raw_filename(year, month)
        # Download all months in the year unless a month is provided
        months = [f"{month:02d}"] if month else [f"{d:02d}" for d in range(1, 13)]
        data_request = {
            "data_format": "grib",
            "variable": "total_precipitation",
            "product_type": "monthly_averaged_reanalysis",
            "year": [year],
            "month": months,
            "time": "00:00",
        }
        self.client.retrieve(
            "reanalysis-era5-single-levels-monthly-means",
            data_request,
            self.local_raw_dir / filename,
        )
        self.save_raw_data(filename)
        return filename

    def process_data(self, raw_filename):
        raw_file_path = self.local_raw_dir / raw_filename
        ds = xr.open_dataset(
            raw_file_path,
            engine="cfgrib",
            drop_variables=["surface", "number"],
            backend_kwargs=dict(
                time_dims=("valid_time", "forecastMonth"), indexpath=("")
            ),
        )
        # Need to expand if there's only one valid_time value
        # ie. we've only gotten data from a single month
        try:
            ds = ds.expand_dims(["valid_time"])
        except ValueError as e:
            print(e)
            pass

        pub_dates = ds.valid_time.values
        ds = ds.rename({"tp": "total precipitation"})
        ds = raster_utils.change_longitude_range(ds, "longitude")

        era5_metadata = OUTPUT_METADATA.copy()
        era5_metadata["units"] = "mm/day"
        era5_metadata["averaging_period"] = "monthly"
        era5_metadata["grid_resolution"] = 0.25
        era5_metadata["source"] = "ECMWF"
        era5_metadata["product"] = "ERA5 Reanalysis"

        for date in pub_dates:
            date_formatted = pd.to_datetime(date).strftime("%Y-%m-%d")
            era5_metadata["year_valid"] = int(date_formatted[:4])
            era5_metadata["month_valid"] = int(date_formatted[5:7])
            ds_sel = ds.sel({"valid_time": date})
            ds_sel.attrs = era5_metadata
            ds_sel = ds_sel * 1000  # Convert from meters to mm
            ds_sel = ds_sel.rio.write_crs("EPSG:4326", inplace=False)

            filename = self._generate_processed_filename(date_formatted)
            self.save_processed_data(ds_sel, filename)

    def run_pipeline(self):
        today = datetime.today()
        cur_year = today.year
        last_month = (today - relativedelta(months=1)).month

        self.logger.info(f"Running ERA5 pipeline in {self.mode} mode...")
        if self.is_update:
            self.logger.info("Retrieving ERA5 data from last month...")
            raw_filename = self.get_raw_data(year=cur_year, month=last_month)
            self.process_data(raw_filename)
        else:
            self.logger.info(
                f"Retrieving ERA5 data from {self.start_year} to {self.end_year}..."
            )
            for year in range(self.start_year, self.end_year + 1):
                if year == cur_year:
                    for month in range(1, last_month + 1):
                        raw_filename = self.get_raw_data(year=year, month=month)
                        self.process_data(raw_filename)
                else:
                    raw_filename = self.get_raw_data(year=year)
                    self.process_data(raw_filename)
        self.logger.info("Completed ERA5 update.")

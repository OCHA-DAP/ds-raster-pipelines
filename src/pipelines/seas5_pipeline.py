import os
from datetime import datetime

import fsspec
import pandas as pd
import xarray as xr
from dateutil.relativedelta import relativedelta
from ecmwfapi import ECMWFService

from ..utils import leadtime_utils, raster_utils
from .pipeline import Pipeline


class SEAS5Pipeline(Pipeline):
    def __init__(self, mode, is_update, start_year, end_year, log_level, **kwargs):
        super().__init__(
            container_name=kwargs["container_name"],
            raw_path=kwargs["raw_path"],
            processed_path=kwargs["processed_path"],
            log_level=log_level,
            mode=mode,
            metadata=kwargs["metadata"],
            use_cache=kwargs["use_cache"],
        )
        self.is_update = is_update
        self.start_year = start_year
        self.end_year = end_year
        self.server = ECMWFService("mars")
        self.aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.bbox = kwargs["bbox"][mode]

    def _generate_raw_filename(self, year, issued_month=None, fc_month=None):
        if year == 2024:
            return f"T8L{issued_month:02}010000{fc_month:02}______1.grib"
        else:
            return f"tprate_{year}.grib"

    def _generate_processed_filename(self, issued_date, leadtime):
        return f"precip_em_i{issued_date}_lt{leadtime}.tif"

    def query_api(self, year, issued_month=None, fc_month=None):
        if year == 2024:
            filename = self._generate_raw_filename(year, issued_month, fc_month)
            aws_filename = filename.split(".")[0]  # File on AWS doesn't have `.grib`
            s3_path = f"s3://{self.aws_bucket_name}/ecmwf/{aws_filename}"
            fs = fsspec.filesystem("s3")
            with fs.open(s3_path) as f:
                with open(self.local_raw_dir / filename, "wb") as temp_file:
                    temp_file.write(f.read())
        else:
            filename = self._generate_raw_filename(year)
            bbox_str = "/".join(
                [
                    str(round(coord, 1))
                    for coord in [
                        self.bbox[3],
                        self.bbox[0],
                        self.bbox[1],
                        self.bbox[2],
                    ]
                ]
            )
            start_date = pd.to_datetime(f"{year}-01-01")
            end_date = pd.to_datetime(f"{year}-12-01")
            date_range = pd.date_range(start=start_date, end=end_date, freq="MS")
            dates_use = "/".join([date.strftime("%Y-%m-%d") for date in date_range])

            # Pre 2016 has 25 ensemble members and then 51 afterwards
            if year <= 2016:
                ensemble_members = "/".join([str(i) for i in range(25)])
            else:
                ensemble_members = "/".join([str(i) for i in range(51)])

            # See docs for more details on parameters:
            # https://confluence.ecmwf.int/display/UDOC/Keywords+in+MARS+and+Dissemination+requests?src=contextnavpagetreemode
            self.server.execute(
                {
                    "class": "od",  # operational archive
                    "date": dates_use,
                    "expver": "0001",  # model version
                    "fcmonth": "1/2/3/4/5/6/7",  # forecast months
                    "levtype": "sfc",  # surface horizontal level
                    "method": "1",
                    "area": bbox_str,
                    "grid": "0.4/0.4",
                    "number": ensemble_members,
                    "origin": "ecmf",
                    "param": "228.172",  # tprate
                    "stream": "msmm",  # multi-model seasonal forecast atmospheric monthly means
                    "system": "5",
                    "time": "00:00:00",
                    "type": "fcmean",  # forecast mean
                    "target": "output",
                },
                self.local_raw_dir / filename,
            )
        self.save_raw_data(filename)
        return filename

    def process_data(self, raw_filename, year, issued_month=None, fc_month=None):
        raw_file_path = self.local_raw_dir / raw_filename
        self.metadata["year_issued"] = year

        # 2024 data from AWS source will just have `number``, `latitude``, and `longitude`` dimensions
        # The month and fc_month are in the filename. Whereas the archived data pre 2024
        # will also contain `forecastMonth` and `time` dimensions that need to be parsed.
        if year == 2024:
            ds = xr.open_dataset(
                raw_file_path,
                engine="cfgrib",
                filter_by_keys={"dataType": "fcmean"},
                indexpath=(""),
            )
        else:
            ds = xr.open_dataset(
                raw_file_path,
                engine="cfgrib",
                drop_variables=["surface", "values"],
                backend_kwargs=dict(
                    time_dims=("time", "forecastMonth"), indexpath=("")
                ),
            )

        ds_mean = ds.mean(dim="number")
        ds_mean = ds_mean * 1000 * 3600 * 24
        ds_mean = ds_mean.rename({"tprate": "total precipitation"})

        if year == 2024:
            leadtime = leadtime_utils.to_leadtime(issued_month, fc_month)
            self.metadata["month_issued"] = issued_month
            self.metadata["year_valid"] = leadtime_utils.to_fc_year(
                issued_month, year, leadtime
            )
            self.metadata["month_valid"] = fc_month
            self.metadata["leadtime"] = leadtime
            ds_mean = raster_utils.round_lat_lon(ds_mean, "latitude", "longitude")
            ds_mean = ds_mean.rio.write_crs("EPSG:4326", inplace=False)
            filename = self._generate_processed_filename(
                f"{year}-{issued_month:02}-01", leadtime
            )
            self.save_processed_data(ds_mean, filename)
        else:
            issued_dates = ds_mean.time.values
            forecast_months = ds_mean.forecastMonth.values
            for issued_date in issued_dates:
                issued_date_formatted = pd.to_datetime(issued_date).strftime("%Y-%m-%d")
                ds_sel = ds_mean.sel({"time": issued_date})
                for month in forecast_months:
                    leadtime = month - 1
                    filename = self._generate_processed_filename(
                        issued_date_formatted, leadtime
                    )

                    ds_sel_month = ds_sel.sel({"forecastMonth": month})
                    ds_sel_month = ds_sel_month.rio.write_crs(
                        "EPSG:4326", inplace=False
                    )

                    issued_year = int(issued_date_formatted[:4])
                    issued_month = int(issued_date_formatted[5:7])
                    self.metadata["month_issued"] = issued_month
                    self.metadata["year_valid"] = leadtime_utils.to_fc_year(
                        issued_month, issued_year, leadtime
                    )
                    self.metadata["month_valid"] = leadtime_utils.to_fc_month(
                        issued_month, leadtime
                    )
                    self.metadata["leadtime"] = leadtime
                    ds_sel_month = raster_utils.round_lat_lon(
                        ds_sel_month, "latitude", "longitude"
                    )
                    self.save_processed_data(ds_sel_month, filename)

    def run_pipeline(self):
        today = datetime.today()
        cur_year = today.year
        last_month = (today - relativedelta(months=1)).month

        self.logger.info(f"Running SEAS5 pipeline in {self.mode} mode...")
        if self.is_update:
            self.logger.info("Retrieving SEAS5 data from last month...")
            for fc_month in leadtime_utils.leadtime_months(last_month, 7):
                raw_filename = self.get_raw_data(
                    year=cur_year, issued_month=last_month, fc_month=fc_month
                )
                self.process_data(
                    raw_filename, cur_year, issued_month=last_month, fc_month=fc_month
                )
            pass

        else:
            self.logger.info(
                f"Retrieving SEAS5 data from {self.start_year} to {self.end_year}..."
            )
            for year in range(self.start_year, self.end_year + 1):
                # TODO: May need updating when cur_year > 2024
                if year == cur_year:
                    for month in range(1, last_month + 1):
                        for fc_month in leadtime_utils.leadtime_months(month, 7):
                            raw_filename = self.get_raw_data(
                                year=cur_year, issued_month=month, fc_month=fc_month
                            )
                            self.process_data(
                                raw_filename,
                                year,
                                issued_month=month,
                                fc_month=fc_month,
                            )
                else:
                    raw_filename = self.get_raw_data(year=year)
                    self.process_data(raw_filename, year)

        self.logger.info("Completed SEAS5 update.")

import os

import pandas as pd
import xarray as xr
from ecmwfapi import ECMWFService

from constants import CONTAINER_GLOBAL, SAS_TOKEN_DEV, STORAGE_ACCOUNT_DEV
from utils.cloud_utils import upload_file

server = ECMWFService("mars")


def download_archive(year, bbox, td):

    tp_raw = os.path.join(td, f"seas5_mars_tprate_{year}.grib")
    temp_base = os.path.basename(tp_raw)
    raw_outpath = os.path.join("mars", "raw", temp_base)

    # Generate a sequence of monthly dates
    start_date = pd.to_datetime(f"{year}-01-01")
    end_date = pd.to_datetime(f"{year}-12-01")
    date_range = pd.date_range(start=start_date, end=end_date, freq="MS")
    date_strings = [date.strftime("%Y-%m-%d") for date in date_range]
    dates_use = "/".join(date_strings)

    # Pre 2016 has 25 ensemble members and then 51 afterwards
    if year <= 2016:
        number_use = "/".join([str(i) for i in range(25)])
    else:
        number_use = "/".join([str(i) for i in range(51)])

    server.execute(
        {
            "class": "od",
            "date": dates_use,
            "expver": "0001",
            "fcmonth": "1/2/3/4/5/6/7",
            "levtype": "sfc",
            "method": "1",
            "area": bbox,
            "grid": "0.4/0.4",
            "number": number_use,
            "origin": "ecmf",
            "param": "228.172",
            "stream": "msmm",
            "system": "5",
            "time": "00:00:00",
            "type": "fcmean",
            "target": "output",
        },
        tp_raw,
    )

    upload_file(
        local_file_path=tp_raw,
        sas_token=SAS_TOKEN_DEV,
        container_name=CONTAINER_GLOBAL,
        storage_account=STORAGE_ACCOUNT_DEV,
        blob_path=raw_outpath,
    )

    # Return the path of the temp file
    return tp_raw


def process_archive(tp_raw, td):

    ds = xr.open_dataset(
        tp_raw,
        engine="cfgrib",
        drop_variables=["surface", "values"],
        backend_kwargs=dict(time_dims=("time", "forecastMonth")),
    )

    ds_mean = ds.mean(dim="number")

    pub_dates = ds_mean.time.values
    forecast_months = ds_mean.forecastMonth.values

    for date in pub_dates:
        date_formatted = pd.to_datetime(date).strftime("%Y-%m-%d")
        ds_sel = ds_mean.sel({"time": date})
        for month in forecast_months:

            tp_processed = os.path.join(
                td, f"seas5_mars_tprate_em_i{date_formatted}_lt{month-1}.tif"
            )
            temp_base = os.path.basename(tp_processed)
            processed_outpath = os.path.join("mars", "processed", temp_base)

            ds_sel_month = ds_sel.sel({"forecastMonth": month})
            ds_sel_month = ds_sel_month.rio.write_crs("EPSG:4326", inplace=False)
            ds_sel_month.rio.to_raster(tp_processed, driver="COG")

            upload_file(
                local_file_path=tp_processed,
                sas_token=SAS_TOKEN_DEV,
                container_name=CONTAINER_GLOBAL,
                storage_account=STORAGE_ACCOUNT_DEV,
                blob_path=processed_outpath,
            )

    return

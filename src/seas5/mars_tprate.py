import logging
import os
from pathlib import Path

import pandas as pd
import xarray as xr
from ecmwfapi import ECMWFService

from constants import CONTAINER_RASTER
from src.utils.cloud_utils import upload_file_by_mode

server = ECMWFService("mars")
logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)

RAW_PATH = Path("seas5") / "mars" / "raw"
PROCESSED_PATH = Path("seas5") / "mars" / "processed"


def download_archive(year, bbox, dir, mode="local"):
    """
    Downloads annual, archival SEAS5 data as .grib files from ECMWF MARS

    Args:
        year (int): Year from which to download data
        bbox (list): Bounding box to define the geographic extent of data to download
        dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        path_raw (str): Location of the output raw data
    """

    logger.info(f"Downloading data from {year}...")

    bbox_str = "/".join(
        [
            str(round(coord, 1))
            for coord in [
                bbox[3],
                bbox[0],
                bbox[1],
                bbox[2],
            ]
        ]
    )

    path_raw = os.path.join(dir, f"tprate_{year}.grib")
    temp_base = os.path.basename(path_raw)
    raw_outpath = RAW_PATH / temp_base

    # Generate a sequence of monthly dates
    start_date = pd.to_datetime(f"{year}-01-01")
    end_date = pd.to_datetime(f"{year}-12-01")
    date_range = pd.date_range(start=start_date, end=end_date, freq="MS")
    date_strings = [date.strftime("%Y-%m-%d") for date in date_range]
    dates_use = "/".join(date_strings)

    # Pre 2016 has 25 ensemble members and then 51 afterwards
    if year <= 2016:
        ensemble_members = "/".join([str(i) for i in range(25)])
    else:
        ensemble_members = "/".join([str(i) for i in range(51)])

    # See docs for more details on parameters:
    # https://confluence.ecmwf.int/display/UDOC/Keywords+in+MARS+and+Dissemination+requests?src=contextnavpagetreemode
    server.execute(
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
        path_raw,
    )
    logger.info(f"Data downloaded successfully. Saved temporarily to {path_raw}.")
    if mode != "local":
        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=path_raw,
            blob_path=raw_outpath,
        )
        logger.info("Data uploaded successfully to Azure.")

    return path_raw


def process_archive(path_raw, dir, mode="local"):
    """
    Processes raw grib files to output analysis-ready COGs (.tif)

    Args:
        path_raw (str): Location of the input raw data
        dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        None
    """

    logger.info(f"Processing temporary file: {path_raw}...")
    ds = xr.open_dataset(
        path_raw,
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
                dir, f"tprate_em_i{date_formatted}_lt{month - 1}.tif"
            )
            temp_base = os.path.basename(tp_processed)
            processed_outpath = PROCESSED_PATH / temp_base

            ds_sel_month = ds_sel.sel({"forecastMonth": month})
            ds_sel_month = ds_sel_month.rio.write_crs("EPSG:4326", inplace=False)
            ds_sel_month.rio.to_raster(tp_processed, driver="COG")

            if mode != "local":
                upload_file_by_mode(
                    mode=mode,
                    container_name=CONTAINER_RASTER,
                    local_file_path=tp_processed,
                    blob_path=processed_outpath,
                )
    logger.info("Files processed successfully.")
    return

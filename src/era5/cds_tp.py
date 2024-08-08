import logging
from datetime import datetime
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr
from azure.storage.blob import StandardBlobTier

from constants import CONTAINER_RASTER
from src.utils.cloud_utils import upload_file_by_mode

dir = "test_outputs"
RAW_PATH = Path("era5") / "monthly" / "raw"
PROCESSED_PATH = Path("era5") / "monthly" / "processed"

client = cdsapi.Client()
logger = logging.getLogger(__name__)
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


def download_grib(year, dir, month=None, mode="local"):
    """
    Downloads annual, archival ERA5 data as .grib files from ECMWF CDS

    Args:
        year (int): Year from which to download data
        dir (str): (Temporary) Location to save the data locally
        month (int): (Optional) Month for which to download data. If None, will download all months.
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        path_raw (str): Location of the output raw data
    """
    logger.info(f"Downloading data from {year}...")
    fname_suffix = f"{month:02d}" if month else "all"
    fname = f"tp_reanalysis_monthly_{year}_{fname_suffix}.grib"

    path_raw = dir / RAW_PATH
    path_raw.mkdir(exist_ok=True, parents=True)
    path_raw = path_raw / fname

    # Download all months in the year unless a month is provided
    month = [f"{month:02d}"] if month else [f"{d:02d}" for d in range(1, 13)]

    # https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview
    data_request = {
        "data_format": "grib",
        "variable": "total_precipitation",
        "product_type": "monthly_averaged_reanalysis",
        "year": [year],
        "month": month,
        "time": "00:00",
    }

    logger.info(f"Data downloaded successfully. Saved temporarily to {path_raw}.")
    client.retrieve(
        "reanalysis-era5-single-levels-monthly-means",
        data_request,
        path_raw,
    )

    if mode != "local":
        raw_outpath = RAW_PATH / fname
        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=path_raw,
            blob_path=raw_outpath,
        )
        logger.info("Data uploaded successfully to Azure.")

    return path_raw


def process_grib(path_raw, dir, mode="local"):
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
        drop_variables=["surface", "number"],
        backend_kwargs=dict(time_dims=("valid_time", "forecastMonth"), indexpath=("")),
    )

    # Need to expand if there's only one valid_time value
    try:
        ds = ds.expand_dims(["valid_time"])
    except ValueError as e:
        print(e)
        pass

    pub_dates = ds.valid_time.values
    path_processed = dir / PROCESSED_PATH
    path_processed.mkdir(exist_ok=True, parents=True)

    for date in pub_dates:
        date_formatted = pd.to_datetime(date).strftime("%Y-%m-%d")
        ds_sel = ds.sel({"valid_time": date})

        fname = f"tp_reanalysis_v{date_formatted}.tif"
        outpath_processed = path_processed / fname

        ds_sel = ds_sel.rio.write_crs("EPSG:4326", inplace=False)
        ds_sel.rio.to_raster(outpath_processed, driver="COG")

        if mode != "local":
            processed_outpath = PROCESSED_PATH / fname
            upload_file_by_mode(
                mode=mode,
                container_name=CONTAINER_RASTER,
                local_file_path=outpath_processed,
                blob_path=processed_outpath,
                content_type="image/tiff",
                blob_tier=StandardBlobTier.HOT,
            )

    logger.info("Files processed successfully.")
    return


def run_update(is_update, start, end, dir, mode="local"):
    if is_update:
        logger.info("Retrieving ERA5 data from current month...")
        year = datetime.now().year
        month = datetime.now().month
        tp_raw = download_grib(year, dir, month, mode)
        process_grib(tp_raw, dir, mode)
    else:
        logger.info(f"Retrieving ERA5 data from {start} to {end}...")
        for year in range(start, end + 1):
            if year == 2024:
                months = list(range(1, 8))
                for month in months:
                    tp_raw = download_grib(year, dir, month, mode)
                    process_grib(tp_raw, dir, mode)
            else:
                tp_raw = download_grib(year, dir, month=None, mode=mode)
                process_grib(tp_raw, dir, mode)

import logging
import os
import sys
from pathlib import Path

import fsspec
import xarray as xr
from azure.storage.blob import StandardBlobTier
from dotenv import load_dotenv

from constants import CONTAINER_RASTER, OUTPUT_METADATA
from src.utils.azure_utils import upload_file_by_mode
from src.utils.leadtime_utils import leadtime_months, to_fc_year, to_leadtime

load_dotenv()

logger = logging.getLogger(__name__)
logging.getLogger("aiobotocore.credentials").setLevel(logging.WARNING)

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
RAW_PATH = Path("seas5") / "aws" / "raw"
PROCESSED_PATH = Path("seas5") / "aws" / "processed"


def download_aws(month, lt_month, dir, mode="local"):
    """
    Downloads a raw monthly forecast .grib file from AWS and (optionally) saves a copy to Azure cloud

    Parameters:
        month (int): Month that the forecast was published
        lt_month (int): Number of months leadtime for the forecast
        dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        path_raw (str): Location of the output raw data
    """
    # TODO: This assumes all data is from 2024
    fname = f"T8L{month:02}010000{lt_month:02}______1"
    s3_path = f"s3://{BUCKET_NAME}/ecmwf/{fname}"
    fs = fsspec.filesystem("s3")
    file_name = os.path.basename(s3_path)
    file_name = f"{file_name}.grib"

    path_raw = dir / RAW_PATH
    path_raw.mkdir(exist_ok=True, parents=True)
    path_raw = path_raw / file_name

    with fs.open(s3_path) as f:
        with open(path_raw, "wb") as temp_file:
            temp_file.write(f.read())

    if mode != "local":
        raw_outpath = RAW_PATH / fname
        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=path_raw,
            blob_path=raw_outpath,
        )
    return path_raw


def process_aws(month, fc_month, path_raw, dir, mode="local"):
    """
    Processes raw .grib files to output analysis-ready COGs (.tif)

    Parameters:
        month (int): Month that the forecast was published
        fc_month (int): Month that the forecast applies to
        path_raw (str): Location of the input raw data
        dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        None
    """
    lt = to_leadtime(month, fc_month)
    # TODO: This assumes all data is from 2024
    fname = f"tprate_em_i2024-{month:02}-01_lt{lt}.tif"

    path_processed = dir / PROCESSED_PATH
    path_processed.mkdir(exist_ok=True, parents=True)
    path_processed = path_processed / fname

    # Take the ensemble mean and write out to COG
    ds = xr.open_dataset(
        path_raw, engine="cfgrib", filter_by_keys={"dataType": "fcmean"}, indexpath=("")
    )

    # Take ensemble mean
    ds_mean = ds.mean(dim="number")
    # Convert from m/s to mm/day
    ds_mean = ds_mean * 1000 * 3600 * 24

    aws_metadata = OUTPUT_METADATA.copy()
    aws_metadata["units"] = "mm/day"
    aws_metadata["averaging_period"] = "monthly"
    aws_metadata["grid_resolution"] = 0.4
    aws_metadata["source"] = "ECMWF"
    aws_metadata["product"] = "SEAS5 Seasonal Forecast"
    aws_metadata["leadtime_units"] = "months"
    aws_metadata["year_issued"] = 2024
    aws_metadata["month_issued"] = month
    aws_metadata["year_valid"] = to_fc_year(month, 2024, lt)
    aws_metadata["month_valid"] = fc_month
    aws_metadata["leadtime"] = lt

    ds_mean = ds_mean.rename({"tprate": "total precipitation"})

    ds_mean.attrs = aws_metadata
    ds_mean = ds_mean.rio.write_crs("EPSG:4326", inplace=False)
    ds_mean.rio.to_raster(path_processed, driver="COG")

    if mode != "local":
        processed_outpath = PROCESSED_PATH / fname
        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=path_processed,
            blob_path=processed_outpath,
            content_type="image/tiff",
            blob_tier=StandardBlobTier.HOT,
        )
    return


def run_update(month, dir, mode):
    """
    Given an input month, processes all .grib files to COGs (.tif)

    Parameters:
        month (int): Month that the forecast was published
        dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        None
    """
    logger.info(f"Processing data for month: {month}...")
    for fc_month in leadtime_months(month, 7):
        try:
            path_raw = download_aws(month, fc_month, dir, mode)
        except FileNotFoundError:
            logger.error("Source data not found in AWS bucket.")
            sys.exit(1)
        process_aws(month, fc_month, path_raw, dir, mode)
    logger.info("All data successfully updated.")

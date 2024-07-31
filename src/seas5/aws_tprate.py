import os
from pathlib import Path

import fsspec
import xarray as xr
from dotenv import load_dotenv

from src.utils.general_utils import to_end_month, to_leadtime

load_dotenv()

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
RAW_PATH = Path("seas5") / "aws" / "raw"
PROCESSED_PATH = Path("seas5") / "aws" / "processed"


def download_aws(month, lt_month, dir, mode="local"):
    """
    Downloads a raw monthly forecast .grib file from AWS and (optionally) saves a copy to Azure cloud

    Args:
        month (int): Month that the forecast was published
        lt_month (int): Number of months leadtime for the forecast
        dir (str): (Temporary) Location to save the data
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        path_raw (str): Location of the output raw data
    """
    # TODO: This assumes all data is from 2024
    fname = f"ecmwf/T8L{month:02}010000{lt_month:02}______1"
    s3_path = f"s3://{BUCKET_NAME}/{fname}"
    fs = fsspec.filesystem("s3")
    file_name = os.path.basename(s3_path)
    file_name = f"{file_name}.grib"

    path_raw = dir / RAW_PATH
    path_raw.mkdir(exist_ok=True, parents=True)
    path_raw = path_raw / file_name

    with fs.open(s3_path) as f:
        with open(path_raw, "wb") as temp_file:
            temp_file.write(f.read())
    return path_raw


def process_aws(month, lt_month, path_raw, dir, mode="local"):
    """
    Processes raw .grib files to output analysis-ready COGs (.tif)

    Args:
        month (int): Month that the forecast was published
        lt_month (int): Number of months leadtime for the forecast
        path_raw (str): Location of the input raw data
        dir (str): (Temporary) Location to save the data
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        None
    """
    lt = to_leadtime(month, lt_month)
    # TODO: This assumes all data is from 2024
    file_name = f"tprate_em_i2024-{month:02}-01_lt{lt}.tif"

    path_processed = dir / PROCESSED_PATH
    path_processed.mkdir(exist_ok=True, parents=True)
    path_processed = path_processed / file_name

    # Take the ensemble mean and write out to COG
    ds = xr.open_dataset(
        path_raw, engine="cfgrib", filter_by_keys={"dataType": "fcmean"}
    )
    ds_mean = ds.mean(dim="number")
    ds_mean = ds_mean.rio.write_crs("EPSG:4326", inplace=False)
    ds_mean.rio.to_raster(path_processed, driver="COG")
    return


def run_update(month, dir, mode):
    for lt_month in to_end_month(month, 7):
        path_raw = download_aws(month, lt_month, dir)
        process_aws(month, lt_month, path_raw, dir)

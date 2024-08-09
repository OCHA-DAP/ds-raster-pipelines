import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
import xarray as xr
from azure.storage.blob import StandardBlobTier

from constants import CONTAINER_RASTER, IMERG_BASE_URL
from src.utils.azure_utils import upload_file_by_mode

logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)


def download(
    date: datetime = datetime.today(),
    run: Literal["E", "L"] = "L",
    version: int = 7,
    save_raw: bool = False,
    output_dir: Path = "",
    mode="local",
):
    """
    Downloads IMERG data for a given date range and saves it to a location in a configured container

    Parameters
        date (datetime.datetime) : Date to download files from
        run (str): "E" for early run, "L" for late run
        version (int): IMERG version (7 is technically 07B)
        save_raw (str): Whether to also save unprocessed raster data
        output_dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        path_raw (str): Location of the output raw data
    """
    run_type = "late" if run == "L" else "early"
    raw_path = Path("imerg") / f"v{version}" / f"{run_type}" / "raw"
    fname = f"imerg-daily-{run_type}-{date.strftime('%Y-%m-%d')}.nc4"

    logger.info(f"Downloading data from {date}: {fname}")

    path_raw = output_dir / raw_path
    path_raw.mkdir(exist_ok=True, parents=True)
    path_raw = path_raw / fname

    version_letter = "B" if version == 7 else ""
    url = IMERG_BASE_URL.format(
        run=run, date=date, version=version, version_letter=version_letter
    )

    try:
        result = requests.get(url)
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.error(f"Failed downloading: {err}")
        return None

    f = open(path_raw, "wb")
    f.write(result.content)
    f.close()

    if save_raw:
        raw_outpath = raw_path / fname
        logger.info(f"Uploading raw file to: {raw_outpath}")

        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=path_raw,
            blob_path=raw_outpath,
        )

    return path_raw


def process_nc4(date, run, version, path_raw, output_dir, mode="local"):
    """
    Processes raw .nc4 files to output analysis-ready COGs (.tif)

    Parameters:
        date (datetime.datetime) : Date to process files from
        run (str): "E" for early run, "L" for late run
        version (int): IMERG version (7 is technically 07B)
        path_raw (str): Location of the input raw data
        output_dir (str): (Temporary) Location to save the data locally
        mode (str): local/dev/prod -- Determines where the output data will be saved

    Returns:
        path_processed (str): Location of the output processed data
    """
    run_type = "late" if run == "L" else "early"
    processed_path = Path("imerg") / f"v{version}" / f"{run_type}" / "processed"
    fname = f"imerg-daily-{run_type}-{date.strftime('%Y-%m-%d')}.tif"

    logger.info(f"Processing {path_raw} to COGs: {fname}")

    path_processed = output_dir / processed_path
    path_processed.mkdir(exist_ok=True, parents=True)
    path_processed = path_processed / fname

    with xr.open_dataset(path_raw) as ds:
        ds = ds.transpose("lat", "lon", "time", "nv")
        var_name = "precipitationCal" if "precipitationCal" in ds else "precipitation"
        da = ds[var_name]
        if not ds["time"].dtype == "<M8[ns]":
            da["time"] = pd.to_datetime(
                [pd.Timestamp(t.strftime("%Y-%m-%d")) for t in da["time"].values]
            )
        da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
        da.rio.to_raster(path_processed, driver="COG")

    if mode != "local":
        processed_outpath = processed_path / fname
        logger.info(f"Uploading raw file to: {processed_outpath}")

        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=path_processed,
            blob_path=processed_outpath,
            content_type="image/tiff",
            blob_tier=StandardBlobTier.HOT,
        )

    return path_processed

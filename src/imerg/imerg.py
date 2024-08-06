from datetime import datetime
import logging
import os
import pandas as pd
import requests
from pathlib import Path
from typing import Literal
import xarray as xr
from azure.storage.blob import StandardBlobTier

from constants import IMERG_BASE_URL, SAS_TOKEN_DEV, CONTAINER_RASTER
from src.utils.azure_utils import upload_file, upload_file_by_mode

logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)


def download(date: datetime = datetime.today(),
             run: Literal["E", "L"] = "L",
             version: int = 7,
             save_raw: bool = False,
             output_dir: Path = "",
             mode="local"):
    """
        Downloads IMERG data for a given date range and saves it to a location in a configured container
        Parameters
        ----------
        date: datetime.datetime
            Date to download
        run:
            "E" for early run, "L" for late run
        version: int
            IMERG version (7 is technically 07B)
        save_raw: bool
            Whether to also save unprocessed raster data
        output_dir: Path
             (Temporary) Location to save the data locally
        mode: str
            local/dev/prod -- Determines where the output data will be saved

        Returns:
            path_raw: str
            Location of the output raw data
        -------
    """
    logger.info(f"Downloading data from {date}...")

    prefix = f"imerg/v{version}/processed/"
    fname = (
        f"{prefix}imerg-daily-{'late' if run == 'L' else 'early'}-{date.strftime('%Y-%m-%d')}.tif"
    )

    processed_file_path = os.path.join(output_dir, fname)
    raw_file_path = processed_file_path.replace(".tif", ".nc4").replace("processed", "raw")

    RAW_PATH = fname.replace(".tif", ".nc4").replace("processed", "raw")

    version_letter = "B" if version == 7 else ""
    url = IMERG_BASE_URL.format(
        run=run, date=date, version=version, version_letter=version_letter
    )
    result = requests.get(url)
    result.raise_for_status()

    if not (os.path.exists(os.path.dirname(raw_file_path)) or os.path.exists(os.path.dirname(processed_file_path))):
        os.makedirs(os.path.dirname(raw_file_path), exist_ok=True)
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)

    f = open(raw_file_path, "wb")
    f.write(result.content)
    f.close()

    logger.info(f"Processing {raw_file_path} to COGs: {fname}")

    with xr.open_dataset(raw_file_path) as ds:
        ds = ds.transpose("lat", "lon", "time", "nv")
        var_name = (
            "precipitationCal" if "precipitationCal" in ds else "precipitation"
        )
        da = ds[var_name]
        if not ds["time"].dtype == "<M8[ns]":
            da["time"] = pd.to_datetime(
                [pd.Timestamp(t.strftime("%Y-%m-%d")) for t in da["time"].values]
            )
        da = da.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
        da.rio.to_raster(processed_file_path, driver="COG")

    if mode != "local":
        upload_file_by_mode(
            mode=mode,
            container_name=CONTAINER_RASTER,
            local_file_path=processed_file_path,
            blob_path=fname,
            content_type="image/tiff",
            blob_tier=StandardBlobTier.HOT)

        if save_raw:
            logger.info(f"Uploading raw file to: {RAW_PATH}")

            upload_file_by_mode(
                mode=mode,
                container_name=CONTAINER_RASTER,
                local_file_path=raw_file_path,
                blob_path=RAW_PATH)

    return raw_file_path


from datetime import datetime
import logging
import os
import pandas as pd
import requests
import tempfile
from typing import Literal
import xarray as xr
from azure.storage.blob import StandardBlobTier

from constants import IMERG_BASE_URL, SAS_TOKEN_DEV, CONTAINER_RASTER, STORAGE_ACCOUNT
from src.utils.cloud_utils import get_container_client, upload_file

logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)


def download_imerg(start_date: datetime = datetime.strptime("2024-06-01", '%Y-%m-%d'),
                   end_date: datetime = datetime.today(),
                   run: Literal["E", "L"] = "L",
                   version: int = 7,
                   save_raw: bool = False):
    """
        Downloads IMERG data for a given date range and saves it to a location in a configured container
        Parameters
        ----------
        start_date: datetime.datetime
            Range start date to download
        end_date: datetime.datetime
            Range end date to download
        run:
            "E" for early run, "L" for late run
        version: int
            IMERG version (7 is technically 07B)
        save_raw: bool
            Whether to also save unprocessed raster data
        Returns
        -------
    """
    logger.info(f"Starting the download_imerg_historical pipeline from {start_date} to {end_date}")

    prefix = f"imerg/v{version}/processed/"
    existing_processed_files = [
        x.name.replace(prefix, "")
        for x in get_container_client(sas_token=SAS_TOKEN_DEV,
                                      container_name=CONTAINER_RASTER).list_blobs(
            name_starts_with=f"{prefix}imerg"
        )
    ]

    for date in pd.date_range(
            start_date, end_date - pd.DateOffset(days=1)
    ):
        output_blob = (
            f"{prefix}imerg-daily-late-{date.strftime('%Y-%m-%d')}.tiff"
        )

        if output_blob in existing_processed_files:
            logger.info(f"{output_blob} already exists, skipping")
            continue
        else:
            logger.info(f"Working on date {date} to generate {output_blob}")

            with (tempfile.TemporaryDirectory() as td):

                processed_file_path = os.path.join(td, output_blob)
                raw_file_path = processed_file_path.replace(".tiff", ".nc4").replace("processed", "raw")

                RAW_OUTPATH = output_blob.replace(".tiff", ".nc4").replace("processed", "raw")

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

                logger.info(f"Processing {raw_file_path} to COGs: {output_blob}")

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

                upload_file(local_file_path=processed_file_path,
                            sas_token=SAS_TOKEN_DEV,
                            container_name=CONTAINER_RASTER,
                            storage_account=STORAGE_ACCOUNT,
                            blob_path=output_blob,
                            blob_tier=StandardBlobTier.HOT)

                if save_raw:
                    logger.info(f"Saving raw file to: {RAW_OUTPATH}")

                    upload_file(local_file_path=raw_file_path,
                                sas_token=SAS_TOKEN_DEV,
                                container_name=CONTAINER_RASTER,
                                storage_account=STORAGE_ACCOUNT,
                                blob_path=RAW_OUTPATH,
                                content_type="application/octet-stream")

    logger.info("Finished running pipeline.")



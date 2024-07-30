from datetime import datetime
import logging
import os
import pandas as pd
import requests
import tempfile
from typing import Literal
import xarray as xr

from constants import IMERG_BASE_URL, SAS_TOKEN_DEV, CONTAINER_GLOBAL, STORAGE_ACCOUNT
from src.utils.cloud_utils import get_container_client, upload_file

logger = logging.getLogger()


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

    prefix = f"imerg/v{version}/"
    existing_processed_files = [
        x.name.replace(prefix, "")
        for x in get_container_client(sas_token=SAS_TOKEN_DEV,
                                      container_name=CONTAINER_GLOBAL).list_blobs(
            name_starts_with=f"{prefix}imerg"
        )
    ]

    for date in pd.date_range(
            start_date, end_date - pd.DateOffset(days=1)
    ):
        output_blob = (
            f"{prefix}imerg-daily-late-{date.strftime('%Y-%m-%d')}.tif"
        )

        if output_blob in existing_processed_files:
            logger.info(f"{output_blob} already exists, skipping")
            continue
        else:
            logger.info(f"Working on date {date} to generate {output_blob}")

            with tempfile.TemporaryDirectory() as td:

                tp = os.path.join(td, output_blob.replace(".tif", ".nc4"))
                temp_base = os.path.basename(tp)

                version_letter = "B" if version == 7 else ""
                url = IMERG_BASE_URL.format(
                    run=run, date=date, version=version, version_letter=version_letter
                )
                result = requests.get(url)
                result.raise_for_status()

                if not os.path.exists(os.path.dirname(tp)):
                    os.makedirs(os.path.dirname(tp), exist_ok=True)

                f = open(tp, "wb")
                f.write(result.content)
                f.close()

                if save_raw:

                    RAW_OUTPATH = os.path.join("imerg", "raw", f"v{version}", temp_base)
                    logger.info(f"Saving raw file to: {RAW_OUTPATH}")

                    upload_file(local_file_path=tp,
                                sas_token=SAS_TOKEN_DEV,
                                container_name=CONTAINER_GLOBAL,
                                blob_path=RAW_OUTPATH)

                PROCESSED_OUTPATH = os.path.join("imerg", f"v{version}", temp_base.replace(".nc4", ".tif"))
                logger.info(f"Processing {temp_base} to COGs: {PROCESSED_OUTPATH}")

                ds = xr.open_dataset(tp)
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
                da.rio.to_raster(tp, driver="COG")

                upload_file(local_file_path=tp,
                            sas_token=SAS_TOKEN_DEV,
                            container_name=CONTAINER_GLOBAL,
                            blob_path=PROCESSED_OUTPATH)

    logger.info("Finished running pipeline.")



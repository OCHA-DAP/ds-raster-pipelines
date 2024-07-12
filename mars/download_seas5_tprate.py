import os
import tempfile

import pandas as pd
from ecmwfapi import ECMWFService

from constants import BBOX_GLOBAL, CONTAINER_GLOBAL, SAS_TOKEN_DEV, STORAGE_ACCOUNT_DEV
from utils.cloud_utils import upload_file

BBOX_STR = "/".join(
    [
        str(round(coord, 1))
        for coord in [
            BBOX_GLOBAL[3],
            BBOX_GLOBAL[0],
            BBOX_GLOBAL[1],
            BBOX_GLOBAL[2],
        ]
    ]
)

server = ECMWFService("mars")


def download_seas5(start_year, end_year):
    for year in range(START_YEAR, END_YEAR):
        print(f"downloading {year}")

        with tempfile.TemporaryDirectory() as td:

            # create outpath in temp dir
            tp = f"seas5_mars_tprate_{year}.grib"
            temp_base = os.path.basename(tp)
            BLOB_OUTPATH = os.path.join("mars", "raw", temp_base)

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
                    "area": BBOX_STR,
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
                tp,
            )

            upload_file(
                local_file_path=tp,
                sas_token=SAS_TOKEN_DEV,
                container_name=CONTAINER_GLOBAL,
                storage_account=STORAGE_ACCOUNT_DEV,
                blob_path=BLOB_OUTPATH,
            )

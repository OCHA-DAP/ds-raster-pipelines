import xarray as xr
import fsspec
from azure.storage.blob import BlobServiceClient
import tempfile
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")


def _to_leadtime_idx(cur_month, leadtime_month):
    if leadtime_month < cur_month:
        leadtime_month += 12
    return leadtime_month - cur_month


def _to_leadtime_months(cur_month, n_leadtime_months):
    return [(cur_month + i - 1) % 12 + 1 for i in range(n_leadtime_months)]


def download_aws(month, lt_month, dir, save_to_cloud=True):
    fname = f"ecmwf/T8L{month:02}010000{lt_month:02}______1"
    s3_path = f"s3://{BUCKET_NAME}/{fname}"
    fs = fsspec.filesystem("s3")
    file_name = os.path.basename(s3_path)
    file_name = f"{file_name}.grib"
    path_raw = os.path.join(dir, file_name)
    with fs.open(s3_path) as f:
        with open(path_raw, "wb") as temp_file:
            temp_file.write(f.read())
    return path_raw


def process_aws(month, lt_month, path_raw, dir, year=2024, save_to_cloud=True):
    lt = _to_leadtime_idx(cur_month, lt_month)
    path_processed = os.path.join(
        dir, f"seas5_mars_tprate_em_i{year}-{month:02}-01_lt{lt}.tif"
    )
    # Take the ensemble mean and write out to COG
    ds = xr.open_dataset(
        path_raw, engine="cfgrib", filter_by_keys={"dataType": "fcmean"}
    )
    ds_mean = ds.mean(dim="number")
    ds_mean = ds_mean.rio.write_crs("EPSG:4326", inplace=False)
    ds_mean.rio.to_raster(path_processed, driver="COG")
    return


dir = "test_output"
now = datetime.now()
cur_month = int(now.strftime("%m"))
year = now.strftime("%Y")

for lt_month in _to_leadtime_months(cur_month, 7):
    path_raw = download_aws(cur_month, lt_month, dir)
    process_aws(cur_month, lt_month, path_raw, dir)

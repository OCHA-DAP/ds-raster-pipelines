import os

from dotenv import load_dotenv

load_dotenv()

SAS_TOKEN_DEV = os.getenv("DSCI_AZ_SAS_DEV")
SAS_TOKEN_PROD = os.getenv("DSCI_AZ_SAS_PROD")
CONTAINER_GLOBAL = os.getenv("CONTAINER_GLOBAL")
CONTAINER_RASTER = os.getenv("CONTAINER_RASTER")
STORAGE_ACCOUNT_DEV = os.getenv("STORAGE_ACCOUNT_DEV")
STORAGE_ACCOUNT_PROD = os.getenv("STORAGE_ACCOUNT_PROD")

# For input to IMERG
IMERG_BASE_URL = (
    "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGD"
    "{run}.0{version}/{date:%Y}/{date:%m}/3B-DAY-{run}.MS.MRG.3IMERG."
    "{date:%Y%m%d}-S000000-E235959.V0{version}{version_letter}.nc4"
)


ERA5_SETTINGS = {
    "container_name": "raster",
    "raw_path": "era5/monthly/raw",
    "processed_path": "era5/monthly/processed",
    "metadata": {
        "units": "mm/day",
        "averaging_period": "monthly",
        "grid_resolution": 0.25,
        "source": "ECMWF",
        "product": "ERA5 Reanalysis",
    },
}

SEAS5_SETTINGS = {
    "container_name": "raster",
    "raw_path": "seas5/raw",
    "processed_path": "seas5/processed",
    "bbox": {
        "dev": [60, 29, 75, 38],
        "prod": [-180, -90, 180, 90],
        # Use smaller bbox to test locally so data retrieval takes less time
        "local": [60, 29, 75, 38],
    },
    "metadata": {
        "units": "mm/day",
        "averaging_period": "monthly",
        "grid_resolution": 0.4,
        "source": "ECMWF",
        "product": "SEAS5 Seasonal Forecasts",
        "leadtime_units": "months",
    },
}

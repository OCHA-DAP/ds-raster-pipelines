import os

from dotenv import load_dotenv

load_dotenv()

BBOX_GLOBAL = [-180, -90, 180, 90]
BBOX_TEST = [60, 29, 75, 38]

SAS_TOKEN_DEV = os.getenv("DSCI_AZ_SAS_DEV")
SAS_TOKEN_PROD = os.getenv("DSCI_AZ_SAS_PROD")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
CONTAINER_GLOBAL = "global"
CONTAINER_RASTER = "raster"


# For input to IMERG
IMERG_BASE_URL = (
    "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGD"
    "{run}.0{version}/{date:%Y}/{date:%m}/3B-DAY-{run}.MS.MRG.3IMERG."
    "{date:%Y%m%d}-S000000-E235959.V0{version}{version_letter}.nc4"
)
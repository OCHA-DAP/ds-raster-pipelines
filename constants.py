import os

from dotenv import load_dotenv

load_dotenv()

BBOX_GLOBAL = [-180, -90, 180, 90]
BBOX_TEST = [60, 29, 75, 38]

SAS_TOKEN_DEV = os.getenv("DSCI_AZ_SAS_DEV")
SAS_TOKEN_PROD = os.getenv("DSCI_AZ_SAS_PROD")
CONTAINER_GLOBAL = "global"
CONTAINER_RASTER = "raster"
STORAGE_ACCOUNT_DEV = "imb0chd0dev"
STORAGE_ACCOUNT_PROD = "imb0chd0prod"

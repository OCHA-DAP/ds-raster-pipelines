import os

from dotenv import load_dotenv

load_dotenv()

BBOX_GLOBAL = [-180, -90, 180, 90]
SAS_TOKEN_DEV = os.getenv("DSCI_AZ_SAS_DEV")
CONTAINER_GLOBAL = "global"
STORAGE_ACCOUNT_DEV = "imb0chd0dev"
STORAGE_ACCOUNT_PROD = "imb0chd0prod"

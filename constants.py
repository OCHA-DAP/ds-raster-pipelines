import os

from dotenv import load_dotenv

load_dotenv()

BBOX_GLOBAL = [-180, -90, 180, 90]
SAS_TOKEN_DEV = os.getenv("DSCI_AZ_SAS_DEV")
CONTAINER_GLOBAL = "global"
STORAGE_ACCOUNT_DEV = "imb0chd0dev"
STORAGE_ACCOUNT_PROD = "imb0chd0prod"

# For input to MARS API
BBOX_STR_GLOBAL = "/".join(
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

BBOX_TEST = [60, 29, 75, 38]
BBOX_STR_TEST = "/".join(
    [
        str(round(coord, 1))
        for coord in [
            BBOX_TEST[3],
            BBOX_TEST[0],
            BBOX_TEST[1],
            BBOX_TEST[2],
        ]
    ]
)

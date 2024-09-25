import os

import yaml
from dotenv import load_dotenv

load_dotenv()

SAS_TOKEN_DEV = os.getenv("DSCI_AZ_SAS_DEV")
SAS_TOKEN_PROD = os.getenv("DSCI_AZ_SAS_PROD")
CONTAINER_RASTER = os.getenv("CONTAINER_RASTER")
STORAGE_ACCOUNT_DEV = os.getenv("STORAGE_ACCOUNT_DEV")
STORAGE_ACCOUNT_PROD = os.getenv("STORAGE_ACCOUNT_PROD")


def load_pipeline_config(pipeline_name):
    config_path = os.path.join(os.path.dirname(__file__), f"{pipeline_name}_config.yml")
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)

    # Process formatted strings
    for key in ["raw_path", "processed_path", "base_url"]:
        if key in config and isinstance(config[key], str):
            config[key] = config[key].replace("\n", "").strip()

    return config

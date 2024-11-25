import logging

import coloredlogs
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContentSettings,
    StandardBlobTier,
)

from ..config.settings import (
    SAS_TOKEN_DEV,
    SAS_TOKEN_PROD,
    STORAGE_ACCOUNT_DEV,
    STORAGE_ACCOUNT_PROD,
)

logger = logging.getLogger(__name__)
coloredlogs.install(
    level="DEBUG",
    logger=logger,
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def download_from_azure(
    blob_service_client, container_name, blob_path, local_file_path
):
    """
    Download a file from Azure Blob Storage.

    Args:
    blob_service_client (BlobServiceClient): The Azure Blob Service Client
    container_name (str): The name of the container
    blob_path (str or Path): The path of the blob in the container
    local_file_path (str or Path): The local path where the file should be saved

    Returns:
    bool: True if download was successful, False otherwise
    """
    try:
        # Get the blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=str(blob_path)
        )

        # Download the blob
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        logger.info(f"Successfully downloaded blob {blob_path} to {local_file_path}")
        return local_file_path

    except ResourceNotFoundError:
        logger.warning(f"Blob {blob_path} not found")

    except Exception as e:
        logger.error(f"An error occurred while downloading {blob_path}: {str(e)}")

    return None

def blob_client(mode):
    if mode == "dev":
        storage_account = STORAGE_ACCOUNT_DEV
        sas_token = SAS_TOKEN_DEV
    if mode == "prod":
        storage_account = STORAGE_ACCOUNT_PROD
        sas_token = SAS_TOKEN_PROD
    account_url = f"https://{storage_account}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=sas_token)


def upload_file(
    sas_token,
    container_name,
    storage_account,
    local_file_path,
    blob_path,
    blob_tier=StandardBlobTier.COOL,
    content_type="application/octet-stream",
):
    """
    Uploads a single file from 'local_file_path'
    to 'blob_path' in Azure Blob Storage.
    """
    base_url = f"https://{storage_account}.blob.core.windows.net"
    sas_url = f"{base_url}/{container_name}/{blob_path}" f"?{sas_token}"

    blob_client = BlobClient.from_blob_url(blob_url=sas_url)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True,
            standard_blob_tier=blob_tier,
            content_settings=ContentSettings(content_type=content_type),
        )


def upload_file_by_mode(
    mode,
    container_name,
    local_file_path,
    blob_path,
    blob_tier=StandardBlobTier.COOL,
    content_type="application/octet-stream",
):
    """
    A thin wrapper on `upload_file()` that handles credentials according to
    `dev` vs `prod` mode.
    """
    sas_token = SAS_TOKEN_PROD if mode == "prod" else SAS_TOKEN_DEV
    storage_account = STORAGE_ACCOUNT_PROD if mode == "prod" else STORAGE_ACCOUNT_DEV
    upload_file(
        local_file_path=local_file_path,
        sas_token=sas_token,
        container_name=container_name,
        storage_account=storage_account,
        blob_path=blob_path,
        blob_tier=blob_tier,
        content_type=content_type,
    )

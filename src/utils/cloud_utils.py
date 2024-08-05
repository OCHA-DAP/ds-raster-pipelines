import logging

from azure.storage.blob import BlobClient, StandardBlobTier, ContentSettings
from azure.storage.blob import ContainerClient

from constants import STORAGE_ACCOUNT

BASE_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"

logger = logging.getLogger()


def get_container_client(sas_token, container_name) -> ContainerClient:
    account_url = f"{BASE_URL}/{container_name}/" f"?{sas_token}"
    return ContainerClient.from_container_url(account_url)


def upload_file(
    sas_token,
    container_name,
    storage_account,
    local_file_path,
    blob_path,
    blob_tier=StandardBlobTier.COOL,
    content_type="image/tiff",
):
    """
    Uploads a single file from 'local_file_path'
    to 'blob_path' in Azure Blob Storage.
    """
    base_url = f"https://{storage_account}.blob.core.windows.net"  # noqa E231
    sas_url = f"{base_url}/{container_name}/{blob_path}" f"?{sas_token}"

    blob_client = BlobClient.from_blob_url(blob_url=sas_url)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True,
            standard_blob_tier=blob_tier,
            content_settings=ContentSettings(content_type=content_type),
        )
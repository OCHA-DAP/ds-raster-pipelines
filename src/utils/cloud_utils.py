from azure.storage.blob import BlobClient, ContentSettings, StandardBlobTier

from constants import (
    SAS_TOKEN_DEV,
    SAS_TOKEN_PROD,
    STORAGE_ACCOUNT_DEV,
    STORAGE_ACCOUNT_PROD,
)


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
        blob_client.upload_blob(data, overwrite=True)


def upload_file_by_mode(
    mode, container_name, local_file_path, blob_path, blob_tier, content_type
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

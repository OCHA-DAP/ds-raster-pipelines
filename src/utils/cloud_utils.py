from azure.storage.blob import BlobClient, ContentSettings, StandardBlobTier


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

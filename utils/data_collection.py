from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Don't forget to run ``az login`` in the command prompt and authenticate!

account_url = "https://firefighterdata.blob.core.windows.net"
default_credential = DefaultAzureCredential()

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url, credential=default_credential)

def upload_file_to_blob(blob_service_client: BlobServiceClient, container_name: str, filepath: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filepath)
    with open(filepath, "rb") as data:
      blob_client.upload_blob(data, overwrite=True)
      print(f"Uploaded {filepath}.")


def download_blob_to_file(blob_service_client: BlobServiceClient, container_name: str, filename: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    with open("data/" + filename, mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())


download_blob_to_file(blob_service_client, 'data', "LFB Mobilisation data from January 2009.zip")
upload_file_to_blob(blob_service_client, 'data', 'requirements.txt')
from urllib.parse import urlparse

from google.cloud import storage
from google.oauth2 import service_account
import os
import requests
import glob
import json

BUCKET_NAME = "deputy_ai_hackathon"
FOLDER_NAME = "generated_mp3"

def sync_bucket_to_local(service_account_path, source_dir_path, prefix, delimiter=None):


    # Create credentials object from the service account file
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    # Create storage client with the credentials
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Make sure the local directory exists
    os.makedirs(source_dir_path, exist_ok=True)

    # List blobs
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter=delimiter)

    print("Checking and downloading files:")
    for blob in blobs:
        # Skip if it's a directory-like prefix
        if blob.name.endswith('/'):
            continue

        # Get just the filename (last part after /)
        filename = os.path.basename(blob.name)
        local_path = os.path.join(source_dir_path, filename)

        # Check if file exists locally
        if not os.path.exists(local_path):
            print(f"Downloading {filename}...")
            blob.download_to_filename(local_path)
            print(f"Downloaded {filename}")
        else:
            print(f"Skipping {filename} - already exists locally")



def upload_file_to_bucket(file_path, service_account_path):
    # Constants


    # Create credentials object from the service account file
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    # Create storage client with the credentials
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Get just the filename from the path
    filename = os.path.basename(file_path)

    # Create the destination blob path (in generated_mp3 folder)
    destination_blob_name = f"{FOLDER_NAME}/{filename}"
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    blob.upload_from_filename(file_path)
    print(f"File {filename} uploaded to {destination_blob_name}")

    # Make the blob public
    blob.make_public()

    # Get the public URL
    public_url = blob.public_url
    print(f"File publicly accessible at: {public_url}")

    return public_url





def get_gcloud_json_file(folder_path: str) -> str:
    # Create folder if it doesn't exist
    os.makedirs(folder_path, exist_ok=True)

    # Get all JSON files in the folder
    json_files = glob.glob(os.path.join(folder_path, "*.json"))

    # Check number of JSON files
    if len(json_files) > 1:
        raise ValueError(f"Found {len(json_files)} JSON files in {folder_path}. Only one JSON file should exist.")

    # If one JSON file exists, return its path
    if len(json_files) == 1:
        return json_files[0]

    # If no JSON file exists, download from URL
    credential_url = os.getenv('GCLOUD_CREDENTIAL_URL')
    if not credential_url:
        raise ValueError("GCLOUD_CREDENTIAL_URL environment variable is not set")

    try:
        # Download the file
        response = requests.get(credential_url)
        response.raise_for_status()

        # Verify it's valid JSON
        try:
            json.loads(response.text)
        except json.JSONDecodeError:
            raise ValueError("Downloaded file is not valid JSON")

        # Extract filename from URL
        filename = os.path.basename(urlparse(credential_url).path)
        if not filename.endswith('.json'):
            raise ValueError("URL must point to a JSON file")

        # Save the file
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'w') as f:
            f.write(response.text)

        return file_path

    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download JSON file: {str(e)}")
    except Exception as e:
        raise Exception(f"Error while handling JSON file: {str(e)}")

if __name__ == "__main__":
    # sync_bucket_to_local('deputy_ai_hackathon','russiannewbot23-6bb070d3edee.json',"sample_voice",'source_wav/',delimiter='/')
    # upload_file_to_bucket("requirements.txt",'russiannewbot23-6bb070d3edee.json')
    print(get_gcloud_json_file("credential"))
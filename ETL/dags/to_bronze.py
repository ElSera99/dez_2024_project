import os
import glob
import subprocess

from google.cloud import storage
from dotenv import load_dotenv

def local_dowload(version, sets):
    try:
        url = f'https://d36yt3mvayqw5m.cloudfront.net/public/{version}/{version}-{sets}.tgz'
        subprocess.run(["mkdir", "bronze"],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        subprocess.run(["mkdir", f"bronze/{version}-{sets}"],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        subprocess.run(["wget", "-P", f"bronze/{version}-{sets}", url ],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        subprocess.run(["gunzip", f"bronze/{version}-{sets}/{version}-{sets}.tgz"],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        subprocess.run(["tar", "-xf", f"bronze/{version}-{sets}/{version}-{sets}.tar", "-C", f"bronze/{version}-{sets}"],stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        subprocess.run(["rm", f"bronze/{version}-{sets}/{version}-{sets}.tar"],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
    except:
        print("Error while locally downloading data")

def upload_to_gcs(credentials_file, bucket_name, dataset_version):
    # Initialize the Google Cloud Storage client with the credentials
    storage_client = storage.Client.from_service_account_json(credentials_file)
    # Get the target bucket
    bucket = storage_client.bucket(bucket_name)

    print(credentials_file)
    print(bucket_name)

    for local_file in glob.glob('bronze/*/*/*'):
        print(local_file)
        blob = bucket.blob(f'{dataset_version}/{local_file}')
        print(f'{dataset_version}/{local_file}')
        blob.upload_from_filename(local_file)

if __name__ == "__main__":
    version = "v1.0"
    sets = ["test_meta","trainval_meta"]
    for element in sets:
        local_dowload(version, element)

    load_dotenv('etl_variables.env')

    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    DATASET_VERSION = os.getenv('DATASET_VERSION')

    upload_to_gcs(GOOGLE_APPLICATION_CREDENTIALS,BUCKET_NAME,DATASET_VERSION)

    subprocess.run(['rm', '-rf', 'bronze/'])
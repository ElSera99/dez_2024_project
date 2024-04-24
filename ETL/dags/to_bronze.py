import os
import subprocess
from google.cloud import storage

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

def upload_folder_to_gcs(local_folder_path, bucket_name, destination_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for root, dirs, files in os.walk(local_folder_path):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            blob = bucket.blob(f"{destination_folder}/{relative_path}")
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {local_file_path} to {destination_folder}/{relative_path}.")

if __name__ == "__main__":
    version = "v1.0"
    sets = ["test_meta","trainval_meta"]
    for element in sets:
        local_dowload(version, element)
        upload_folder_to_gcs(f"bronze/{element}", "test-bucket-dez", "bronze")
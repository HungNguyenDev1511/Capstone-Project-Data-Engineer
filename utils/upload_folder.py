from minio import Minio
from helpers import load_cfg
from glob import glob
import os


CFG_FILE = '/home/hungnguyen/lake-house-with-minio/utils/config.yaml'

def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.


    client = Minio(
        endpoint= "localhost:9000",
        access_key= "minio_access_key",
        secret_key= "minio_secret_key",
        secure=False,
    )

    # Create bucket if not exist.
    found = client.bucket_exists(bucket_name= "nyc-taxi")
    if not found:
        client.make_bucket(bucket_name= "nyc-taxi")
    else:
        print(f'Bucket nyc-taxi already exists, skip creating!')

    # put object.
    local_folder = '/home/hungnguyen/lake-house-with-minio/data/column' 
    minio_bucket = 'nyc-taxi' 

    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            minio_path = os.path.relpath(local_path, local_folder)

            client.fput_object(minio_bucket, minio_path, local_path)
            print(f'Uploaded: {minio_path}')

if __name__ == '__main__':
    main()
from minio import Minio
from helpers import load_cfg
from glob import glob
import os

CFG_FILE = '/home/hungnguyen/lake-house-with-minio/utils/config.yaml'

def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    fake_data_cfg = cfg["fake_data"]

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )

    # Create bucket if not exist.
    found = client.bucket_exists(bucket_name=datalake_cfg["bucket_name"])
    if not found:
        client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
    else:
        print(f'Bucket {datalake_cfg["bucket_name"]} already exists, skip creating!')

    # Upload files.
    all_fps = glob(
        os.path.join(
            fake_data_cfg["folder_path"],
            "*.parquet"
        )
    )

    for fp in all_fps:
        print(f"Uploading {fp}")
        client.fput_object(
            bucket_name=datalake_cfg["bucket_name"],
            object_name=os.path.join(
                datalake_cfg["folder_name"],
                os.path.basename(fp)
            ),
            file_path=fp
        )

if __name__ == '__main__':
    main()
from minio import Minio
from helpers import load_cfg
from glob import glob
import os

CFG_FILE = '/home/hungnguyen/Caption-Project/utils/config.yaml'

def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    print(cfg)
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
    all_fps_parquet = glob(
        os.path.join(
            '/home/hungnguyen/Caption-Project/data/taxi-data',
            "*.parquet"
        )
    )

    all_fps_json = glob(
        os.path.join(
            '/home/hungnguyen/Caption-Project/data/taxi-data',
            '**',
            "*.json"
        ),
        recursive = True
    )

    for fp in all_fps_parquet:
        print(f"Uploading {fp}")
        client.fput_object(
            bucket_name=datalake_cfg["bucket_name"],
            object_name=os.path.join(
                datalake_cfg["folder_name"],
                os.path.basename(fp)
            ),
            file_path=fp
        )

    for fp in all_fps_json:
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
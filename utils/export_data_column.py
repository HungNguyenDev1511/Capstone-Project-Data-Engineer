import pandas as pd
from minio import Minio
from helpers import load_cfg
from glob import glob
import os
from deltalake.writer import write_deltalake

def main():
    output_folder = "/home/hungnguyen/lake-house-with-minio/data/column"
    df = pd.read_parquet("/home/hungnguyen/lake-house-with-minio/data/taxi_combined/part0/0-3112e580-4561-42de-a195-5da9db7daf04-0.parquet")
    for column_name in df.columns:
        column_path = os.path.join(f"{output_folder}/{column_name}")
        os.makedirs(column_path)
        df_column = df[[column_name]]
        write_deltalake(column_path, df_column)
if __name__ == '__main__':
    main()

import pandas as pd
from helpers import load_cfg
from glob import glob
import os
from deltalake.writer import write_deltalake

def main():
    # output_folder = "/home/hungnguyen/lake-house-with-minio/data/column"
    df = pd.read_parquet("/home/hungnguyen/Caption-Project/data/taxi/yellow_tripdata_2023-07.parquet")
    output_folder = "/home/hungnguyen/Caption-Project/data/taxi-data"
    write_deltalake(output_folder, df)
if __name__ == '__main__':
    main()

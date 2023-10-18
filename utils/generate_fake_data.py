import pandas as pd
import numpy as np
import os
from helpers import load_cfg
import shutil
from deltalake.writer import write_deltalake

CFG_PATH = '/home/hungnguyen/lake-house-with-minio/utils/config.yaml'

if __name__ == '__main__':
    # Range of timestamp to generate
    start_ts = '26-09-2022'
    end_ts = '25-09-2023'

    # Features to generate
    features = ['pressure', 'velocity', 'speed']

    # Create the timestamp column
    # ts = pd.date_range(start=start_ts, end=end_ts, freq='H')
    # df = pd.DataFrame(ts, columns=['event_timestamp'])
    
    df = pd.read_parquet("/home/hungnguyen/lake-house-with-minio/nyc1.parquet")


    # Random floats in the half-open interval [0.0, 1.0)
    # to add other columns in the dataframe
    # for feature in features:
    #     df[feature] = np.random.random_sample((len(ts),))
    
    # Load our pre-defined config to find where the 
    # fake data path will reside in
    cfg = load_cfg(CFG_PATH)
    fake_data_cfg = cfg["fake_data"]
    num_files = fake_data_cfg["num_files"]

    # Suffle all rows
    df_sampled = df.sample(frac=1).reset_index(drop=True)

    # Split data frame by num_files
    df_splits = np.array_split(df_sampled, num_files)

    for i in range(num_files):
        print(f"Processing file {i}...")
        if os.path.exists(            
            os.path.join(
                fake_data_cfg["folder_path"],
                f"part_{i}"
            )
        ):
            shutil.rmtree(            
                os.path.join(
                    fake_data_cfg["folder_path"],
                    f"part_{i}"
                )
            )
        write_deltalake(
            os.path.join(
                fake_data_cfg["folder_path"],
                f"part_{i}"
            ),
            df_splits[i].reset_index()
        )
        print(f"Generated the file {i} successfully!")
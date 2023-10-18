import pandas as pd
import pyarrow.parquet as pq


if __name__ == '__main__':
#List the paths to the 12 Parquet files
    parquet_file_paths = [
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2022-08.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2022-09.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2022-10.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2022-11.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2022-12.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-01.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-02.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-03.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-04.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-05.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-06.parquet",
        "/home/hungnguyen/lake-house-with-minio/data/taxi/yellow_tripdata_2023-07.parquet"
        # Add the paths for the other files here
    ]

    #Initialize an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()

    #Read and concatenate the Parquet files
    for path in parquet_file_paths:
        table = pq.read_table(path)
        df = table.to_pandas()
        combined_df = pd.concat([combined_df, df])

    #Write the combined DataFrame to a new Parquet file
    combined_output_path = "path_to_combined_file.parquet"
    table = pq.Table.from_pandas(combined_df)
    pq.write_table(table, combined_output_path)
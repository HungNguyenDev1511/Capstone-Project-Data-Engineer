from deltalake.writer import write_deltalake
import pandas as pd
import os
import pyarrow.parquet as pq

#Specify the directory where the Parquet files are stored
parquet_directory = "/home/hungnguyen/Caption-Project/utils/output.parquet"  # Update this to the actual directory path

# #List the Parquet files in the directory
# parquet_files = [os.path.join(parquet_directory, file) for file in os.listdir(parquet_directory) if file.endswith(".parquet")]

# #Initialize an empty DataFrame to store the combined data
# combined_df = pd.DataFrame()

# #Read and concatenate the Parquet files
# for path in parquet_files:
#     table = pq.read_table(path)
#     df = table.to_pandas()
#     combined_df = pd.concat([combined_df, df])

# #Now, combined_df contains the combined data from all Parquet files
# #Define the number of partitions (12 in this case)
# num_partitions = 12

# #Calculate the number of rows in each partition
# rows_per_partition = len(combined_df) // num_partitions

# #Split the combined DataFrame into 12 smaller DataFrames
# dataframes = [combined_df[i * rows_per_partition:(i + 1) * rows_per_partition] for i in range(num_partitions)]

#Specify the output directory for the new Parquet files with Delta Lake logs
output_dir = "/home/hungnguyen/lake-house-with-minio/data/taxi_combined"

#Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

#Write each partitioned DataFrame to a separate Parquet file with Delta Lake log
for i, df in enumerate(parquet_directory):
    partition_dir = os.path.join(output_dir, f"part{i}")
    write_deltalake(partition_dir, df)
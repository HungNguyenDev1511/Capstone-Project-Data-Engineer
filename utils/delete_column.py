import pandas as pd

df = pd.read_parquet('/home/hungnguyen/Caption-Project/data/taxi_combined/part0/0-ec436d91-69d4-42db-b8d0-8ce835ba225b-0.parquet')

column_to_remove = 'Airport_fee'

df = df.drop(columns=[column_to_remove])

df.to_parquet('/home/hungnguyen/Caption-Project/data/taxi_combined/part0/new-parquet.parquet', index= False)

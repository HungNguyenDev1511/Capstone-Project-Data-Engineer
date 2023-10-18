from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import pandas as pd
import numpy as np
import datetime

# Load Delta Lake table
print("*"*80)
dt = DeltaTable("data/pump/part_0")
print("Current Delta table:")
print(dt.to_pandas())
df = pd.DataFrame({
    # Randomize a datetime
    'index': 1000,
    'event_timestamp': [np.random.choice(
        pd.date_range(
            datetime.datetime(2023,9,26),
            datetime.datetime(2023,9,27)
        )
    )],
    'pressure': [np.random.rand()], 
    'velocity': [np.random.rand()],
    'speed': [np.random.rand()]
})
print(df)
# Append to create new versions
# Take a look at this for more details: https://delta.io/blog/2022-10-15-version-pandas-dataset/
write_deltalake(dt, df, mode="append")
print("Final Delta table:")
dt2 = DeltaTable("data/pump/part_0")
print(dt2.to_pandas())
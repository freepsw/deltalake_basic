import pandas as pd

df = pd.read_parquet("./my-delta-table/0-7e5ed444-9c1c-4076-8f08-b913494acd91-0.parquet")
print(df.head(5))

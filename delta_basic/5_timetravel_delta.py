from deltalake import write_deltalake, DeltaTable

dt = DeltaTable("./my-delta-table", version=1).to_pandas()

print(dt.head(10))
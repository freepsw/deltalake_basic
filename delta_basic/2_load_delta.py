from deltalake import DeltaTable

dt = DeltaTable("./my-delta-table")

print(f"Version: {dt.version()}")
print(f"Files: {dt.files()}")
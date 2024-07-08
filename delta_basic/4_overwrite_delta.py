import pandas as pd
from deltalake import write_deltalake, DeltaTable

df = pd.DataFrame({"num": [11, 22], "letter": ["aa", "bb"]})

write_deltalake("./my-delta-table", df, mode="overwrite")

# 3. 저장된 deltalake table 조회
ddf = DeltaTable("./my-delta-table").to_pandas()
print(ddf.head(5))
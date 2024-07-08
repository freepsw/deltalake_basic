from deltalake import DeltaTable, write_deltalake
import pandas as pd

# 1. dataframe 생성
df = pd.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})

# 2. delalake format으로 저장
write_deltalake("./my-delta-table", df)

# 3. 저장된 deltalake table 조회
ddf = DeltaTable("./my-delta-table").to_pandas()
print(ddf.head(5))
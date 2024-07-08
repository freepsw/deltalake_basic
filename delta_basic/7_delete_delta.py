from deltalake import DeltaTable

dt = DeltaTable("./my-delta-table")

# num 칼럼이 2볻 큰 레코드를 삭제
dt.delete("num > 2")

# 저장된 deltalake table 조회
ddf = DeltaTable("./my-delta-table").to_pandas()
print(ddf.head(5))
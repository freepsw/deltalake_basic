from deltalake import DeltaTable

dt = DeltaTable("./my-delta-table")

# num 칼럼이 2볻 큰 레코드를 삭제
del_data = dt.delete("num > 8")

print(del_data)
# 저장된 deltalake table 조회
print(DeltaTable("./my-delta-table").to_pandas())

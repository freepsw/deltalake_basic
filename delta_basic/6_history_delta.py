import json
from deltalake import DeltaTable

dt = DeltaTable("./my-delta-table")

# 테이블의 변경이력을 가져온다.
list_history = dt.history()

# 전체 변경이력을 순서대로 출력한다.
for i in list_history:
    print(json.dumps(i, indent=2))


# deltalake_basic


## Deltalake with python 
- https://delta-io.github.io/delta-rs/
### 0. Create virtual env 
- CentOS 9 환경에서 실행
```
> cd ~
> mkdir ~/deltalake_test
> cd ~/deltalake_test
> python3 -m venv delta_virtualenv
> source delta_virtualenv/bin/activate
(delta_virtualenv) [fre@instance-20240707-225918 deltalake_test]$

```

### 1. Install deltalake library with pip 
```
(delta_virtualenv) > pip install deltalake
(delta_virtualenv) > pip install pandas
```

### 2. Create delta table 
- deltalake 테이블을 생성하는 샘플 코드를 작성 후 실행한다.
```
(delta_virtualenv) > vi delta_basic.py
```

- delta_basic.py
```python
from deltalake import DeltaTable, write_deltalake
import pandas as pd

# 1. dataframe 생성
df = pd.DataFrame({"x": [1,2,3]})

# 2. delalake format으로 저장
write_deltalake("my_table",df)

# 3. 저장된 deltalake table 조회
ddf = DeltaTable("my_table").to_pandas()
print(ddf.head(5))
```

- run python code
```
(delta_virtualenv) > python delta_basic.py
   x
0  1
1  2
2  3

```

### 3. Check deltalake directory
- 데이터가 저장된 parquet 파일 1개와 
- deltalake 형식의 메타정보를 저장한 "_delta_log" 디렉토리가 생성됨
```
> ls -alh my_table/
total 4.0K
-rw-r--r--. 1 freepsw18 freepsw18 580 Jul  8 11:54 0-7e5ed444-9c1c-4076-8f08-b913494acd91-0.parquet
drwxr-xr-x. 2 freepsw18 freepsw18  39 Jul  8 11:54 _delta_log
```

#### 3.1 parquet 파일 확인
- "my_table"이라는 deltalake 테이블 폴더에 저장된 parquet 파일에 실제 데이터가 저장되었는지 확인하는 코드 작성
```
(delta_virtualenv) > vi check_parquet.py
```

- 실제 parquet에 데이터가 저장되었는지 확인
```python
import pandas as pd

# my_table에 저장되어 있는 parquet 파일만 직접 읽어옴
df = pd.read_parquet("./my_table/0-7e5ed444-9c1c-4076-8f08-b913494acd91-0.parquet")
print(df.head(5))
```

- 실제 저장된 데이터가 출력됨. 
```
(delta_virtualenv) > python check_parquet.py
   x
0  1
1  2
2  3
```

#### 3.2 deltalake log 디렉토리 확인
```
> ls -alh my_table/_delta_log/
total 4.0K
-rw-r--r--. 1 freepsw18 freepsw18 1.4K Jul  8 11:54 00000000000000000000.json

> cat  my_table/_delta_log/00000000000000000000.json
```
- 아래와 같은 json 데이터가 확인됨
```json
# deltalake reader & writer protocol version 확인
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}

# 
{
  "metaData": {
    "id": "c84497c6-c420-41e8-8257-19210ebcba5c",
    "name": null,
    "description": null,
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "createdTime": 1720439642288,
    "configuration": {}
  }
}

{
  "add": {
    "path": "0-7e5ed444-9c1c-4076-8f08-b913494acd91-0.parquet",
    "partitionValues": {},
    "size": 580,
    "modificationTime": 1720439642288,
    "dataChange": true,
    "stats": "{\"numRecords\": 3, \"minValues\": {\"x\": 1}, \"maxValues\": {\"x\": 3}, \"nullCount\": {\"x\": 0}}",
    "tags": null,
    "deletionVector": null,
    "baseRowId": null,
    "defaultRowCommitVersion": null,
    "clusteringProvider": null
  }
}

{
  "commitInfo": {
    "timestamp": 1720439642288,
    "operation": "CREATE TABLE",
    "operationParameters": {
      "mode": "ErrorIfExists",
      "protocol": "{\"minReaderVersion\":1,\"minWriterVersion\":2}",
      "metadata": "{\"configuration\":{},\"createdTime\":1720439642288,\"description\":null,\"format\":{\"options\":{},\"provider\":\"parquet\"},\"id\":\"c84497c6-c420-41e8-8257-19210ebcba5c\",\"name\":null,\"partitionColumns\":[],\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"x\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\"}",
      "location": "file:///home/freepsw18/deltalake_test/my_table"
    },
    "clientVersion": "delta-rs.0.18.1"
  }
}
```


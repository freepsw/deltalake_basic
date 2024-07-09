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
# 1. deltalake reader & writer protocol version 확인
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}

# 2. Deltalake Table에 대한 메타 정보 제공
## Schema, partition column, file format, config 설정 등
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

# 3. 현재 파일이 어떻게 생성되었는지 명시
## add는 데이터가 추가되면서 parquet 파일이 생성되었음을 의미함. 
## stats에서 현재 parquet 파일에 대한 통계정보 제공 (향후 데이터 조회시 성능 향상)
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

# 4. 파일이 Commit 되는 시점에 대한 상세 정보
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

### Test example code
```
> cd ~
> sudo dnf install -y git
> git clone https://github.com/freepsw/deltalake_basic.git
> cd deltalake_basic/

# Create virtual env 
> python3 -m venv delta_virtualenv
> source delta_virtualenv/bin/activate
(delta_virtualenv) > pip install deltalake
(delta_virtualenv) > pip install pandas

> ls delta_basic/
0_create_delta.py   2_load_delta.py    4_overwrite_delta.py   6_history_delta.py
1_check_parquet.py  3_append_delta.py  5_timetravel_delta.py  7_delete_delta.py

> cd delta_basic/
```

#### 0. Create Delta Table
```
> python 0_create_delta.py
   num letter
0    1      a
1    2      b
2    3      c

> ls -alh my-delta-table
total 8.0K
drwxr-xr-x. 3 freepsw18 freepsw18   80 Jul  9 11:31 .
drwxr-xr-x. 3 freepsw18 freepsw18 4.0K Jul  9 11:31 ..
-rw-r--r--. 1 freepsw18 freepsw18  870 Jul  9 11:31 0-eb941cae-b8a8-47ec-bd63-8e16db41bbca-0.parquet
drwxr-xr-x. 2 freepsw18 freepsw18   39 Jul  9 11:31 _delta_log

> ls -alh my-delta-table/_delta_log/
total 4.0K
drwxr-xr-x. 2 freepsw18 freepsw18   39 Jul  9 11:31 .
drwxr-xr-x. 3 freepsw18 freepsw18   80 Jul  9 11:31 ..
-rw-r--r--. 1 freepsw18 freepsw18 1.7K Jul  9 11:31 00000000000000000000.json
```

#### 2. Load delta table
```
> python 2_load_delta.py
Version: 0
Files: ['0-eb941cae-b8a8-47ec-bd63-8e16db41bbca-0.parquet']
```

#### 3. Insert(append) data into delta table
```
> python 3_append_delta.py
   num letter
0    8     dd
1    9     ee
2    1      a
3    2      b
4    3      c
```

#### 4. Overwrite data to delta table
- 기존 데이터를 삭제하고, 새로운 데이터만 입력
```
> python 4_overwrite_delta.py
   num letter
0   11     aa
1   22     bb

> ls -alh my-delta-table
total 16K
drwxr-xr-x. 3 freepsw18 freepsw18  192 Jul  9 11:36 .
drwxr-xr-x. 3 freepsw18 freepsw18 4.0K Jul  9 11:31 ..
-rw-r--r--. 1 freepsw18 freepsw18  870 Jul  9 11:31 0-eb941cae-b8a8-47ec-bd63-8e16db41bbca-0.parquet
-rw-r--r--. 1 freepsw18 freepsw18  866 Jul  9 11:35 1-0ed14766-e3d7-4f75-9f0d-ee7a714dd777-0.parquet
-rw-r--r--. 1 freepsw18 freepsw18  866 Jul  9 11:36 2-125f2788-bfad-46ce-aae8-90df04c7b03c-0.parquet
drwxr-xr-x. 2 freepsw18 freepsw18  105 Jul  9 11:36 _delta_log

> ls -alh my-delta-table/_delta_log/
total 12K
drwxr-xr-x. 2 freepsw18 freepsw18  105 Jul  9 11:36 .
drwxr-xr-x. 3 freepsw18 freepsw18  192 Jul  9 11:36 ..
-rw-r--r--. 1 freepsw18 freepsw18 1.7K Jul  9 11:31 00000000000000000000.json
-rw-r--r--. 1 freepsw18 freepsw18  586 Jul  9 11:35 00000000000000000001.json
-rw-r--r--. 1 freepsw18 freepsw18  957 Jul  9 11:36 00000000000000000002.json
```


#### 5. Time travel 
- 초기 버전(version 1)의 데이터로 복구(time travel)
- Append가 실행된 시점의 데이터로 복구
```
> python 5_timetravel_delta.py
   num letter
0    8     dd
1    9     ee
2    1      a
3    2      b
4    3      c

# Time travel을 하는 경우, 
# 해당 버전의 데이터만 조회했기 때문에 별도의 parquet파일이 생성되거나
# _delta_log 파일에 json이 추가되지 않는다. 

> ls -alh my-delta-table
total 16K
drwxr-xr-x. 3 freepsw18 freepsw18  192 Jul  9 11:36 .
drwxr-xr-x. 3 freepsw18 freepsw18 4.0K Jul  9 11:31 ..
-rw-r--r--. 1 freepsw18 freepsw18  870 Jul  9 11:31 0-eb941cae-b8a8-47ec-bd63-8e16db41bbca-0.parquet
-rw-r--r--. 1 freepsw18 freepsw18  866 Jul  9 11:35 1-0ed14766-e3d7-4f75-9f0d-ee7a714dd777-0.parquet
-rw-r--r--. 1 freepsw18 freepsw18  866 Jul  9 11:36 2-125f2788-bfad-46ce-aae8-90df04c7b03c-0.parquet
drwxr-xr-x. 2 freepsw18 freepsw18  105 Jul  9 11:36 _delta_log

> ls -alh my-delta-table/_delta_log/
total 12K
drwxr-xr-x. 2 freepsw18 freepsw18  105 Jul  9 11:36 .
drwxr-xr-x. 3 freepsw18 freepsw18  192 Jul  9 11:36 ..
-rw-r--r--. 1 freepsw18 freepsw18 1.7K Jul  9 11:31 00000000000000000000.json
-rw-r--r--. 1 freepsw18 freepsw18  586 Jul  9 11:35 00000000000000000001.json
-rw-r--r--. 1 freepsw18 freepsw18  957 Jul  9 11:36 00000000000000000002.json
```

#### 6. Get table history
- Delta table의 모든 이력(history)를 읽어와서 출력한다.
```
> python 6_history_delta.py
```
- 아래와 같이 json 포맷으로 확인 가능
```json
{
  "timestamp": 1720524963628,
  "operation": "WRITE",
  "operationParameters": {
    "mode": "Overwrite",
    "partitionBy": "[]"
  },
  "clientVersion": "delta-rs.0.18.1",
  "version": 2
}
{
  "timestamp": 1720524904029,
  "operation": "WRITE",
  "operationParameters": {
    "partitionBy": "[]",
    "mode": "Append"
  },
  "clientVersion": "delta-rs.0.18.1",
  "version": 1
}
{
  "timestamp": 1720524711396,
  "operation": "CREATE TABLE",
  "operationParameters": {
    "protocol": "{\"minReaderVersion\":1,\"minWriterVersion\":2}",
    "mode": "ErrorIfExists",
    "location": "file:///home/freepsw18/deltalake_basic/delta_basic/my-delta-table",
    "metadata": "{\"configuration\":{},\"createdTime\":1720524711396,\"description\":null,\"format\":{\"options\":{},\"provider\":\"parquet\"},\"id\":\"6ef76d94-ce63-4ba6-8266-598a81a2d937\",\"name\":null,\"partitionColumns\":[],\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"num\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"letter\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\"}"
  },
  "clientVersion": "delta-rs.0.18.1",
  "version": 0
}
```

#### 7. Delete record from delta table.
- Delta table의 일부 레코드를 삭제한다. (num 칼럼이 8보다 큰 레코드 삭제)
```
> python 7_delete_delta.py
{'num_added_files': 1, 'num_removed_files': 1, 'num_deleted_rows': 1, 'num_copied_rows': 1, 'execution_time_ms': 3, 'scan_time_ms': 1, 'rewrite_time_ms': 1}
   num letter
0    8     dd
1    1      a
2    2      b
3    3      c
```

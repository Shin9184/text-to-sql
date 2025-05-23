import boto3
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq

# S3 클라이언트
s3 = boto3.client('s3')

# 버킷 및 테이블 설정
bucket = "<DestinationBucket>"
user_info_key = "user_info.json"
add_info_key = "add_info.json"
output_key = "<DestinationPath>/output.parquet"

# S3에서 JSON 파일 읽기
def read_json_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return pd.read_json(io.StringIO(content), lines=False)

# 데이터 불러오기
user_df = read_json_from_s3(bucket, user_info_key)
add_df = read_json_from_s3(bucket, add_info_key)

# 병합
merged_df = pd.merge(user_df, add_df, on="id", how="inner")

# DataFrame → Parquet (in-memory)
table = pa.Table.from_pandas(merged_df)
buf = io.BytesIO()
pq.write_table(table, buf)
buf.seek(0)

# S3에 Parquet 업로드
s3.put_object(Bucket=bucket, Key=output_key, Body=buf.getvalue())

print(f"병합 완료")
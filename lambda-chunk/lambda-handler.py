import json
import boto3
import uuid

s3 = boto3.client('s3')

# S3에 JSON 데이터를 업로드하는 함수
def upload_to_s3(bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Uploaded {key} to s3://{bucket}/{key}")

# 'schema' 파일을 처리하여 chunk_sample_schema.json으로 저장하는 함수
def process_schema_file(chunks, input_bucket, output_prefix):
    # 파일 전체를 contentBody에 담아 하나의 파일로 저장
    formatted_chunk = {
        "fileContents": [
            {
                "contentType": "application/json",
                "contentMetadata": {},
                "contentBody": json.dumps(chunks, ensure_ascii=False)
            }
        ]
    }
    file_name = "chunk_sample_schema.json"
    s3_key = f"{output_prefix}/{file_name}"
    upload_to_s3(input_bucket, s3_key, formatted_chunk)
    return [{"key": s3_key}]

# 'query' 파일 및 기타 파일을 처리하여 chunk_sample_query_N.json으로 저장하는 함수
def process_query_file(chunks, input_bucket, output_prefix):
    all_chunks = []
    for idx, chunk in enumerate(chunks, start=1):
        input_text = chunk.get("input", "").strip()
        query_text = chunk.get("query", "").strip()
        if not input_text or not query_text:
            print(f"Missing input or query in chunk {idx}")
            continue
        # 각 chunk를 별도의 파일로 저장
        formatted_chunk = {
            "fileContents": [
                {
                    "contentType": "application/json",
                    "contentMetadata": {},
                    "contentBody": json.dumps({
                        "input": input_text,
                        "query": query_text
                    }, ensure_ascii=False)
                }
            ]
        }
        file_name = f"chunk_sample_query_{idx}.json"
        s3_key = f"{output_prefix}/{file_name}"
        upload_to_s3(input_bucket, s3_key, formatted_chunk)
        all_chunks.append({"key": s3_key})
    return all_chunks

# 각 파일의 batch를 처리하는 함수 (schema/query 분기)
def handle_file_batch(batch, input_bucket, output_prefix):
    key = batch['key']
    try:
        # S3에서 파일을 읽어옴
        obj = s3.get_object(Bucket=input_bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')
        wrapper = json.loads(content)
        file_contents = wrapper.get('fileContents', [])
        if not file_contents:
            print(f"No fileContents found in {key}")
            return []
        body = file_contents[0].get('contentBody', "")
        if not body:
            print(f"No contentBody in {key}")
            return []
        chunks = json.loads(body)
        key_lower = key.lower()
        # 파일 이름에 따라 분기 처리
        if "schema" in key_lower:
            return process_schema_file(chunks, input_bucket, output_prefix)
        else:
            # query 파일이든 기타 파일이든 동일하게 처리
            return process_query_file(chunks, input_bucket, output_prefix)
    except Exception as e:
        print(f"Failed to process {key}: {e}")
        return []

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    input_bucket = event['bucketName']
    output_prefix = "chunks"
    output_files = []
    # 입력 파일 리스트 순회
    for file_info in event['inputFiles']:
        all_chunks = []
        original_location = file_info.get("originalFileLocation", {})
        file_metadata = file_info.get("fileMetadata", {})
        # 각 파일의 contentBatches 순회
        for batch in file_info['contentBatches']:
            batch_chunks = handle_file_batch(batch, input_bucket, output_prefix)
            all_chunks.extend(batch_chunks)
        # 결과에 originalFileLocation, fileMetadata, contentBatches 포함
        output_files.append({
            "originalFileLocation": original_location,
            "fileMetadata": file_metadata,
            "contentBatches": all_chunks
        })
    # 최종 결과 반환
    result = {
        "outputFiles": output_files
    }
    print(result)
    return result
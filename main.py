import os
import time
import random
import json
from flask import Flask, request, jsonify
import base64
from dotenv import load_dotenv
import itertools

from pydantic import BaseModel, Field
from typing import Optional, List

# Vertex AI SDK for Python
from google import genai
from google.genai.types import (
    FunctionDeclaration,
    GenerateContentConfig,
    GoogleSearch,
    HarmBlockThreshold,
    HarmCategory,
    Part,
    SafetySetting,
    ThinkingConfig,
    Tool,
    ToolCodeExecution,
)
from google.cloud import bigquery, storage
import chardet

load_dotenv()
app = Flask(__name__)

# [END cloudrun_pubsub_server]

# GET Env variables
project_id = os.environ.get('PROJECT_ID', '')
gemini_location = os.environ.get('GEMINI_LOCATION', '')
gemini_model = os.environ.get('GEMINI_MODEL','')
pubsub_topic_name = os.environ.get('PUBSUB_TOPIC_NAME','')
bq_location = os.environ.get('BQ_LOCATION','')

# Define the size threshold (50MB in bytes)
BIG_FILE_THRESHOLD = 50 * 1024 * 1024 - 10
BIG_FILE_SKIP_THRESHOLD = 50 * 1024 * 1024 *1024 -100

# --- Initialize Vertex AI ---
client = genai.Client(vertexai=True, project=project_id, location=gemini_location)

class PIData(BaseModel):
    """Extracted Personal Information Data Model"""
    name: Optional[str] = Field(default=None, description="personal name")
    gender: Optional[str] = Field(default=None, description="gender(Man or Woman)")
    birthday: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    credit_card_no: Optional[str] = Field(default=None, description="credit card number")
    phone_number: Optional[str] = Field(default=None, description="personal phone number")
    address: Optional[str] = Field(default=None, description="full address")
    passport_number: Optional[str] = Field(default=None, description="passport number")
    social_security_number: Optional[str] = Field(default=None, description="social security number")
    drivers_licence_number: Optional[str] = Field(default=None, description="drivers licence number")
    is_sensitive_document: Optional[bool] = Field(
        default=None,
        description="if given document is `medical records` or `family relationship certificates` or `documents containing salary information` or identification information like `passport`, `driving license`, etc. then `true` else `false`"
    )
    email: Optional[str] = Field(default=None, description="email address")
    others: Optional[str] = Field(default=None, description="other information")

# instruction, sturctured ouput schema and prompt
system_instruction = """
You need to extract personal information from a given document or image. Given documents or images include passports from various countries, driver's licence and social security card etc.

Personal information includes person's name, gender, birthday, phone_number, address, passport_number, social_security_number, drivers_licence_number, email etc.
If the name is separately written in the document or image as given/first name and sur/family name, combine those two into a full name.
If the personal information is given in a table format, considering the structure of the table, extract and match the information correctly.
And determine if the given document is `medical records (의료 기록)` or `family relationship certificates (가족관계증명서)` or `identification information (passport, driving license, 여권 등)` or `salary information (급여정보)`.
Check the file name.

The results are output in the following json format, which is a **list of objects (dictionaries)**, allowing for the extraction of information for **multiple individuals**.
If there is no personal information in the given document or image, output should be an **empty list** (`[]`)

 - name: personal name
 - gender: gender(Man or Woman)
 - birthday: YYYY-MM-DD
 - credit_card_no: credit card number
 - phone_number: personal phone number
 - address: full address
 - passport_number: passport number
 - social_security_number: social security number
 - drivers_licence_number: drivers licence number
 - is_sensitive_document: if given document is `medical records` or `family relationship certificates` or `documents containing salary information` or identification information like `passport`, `driving license`, etc. then `true` else `false`
 - email: email address
 - others: other information
"""
prompt = "Please extract the personal information."
response_schema = {
    "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "name": {
        "type": "string"
      },
      "gender": {
        "type": "string"
      },
      "birthday": {
        "type": "string"
      },
      "credit_card_no": {
        "type": "string"
      },
      "phone_number": {
        "type": "string"
      },
      "address": {
        "type": "string"
      },
      "passport_number": {
        "type": "string"
      },
      "social_security_number": {
        "type": "string"
      },
      "drivers_licence_number": {
        "type": "string"
      },
      "is_sensitive_document": {
        "type": "boolean"
      },
      "email": {
        "type": "string"
      },
      "others": {
        "type": "string"
      }
    },
  }
}

def big_file_process(url: str, content_type: str) -> str:
    """
    Processes large text files from GCS by streaming and chunking.

    Args:
        url: The Google Cloud Storage URI of the file (e.g., "gs://bucket/file.csv").
        content_type: The MIME type of the file.

    Returns:
        A JSON string representing the aggregated list of extracted PI data.

    Raises:
        ValueError: If the content_type is not 'text/csv' or URL is invalid.
        Exception: For GCS or other processing errors.
    """
    if not url.startswith("gs://"):
        raise ValueError("URL must start with 'gs://'")
    if content_type != 'text/csv':
        raise ValueError("Content type must be 'text/csv' for big file processing.")

    storage_client = storage.Client(project=project_id)
    bucket_name, blob_name = url.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    result_pi_data = []
    
    print(f"--- Starting big file processing for {url} ---")
    with blob.open("r", encoding="utf-8") as f:
        while True:
            # Read 5000 lines at a time
            chunk_lines = list(itertools.islice(f, 5000))
            if not chunk_lines:
                break
            
            chunk_content = "".join(chunk_lines)
            #print(f"Processing a chunk of {len(chunk_lines)} lines...")
            
            # Call Gemini for the chunk
            pi_data_list_chunk = call_gemini_str(content=chunk_content)
            if pi_data_list_chunk:
                result_pi_data.extend(pi_data_list_chunk)
            del chunk_lines, chunk_content

    # Convert the list of Pydantic objects to a JSON string
    json_compatible_list = [item.model_dump(exclude_none=True) for item in result_pi_data]
    return json.dumps(json_compatible_list, ensure_ascii=False)


def convert_gcs_encoding_to_utf8_cwd(gcs_uri: str, src_encoding: str):
    """
    GCS 파일을 현재 디렉토리로 다운로드하여 지정된 인코딩(src_encoding)을
    UTF-8로 변환한 뒤, GCS에 덮어쓰고 로컬 파일은 삭제합니다.

    Args:
        gcs_uri (str): gs://bucket-name/path/to/file 형식의 URI
        src_encoding (str): 원본 파일의 인코딩 (예: 'euc-kr', 'cp949', 'utf-16')
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError("URI must start with 'gs://'")

    # 1. URI 파싱 및 클라이언트 설정
    parts = gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]
    
    # 파일명 추출 및 로컬 경로 설정
    filename = os.path.basename(blob_name)
    local_input_path = filename
    local_output_path = f"utf8_{filename}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        # 2. 현재 디렉토리로 다운로드
        blob.download_to_filename(local_input_path)

        # 3. 인코딩 변환 (src_encoding -> utf-8)
        with open(local_input_path, "r", encoding=src_encoding) as f_in:
            with open(local_output_path, "w", encoding="utf-8") as f_out:
                for line in f_in:
                    f_out.write(line)

        # 4. GCS에 업로드 (덮어쓰기)
        blob.upload_from_filename(
            local_output_path,
            content_type=blob.content_type  # 기존 Content-Type 유지
        )
        print(f"Successfully converted and uploaded. {gcs_uri} ")

    except UnicodeDecodeError:
        print(f"Error: '{src_encoding}' 인코딩으로 파일을 읽을 수 없습니다. 인코딩을 확인해주세요.")
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # 5. 로컬 파일 정리
        if os.path.exists(local_input_path):
            os.remove(local_input_path)
            
        if os.path.exists(local_output_path):
            os.remove(local_output_path)
            print("Cleanup complete.")

def call_gemini(url: str, type: str):
    """
    Calls the Gemini model with a given file URI and includes retry logic
    with exponential backoff.

    Args:
        url: The Google Cloud Storage URI of the file (e.g., "gs://bucket/file.csv").
        type: The MIME type of the file.

    Returns:
        The response text from the Gemini model as a JSON string.

    Raises:
        ValueError: If the URL does not start with "gs://".
        Exception: If the API call fails after multiple retries.
    """
    if not url.startswith("gs://"):
        raise ValueError("URL must start with 'gs://'")

    encoding = detect_gcs_encoding(url)

    if encoding:
        if encoding.lower() == 'utf-8':
            pass
        elif encoding.lower() == 'utf-16' or encoding.lower() == 'euc-kr':
            convert_gcs_encoding_to_utf8_cwd(url, encoding.lower())
        else:
            convert_gcs_encoding_to_utf8_cwd(url, encoding.lower())

    csv_file = Part.from_uri(file_uri=url, mime_type="text/csv")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Generate content
            response = client.models.generate_content(
                model=gemini_model,
                contents=[csv_file, prompt],
                config=GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=response_schema,
                ),
            )

            result = response.text

            if not result or not result.strip():
                result = "[]"

            return result
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")

            error_str = str(e)
            if "400" in error_str and "INVALID_ARGUMENT" in error_str:
                attempt = max_retries

            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt + random seconds
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Failed to call Gemini API.")
                raise # Re-raise the last exception

def call_gemini_str(content: str) -> List[PIData]:
    """
    Calls the Gemini model with a given string content.

    Args:
        content: The string content to be analyzed.

    Returns:
        A list of PIData objects if successful, otherwise an empty list.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Generate content
            response = client.models.generate_content(
                model=gemini_model,
                contents=[f"{prompt}\n\n{content}"], # Combine prompt and content
                config=GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=response_schema,
                ),
            )

            result_text = response.text

            if not result_text or not result_text.strip():
                return []

            # Parse the JSON string and create a list of PIData objects
            result_json = json.loads(result_text)
            return [PIData(**item) for item in result_json]

        except Exception as e:
            print(f"Error on attempt {attempt + 1} in call_gemini_str: {e}")
            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt + random seconds
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Failed to call Gemini API with string content.")
                return [] # Return empty list after all retries fail

def update_analysis_status(uri: str, status: str, result: str = None):
    """
    BigQuery의 csv_analysis_status 테이블에서 특정 URI의 분석 상태와 결과를 업데이트합니다.
    
    Args:
        uri (str): 업데이트할 대상 파일의 GS URI
        status (str): 상태 값 ('completed', 'failed', 'processing' 중 하나)
        result (str, optional): 분석 결과 JSON 문자열 (nullable)
    """
    
    # 1. Status 유효성 검사
    valid_statuses = ['completed', 'failed', 'processing']
    if status not in valid_statuses:
        raise ValueError(f"Status must be one of: {valid_statuses}")

    client = bigquery.Client(project=project_id, location=bq_location)
    
    # 2. UPDATE 쿼리 작성 (updated 컬럼은 현재 시간으로 자동 갱신)
    query = f"""
        INSERT INTO `{project_id}.csv_parse_ds.csv_analysis_status_result` (uri, status, result)
        VALUES (@uri, @status, @result)
        """
    
    # 3. 쿼리 파라미터 설정 (SQL Injection 방지 및 타입 안전성)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("uri", "STRING", uri),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("result", "STRING", result),
        ]
    )
    
    try:
        # 4. 쿼리 실행 및 완료 대기
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        print(f"-------Successfully updated status to '{status}' for URI: {uri}")
        
    except Exception as e:
        print(f"Failed to update BigQuery status: {e}")
        raise

def detect_gcs_encoding(gcs_uri: str, chunk_size: int = 1024) -> str:
    """
    GCS 파일을 스트리밍으로 열어 초반 1024바이트(chunk_size)만 읽은 뒤
    파일의 인코딩 방식을 감지하여 반환합니다.

    Args:
        gcs_uri (str): gs://bucket-name/path/to/file 형식의 URI
        chunk_size (int): 읽어올 바이트 수 (기본 1024)

    Returns:
        str: 감지된 인코딩 이름 (예: 'utf-8', 'EUC-KR', 'ascii'). 
             감지 실패 시 None 반환.
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError("URI must start with 'gs://'")

    # 1. URI 파싱
    parts = gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]

    try:
        # 2. GCS 클라이언트 및 Blob 초기화
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # 3. 스트리밍으로 열어서 지정된 바이트(1024)만큼만 읽기
        # 'rb' 모드로 열어야 바이너리 데이터를 읽을 수 있습니다.
        with blob.open("rb") as f:
            raw_data = f.read(chunk_size)

        # 4. 인코딩 감지
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']

        print(f"Detected: {encoding} (Confidence: {confidence})")
        
        return encoding

    except Exception as e:
        print(f"Error detecting encoding: {e}")
        return None

# [START cloudrun_pubsub_handler]
@app.route("/", methods=["POST"])
def index():
    """Receive and parse Pub/Sub messages."""
    
    envelope = request.get_json()

    # 1. 요청 JSON 파싱
    if not envelope:
        msg = "no JSON message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 204

    pubsub_message = envelope["message"]

    message_data = ""
    if "data" in pubsub_message:
        # Base64 decode the data
        data_bytes = base64.b64decode(pubsub_message["data"])
        # UTF-8 decode the bytes to get the original string
        message_data = data_bytes.decode("utf-8")
    else:
        message_data = ""

    if not message_data:
        return f"Bad Request: {message_data}", 204

    requested_msg = json.loads(message_data)

    # 2. 필수 파라미터 추출 (url, content_type)
    url = requested_msg.get("uri")
    content_type = requested_msg.get("content_type")
    file_size = requested_msg.get("size")

    if not all([url, content_type, file_size is not None]):
        msg = "Missing 'uri', 'content_type', or 'size' in the request."
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 204

    # 3. Gemini 호출 및 예외 처리
    try:
        print(f"-------Processing request for URL: {url}, Size: {file_size} bytes")

        # # Check if file size exceeds SKIP threshold
        if file_size > BIG_FILE_SKIP_THRESHOLD:
            print(f"File size {file_size} exceeds skip threshold {BIG_FILE_SKIP_THRESHOLD}. Skipping.")
            update_analysis_status(uri=url, status='skipped', result=None)
            # Return 200 to acknowledge message and stop processing
            return jsonify({"status": "skipped", "reason": "file too large"}), 200

        gemini_response_text = ""
        # If file size is over the threshold and it's a CSV, use the big file processor
        if file_size > BIG_FILE_THRESHOLD and content_type == 'text/csv':
            gemini_response_text = big_file_process(url=url, content_type=content_type)
        else:
            # For smaller files or other content types, use the standard call
            gemini_response_text = call_gemini(url=url, type=content_type)
        
        # Gemini 응답(JSON String)을 파이썬 객체로 변환
        response_data = json.loads(gemini_response_text)

        # ---------------------------------------------------------
        # 성공 시 BigQuery 업데이트: Status -> completed, Result -> JSON string
        # ---------------------------------------------------------
        # gemini_response_text 자체가 JSON 문자열이므로 이를 그대로 result 컬럼에 저장합니다.
        update_analysis_status(uri=url, status='completed', result=gemini_response_text)

        
        # 성공 응답 반환 (200 OK)
        return jsonify(response_data), 200

    except ValueError as ve:
        # url 검증 실패 등 입력값 오류 처리 (400 Bad Request)
        print(f"Validation error: {ve}")

        # 실패 시 BigQuery 업데이트: Status -> failed, Result -> NULL (None)
        if url: 
            try:
                update_analysis_status(uri=url, status='failed', result=None)
            except Exception as e:
                print(f"Failed to update status to failed in BigQuery: {e}")


        return f"Bad Request: {ve}", 204
        
    except Exception as e:
        # Gemini API 호출 실패 등 서버 내부 오류 처리 (500 Internal Server Error)
        print(f"Internal Server Error: {e}")
        # ---------------------------------------------------------
        # 실패 시 BigQuery 업데이트: Status -> failed, Result -> NULL (None)
        # ---------------------------------------------------------
        if url:
            try:
                update_analysis_status(uri=url, status='failed', result=None)
            except Exception as db_e:
                print(f"Failed to update status to failed in BigQuery: {db_e}")
                
        return f"Internal Server Error: {e}", 204

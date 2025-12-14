# Copyright 2019 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START cloudrun_pubsub_server]
import os
import time
import random
import json
from flask import Flask, request, jsonify
import base64

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
from google.cloud import bigquery


app = Flask(__name__)

# [END cloudrun_pubsub_server]

# GET Env variables
project_id = os.environ.get('PROJECT_ID', '')
gemini_location = os.environ.get('GEMINI_LOCATION', '')
gemini_model = os.environ.get('GEMINI_MODEL','')
pubsub_topic_name = os.environ.get('PUBSUB_TOPIC_NAME','')
bq_location = os.environ.get('BQ_LOCATION','')

# --- Initialize Vertex AI ---
client = genai.Client(vertexai=True, project=project_id, location=gemini_location)

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
prompt = "Please extract the personal information for the attached files."
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

    csv_file = Part.from_uri(
            file_uri=url,
            mime_type=type,
        )

    max_retries = 6
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

            print(f"gemini result : {result}")

            return result
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt + random seconds
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Failed to call Gemini API.")
                raise # Re-raise the last exception

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
        UPDATE `{project_id}.csv_parse_ds.csv_analysis_status`
        SET 
            status = @status,
            result = @result,
            updated = CURRENT_TIMESTAMP()
        WHERE uri = @uri
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

# [START cloudrun_pubsub_handler]
@app.route("/", methods=["POST"])
def index():
    """Receive and parse Pub/Sub messages."""
    
    envelope = request.get_json()

    # 1. 요청 JSON 파싱
    if not envelope:
        msg = "no JSON message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]

    print(pubsub_message)


    message_data = ""
    if "data" in pubsub_message:
        # Base64 decode the data
        data_bytes = base64.b64decode(pubsub_message["data"])
        # UTF-8 decode the bytes to get the original string
        message_data = data_bytes.decode("utf-8")
    else:
        message_data = ""

    print(f"Received message: {message_data}")

    if not message_data:
        return f"Bad Request: {message_data}", 400

    requested_msg = json.loads(message_data)

    # 2. 필수 파라미터 추출 (url, content_type)
    url = requested_msg.get("uri")
    content_type = requested_msg.get("content_type")

    if not url or not content_type:
        msg = "Missing 'url' or 'content_type' in the request."
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    # 3. Gemini 호출 및 예외 처리
    try:
        print(f"-------Processing request for URL: {url}, Content-Type: {content_type}")
        
        # call_gemini 함수 호출 (파라미터 전달)
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


        return f"Bad Request: {ve}", 400
        
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
                
        return f"Internal Server Error: {e}", 500

    # { "url": "gs://bucket/file.csv", "content_type": "text/csv"} 형식의 envelope 
    # url 과 content_type 을 

    # if not isinstance(envelope, dict) or "message" not in envelope:
    #     msg = "invalid Pub/Sub message format"
    #     print(f"error: {msg}")
    #     return f"Bad Request: {msg}", 400

    # pubsub_message = envelope["message"]

    # name = "World"
    # if isinstance(pubsub_message, dict) and "data" in pubsub_message:
    #     name = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()

    # print(f"Hello {name}!")

    #return ("", 204)


# [END cloudrun_pubsub_handler]

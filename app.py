from flask import Flask, request, jsonify
import json
import requests
from flask_cors import CORS
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)
CORS(app)

api_key = os.getenv("DOMO_DEVELOPER_TOKEN")
API_URL = os.getenv("API_URL")

system_instruction = {
    "role": "system",
    "content": """You are a Snowflake SQL expert and DOMO Magic ETL expert.

TASK:

1. **Analyze** the provided DOMO Magic ETL JSON:
   - It contains a data transformation pipeline and schema details of all involved tables.

2. **Generate** a complete Snowflake stored procedure named `SP_INSERT_COMPLETE_SALES_FINAL`:
   - Replicate the transformations exactly as per the JSON.
   - Insert the final result into the table `COMPLETE_SALES_FINAL`.
   - Use this exact template:

     USE DATABASE INFORMATION;
     USE SCHEMA PUBLIC;
     USE WAREHOUSE COMPUTE_WH;

     CREATE OR REPLACE PROCEDURE SP_INSERT_COMPLETE_SALES_FINAL()
     RETURNS STRING
     LANGUAGE SQL
     AS
     $$
     BEGIN
         CREATE OR REPLACE TEMP TABLE ... AS
         SELECT ... FROM ...;


         CREATE OR REPLACE TEMP TABLE ... AS
         SELECT ... FROM ...;


         INSERT INTO COMPLETE_SALES_FINAL (...) SELECT ... FROM ...;

         RETURN 'Success';
     END;
     $$;

   - Only use Snowflake-compatible SQL syntax.
   - Add comments describing each step’s transformation.
   - Use only fields present in the JSON — do NOT invent columns or tables.
   - **Don't use -- command lines** until if the step is unclear or misssing data 

3. **Extract** all `sourceName` values linked to `dataSourceName` fields from the JSON (no duplicates).

4. **Don't use any of "\\n" or "\\\\n" — give raw multiline Output** and **Don't use** command lines and steps instructions until if the step is unclear or misssing data 

5. **Output strictly in JSON** (no extra text), in this format:

{
  "sql": "<full stored procedure code here>",
  "datasourceName": ["<source1>", "<source2>", ...]
}
"""
}

@app.route('/generate-sql', methods=['POST'])
def generate_sql():
    try:
        if not request.is_json:
            return jsonify({"error": "Request body must be in JSON format"}), 400

        data = request.get_json()

        if 'inputJson' not in data:
            return jsonify({"error": "'inputJson' key not found in request body"}), 400

        input_json = data['inputJson']

        if isinstance(input_json, str):
            try:
                input_json = json.loads(input_json)
            except json.JSONDecodeError as e:
                return jsonify({
                    "error": "inputJson string is not a valid JSON object",
                    "details": str(e)
                }), 400

        payload = {
            "model": "domo.openai.gpt-4o-mini",
            "input": system_instruction["content"] + "\nDOMO's MAGIC ETL JSON :" + json.dumps(input_json),
            "parameters": {
                "temperature": 0.2
            }
        }

        headers = {
            "Content-Type": "application/json",
            "X-DOMO-Developer-Token": api_key
        }

        response = requests.post(API_URL, headers=headers, json=payload)

        if response.status_code != 200:
            return jsonify({"error": "API call failed", "details": response.text}), 500

        response_content = response.json().get("output", "")

        if not response_content.strip():
            return jsonify({
                "error": "DOMO API returned empty output",
                "raw_output": response_content
            }), 500

        try:
            response_json = json.loads(response_content)
            sql = response_json.get("sql")
            inputs = response_json.get("datasourceName", "[]")

            if not sql:
                return jsonify({
                    "error": "DOMO API output is missing required fields",
                    "raw_output": response_content
                }), 500

            return jsonify({
                "Output": sql,
                "inputs":inputs,
            })

        except json.JSONDecodeError as e:
            return jsonify({
                "error": "Invalid JSON returned from DOMO API",
                "raw_output": response_content,
                "details": str(e)
            }), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=['GET', 'POST'])
def home():
    return "Running"

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

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
    "content": """You are a Snowflake SQL expert and DOMO Magic ETL translator.

TASK:
1. **Analyze** the provided DOMO Magic ETL JSON:
   - Input schemas, transformation steps, execution order, and output table definition.

2. **Generate** a complete Snowflake stored procedure:
   - Procedure name = "SP_<output_table_name>" (prefix "SP_" + output table).
   - Insert final results into the output table defined in JSON.
   - Use only Snowflake SQL syntax.

3. **SQL Template Rules**:
   - Always begin with:
       USE DATABASE <database_from_json>;
       USE SCHEMA <schema_from_json>;
       USE WAREHOUSE <warehouse_from_json>;
   - CREATE OR REPLACE PROCEDURE <procedure_name>()
     RETURNS STRING
     LANGUAGE SQL
     AS
     $$
     BEGIN
         <Each ETL step is a TEMP TABLE , following JSON execution order>
         
         INSERT INTO <output_table_name> (<columns from JSON>)
         SELECT <columns from last step>
         FROM <last_temp_table>;
         
         RETURN 'Success';
     END;
     $$;

4. **Transformation Rules**:
   - Map DOMO “Replace Text” → REPLACE/REGEXP_REPLACE in SQL.
   - “Add Formula / ExpressionEvaluator” → computed columns (CASE, DATE, YEAR, TRIM, etc.).
   - “Rank & Window” → ROW_NUMBER() OVER (...) with QUALIFY or filter.
   - “Filter Rows” → WHERE conditions.
   - “Join Data / MergeJoin” → JOIN using JSON keys.
   - “Select Columns” → final projection, matching output table schema.
   - Remove columns only if marked `"remove": true` in JSON.

5. **STRICT OUTPUT RULES**:
   - Use only table/column names present in the JSON.
   - No hard-coded table names or procedures—derive from JSON.
   - **No `\\n` or escaped newlines—return raw multiline SQL.**
   - No comments unless JSON step is missing or unclear.
   - Output strictly in JSON format:
     {
       "sql": "<full stored procedure here>"
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
            "input": system_instruction["content"] + "\nDOMO's MAGIC ETL JSON :" + json.dumps(input_json)
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

            if not sql:
                return jsonify({
                    "error": "DOMO API output is missing required fields",
                    "raw_output": response_content
                }), 500
                
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            combined_use = []
            other_statements = []
            for stmt in statements:
                if stmt.upper().startswith("USE "):
                    combined_use.append(stmt + ";")
                else:
                    other_statements.append(stmt + ";")

            indexed_sql = {}
            if combined_use:
                indexed_sql["0"] = " ".join(combined_use)
            for i, stmt in enumerate(other_statements, start=1):
                indexed_sql[str(i)] = stmt
            
            return jsonify({
                "Output": indexed_sql
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
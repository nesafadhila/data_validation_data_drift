from flask import Flask, request, jsonify
import pandas as pd
import warnings
import json
import logging
import os
import re
import traceback
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
from pyhive import hive

from evidently.pipeline.column_mapping import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

# ======================================================
# LOAD ENV
# ======================================================
load_dotenv()

# ======================================================
# CONFIG
# ======================================================
API_UPLOAD = os.getenv("API_UPLOAD")
API_DOWNLOAD = os.getenv("API_DOWNLOAD")

HIVE_HOST = os.getenv("HIVE_HOST")
HIVE_PORT = int(os.getenv("HIVE_PORT", 10000))
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "default")

UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "generated_reports")
LOG_PATH = os.getenv("LOG_PATH", "logs/error.log")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)

LOGGER = logging.getLogger(__name__)

warnings.filterwarnings("ignore")

app = Flask(__name__)

# ======================================================
# SAFE DATETIME PARSER
# ======================================================
def parse_dt(dt_str):
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except:
            continue

    LOGGER.warning(f"Failed parsing datetime: {dt_str}")
    return None

# ======================================================
# SAFE HIVE QUERY
# ======================================================
def hive_query(table_id):
    try:
        table = table_id.replace("-", "_")

        if not re.match(r'^[A-Za-z0-9_]+$', table):
            raise ValueError("Invalid table name")

        conn = hive.Connection(
            host=HIVE_HOST,
            port=HIVE_PORT,
            auth='KERBEROS',
            kerberos_service_name="hive",
            database=HIVE_DATABASE
        )

        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM `{table}`")

        df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])

        cursor.close()
        conn.close()

        return df

    except Exception as e:
        LOGGER.error(f"Hive error: {e}", exc_info=True)
        return None

# ======================================================
# SAVE FILES
# ======================================================
def save_files(report, serving_id):
    try:
        html_path = os.path.join(UPLOAD_FOLDER, f"{serving_id}.html")
        json_path = os.path.join(UPLOAD_FOLDER, f"{serving_id}.json")

        report.save_html(html_path)

        with open(json_path, "w") as f:
            json.dump(report.as_dict(), f, indent=4)

        return html_path, json_path

    except Exception as e:
        LOGGER.error(f"Save error: {e}")
        return None, None

# ======================================================
# UPLOAD FILES (SAFE)
# ======================================================
def upload_files(serving_id):
    try:
        html_path = os.path.join(UPLOAD_FOLDER, f"{serving_id}.html")
        json_path = os.path.join(UPLOAD_FOLDER, f"{serving_id}.json")

        url = f"{API_UPLOAD}{serving_id}/upload-xml"

        with open(html_path, "rb") as h, open(json_path, "rb") as j:
            files = [
                ('files', (f"{serving_id}.html", h, 'text/html')),
                ('files', (f"{serving_id}.json", j, 'application/json'))
            ]

            res = requests.post(url, files=files, timeout=30)

        if res.status_code == 200:
            LOGGER.info("Upload success")
            return True
        else:
            LOGGER.error(f"Upload failed: {res.text}")
            return False

    except Exception as e:
        LOGGER.error(f"Upload error: {e}")
        return False

# ======================================================
# CLEAN TEMP FILES
# ======================================================
def cleanup(serving_id):
    for ext in ["html", "json"]:
        path = os.path.join(UPLOAD_FOLDER, f"{serving_id}.{ext}")
        if os.path.exists(path):
            os.remove(path)

# ======================================================
# MAIN DRIFT ROUTE
# ======================================================
@app.route("/drift", methods=["POST"])
def drift():
    try:
        data = request.json

        ref_table = data.get("id_ref")
        cur_table = data.get("id_cur")
        ref_cols = data.get("col_ref", [])
        cur_cols = data.get("col_cur", [])
        threshold = float(data.get("threshold", 0.5))
        serving_id = data.get("report")

        if not all([ref_table, cur_table, ref_cols, cur_cols, serving_id]):
            return jsonify({"error": "Missing required fields"}), 400

        df_ref = hive_query(ref_table)
        df_cur = hive_query(cur_table)

        if df_ref is None or df_cur is None:
            return jsonify({"error": "Hive fetch failed"}), 500

        df_ref = df_ref[ref_cols]
        df_cur = df_cur[cur_cols]

        # Convert numeric safely
        df_ref = df_ref.apply(pd.to_numeric, errors="ignore")
        df_cur = df_cur.apply(pd.to_numeric, errors="ignore")

        # Align columns
        df_cur.columns = df_ref.columns

        report = Report(metrics=[DataDriftPreset(stattest_threshold=threshold)])
        report.run(reference_data=df_ref, current_data=df_cur, column_mapping=ColumnMapping())

        html_path, json_path = save_files(report, serving_id)

        upload_files(serving_id)
        cleanup(serving_id)

        return jsonify({
            "status": "OK",
            "message": "Drift analysis completed",
            "report": serving_id
        })

    except Exception as e:
        LOGGER.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    app.run(
        host=os.getenv("FLASK_HOST", "0.0.0.0"),
        port=int(os.getenv("FLASK_PORT", "PORT")),
        debug=os.getenv("FLASK_DEBUG", "False") == "True"
    )

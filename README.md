# Data Validation API (Great Expectations)

This project provides a REST API for validating data using Great Expectations (v1.3.7).
It retrieves data from Hive (primary source) and falls back to Redis if Hive fails.

---

## 🚀 Features

- Data validation using Great Expectations
- Multiple validation types:
  - Data type validation
  - Missing values check
  - Mean / Min / Max / Std validation
  - Length validation
  - Regex validation
  - Uniqueness & duplicates
  - Row count validation
- Dual data source:
  - Hive (primary)
  - Redis (fallback)
- REST API built with Flask

---

## 🧱 Tech Stack

- Python 3.10.13
- Flask
- Great Expectations 1.3.7
- Pandas
- PyHive
- Redis

---

## 📂 Project Structure

.
├── valid_vers2.py
├── requirements.txt
└── README.md

---

## ⚙️ Installation

### 1. Create virtual environment

python3.10 -m venv venv

### 2. Activate environment

Linux / Mac:
source venv/bin/activate

Windows:
venv\Scripts\activate

### 3. Install dependencies

pip install -r requirements.txt

---

## 🔐 Configuration

Update inside `valid_vers2.py`

### Hive

host = "your-hive-host"  
port = 10000  
principal = "hive/_HOST@YOUR.REALM"  
database = "default"  

### Redis

REDIS_HOST = "your-redis-host"  
REDIS_PORT = 000  
REDIS_DB = 0  

---

## ▶️ Run Application

python valid_vers2.py

Server runs on:
http://<host>:port

---

## 📡 API Endpoints

### Type Validation
GET /type?id=<table>&columns=<column>&type=<datatype>

### Missing Values
GET /miss?id=<table>&columns=<column>

### Mean Validation
GET /mean?id=<table>&columns=<column>&min=<min>&max=<max>

### Standard Deviation
GET /std?id=<table>&columns=<column>&min=<min>&max=<max>

### Min Value
GET /min?id=<table>&columns=<column>&min=<min>&max=<max>

### Max Value
GET /max?id=<table>&columns=<column>&min=<min>&max=<max>

### Length Validation
GET /minl?id=<table>&columns=<column>&min=<min>&max=<max>  
GET /maxl?id=<table>&columns=<column>&min=<min>&max=<max>

### Sum Validation
GET /total?id=<table>&columns=<column>&min=<min>&max=<max>

### Duplicate Check
GET /duplicate?id=<table>&columns=<column>&value=a,b,c

### Uniqueness Check
GET /uniqueness?id=<table>&columns=<column>

### Mode Validation
GET /mode?id=<table>&columns=<column>&value=a,b,c

### Row Count
GET /rows?id=<table>&min=<min>&max=<max>

### Regex Validation
GET /regex?id=<table>&columns=<column>&character=<regex>

---

## 🔄 Data Flow

Request → Hive → (fallback) Redis → Pandas → Great Expectations → JSON Response

---

## 📊 Response Example

{
    "data": {
        "expectation_results": [
            {
                "column": "acronym",
                "expectation_type": "expect_column_values_to_be_of_type",
                "observed_values": null,
                "success": true,
                "unexpected_count": 0,
                "unexpected_list": []
            }
        ],
        "source_used": "hive",
        "success": true
    },
    "message": "Validation successful using hive.",
    "status": "OK",
    "statusCode": 200
}

---

## ⚠️ Notes

- Hive table name automatically replaces "-" with "_"
- Redis supports multiple key types
- Datasource is recreated per request
- Logging file:
  /home/apps/valid/validasi/error.log

---

## 🧪 Example

curl "http://localhost:5522/type?id=my_table&columns=age&type=Integer"

# Data Drift API (Evidently)

This service detects **data drift** between two datasets using **Evidently (v0.6.5)**.

It compares:
- Reference dataset (baseline)
- Current dataset (new data)

---

## 🚀 Features

- Data drift detection
- Numeric & categorical analysis
- Threshold-based drift sensitivity
- Time-based filtering (optional)
- HTML & JSON report generation
- Auto upload to external API
- Temporary file cleanup

---

## 🧱 Tech Stack

- Python 3.10.13
- Flask
- Evidently 0.6.5
- Pandas
- PyHive
- Requests

---

## 📂 Project Structure

.
├── drift.py
├── requirements.txt
└── README.md

---

## ⚙️ Installation

### 1. Create virtual environment

python3.10 -m venv venv

### 2. Activate environment

Linux / Mac:
source venv/bin/activate

Windows:
venv\Scripts\activate

### 3. Install dependencies

pip install -r requirements.txt

---

## 🔐 Configuration

### Hive

host = "your-hive-host"  
port = 10000  
principal = "hive/_HOST@YOUR.REALM"  
database = "default"  

---

### External API

API_UPLOAD = "http://your-upload-api/"  
API_DOWNLOAD = "http://your-download-api/"  

---

## ▶️ Run Application

python drift.py

Server:
http://<host>:port

---

## 📡 API Endpoint

### Data Drift

POST /drift

---

## 📥 Request Example

{
  "id_ref": "table_ref",
  "id_cur": "table_cur",
  "col_ref": ["col1"],
  "col_cur": ["col1"],
  "threshold": 0.5,
  "report": "drift_report"
}

---

## 📤 Response Example

{
    "data": {
        "announcement": 1.0,
        "company": 1.0,
        "title": 1.0
    },
    "html_filename": "test1.html",
    "json_filename": "test1.json",
    "message": "checking the data drift is done",
    "status": "OK",
    "statusCode": 200
}

---

## 🔄 Workflow

Request → Hive → Pandas → Evidently → Save → Upload → Cleanup

---

## 📊 Output

Generated:
- HTML report
- JSON report

Stored in:
generated_reports/

---

## 🧪 Example

curl -X POST http://localhost:3636/drift \
-H "Content-Type: application/json" \
-d {
    "id_ref": "{file_id}",
    "col_ref": ["{file_column}", "{file_column}", "{file_column}"],
    "date_ref1": "2023-05-09 06:31:16.747977",
    "date_ref2": "2023-05-09 06:31:16.747977",

    "id_cur": "{file_id}{file_id}",
    "col_cur": ["{file_column}", "{file_column}", "{file_column}"],
    "date_cur1": "2023-05-09 06:31:16.747977",
    "date_cur2": "2023-05-09 06:31:16.747977",

    "threshold": "0.23",

    "report": "test1"
}

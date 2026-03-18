from flask import Flask, request, jsonify
import json
import logging
import pandas as pd
import great_expectations as gx
import warnings
from pyhive import hive
import redis
import re
import os
from dotenv import load_dotenv

# ======================================================
# LOAD ENV
# ======================================================
load_dotenv()

# ======================================================
# CONFIG
# ======================================================
HIVE_HOST = os.getenv("HIVE_HOST")
HIVE_PORT = int(os.getenv("HIVE_PORT", 10000))
HIVE_PRINCIPAL = os.getenv("HIVE_PRINCIPAL")
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "default")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

LOG_PATH = os.getenv("LOG_PATH", "logs/error.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)

warnings.filterwarnings("ignore")

app = Flask(__name__)

# ======================================================
# HIVE QUERY
# ======================================================
def hive_query(user_id):
    try:
        table = user_id.replace("-", "_")

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
        logging.error(f"Hive error: {e}", exc_info=True)
        return None


# ======================================================
# REDIS QUERY
# ======================================================
def redis_query(key):
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_timeout=5
        )

        base = key.replace("-", "_")
        candidates = {key, base, base.replace("_", "-")}

        for k in r.scan_iter(f"{base}:*"):
            candidates.add(k)

        raw = None

        for k in candidates:
            t = r.type(k)

            if t != "none":
                if t == "hash":
                    raw = r.hgetall(k)
                elif t == "string":
                    raw = {k: r.get(k)}
                elif t == "list":
                    raw = {f"{k}[{i}]": v for i, v in enumerate(r.lrange(k, 0, -1))}
                elif t == "set":
                    raw = {f"{k}[{i}]": v for i, v in enumerate(r.smembers(k))}
                elif t == "zset":
                    raw = {f"{k}[{i}]": v for i, v in enumerate(r.zrange(k, 0, -1))}
                break

        if not raw:
            return None

        clean = {}
        for k, v in raw.items():
            txt = re.sub(r'[\{\}\[\]\"]', '', str(v)).strip()
            clean[k.replace("-", "_")] = txt.split()[-1] if txt else ""

        return pd.DataFrame([clean])

    except Exception as e:
        logging.error(f"Redis error: {e}", exc_info=True)
        return None


# ======================================================
# DATA FETCH
# ======================================================
def get_data(user_id):
    df = hive_query(user_id)
    source = "hive"

    if df is None or df.empty:
        df = redis_query(user_id)
        source = "redis"

    return df, source


# ======================================================
# VALIDATION ENGINE
# ======================================================
def validate(df, func):
    context = gx.get_context()

    ds_name = "runtime_ds"
    asset_name = "runtime_asset"

    for ds in context.list_datasources():
        if ds["name"] == ds_name:
            context.delete_datasource(ds_name)

    ds = context.data_sources.add_pandas(name=ds_name)
    asset = ds.add_dataframe_asset(name=asset_name)
    batch = asset.build_batch_request(options={"dataframe": df})
    validator = context.get_validator(batch_request=batch)

    func(validator)
    res = validator.validate()

    return {
        "success": res["success"],
        "results": [
            {
                "type": r["expectation_config"]["type"],
                "column": r["expectation_config"]["kwargs"].get("column"),
                "success": r["success"],
                "unexpected": r["result"].get("unexpected_list", [])
            }
            for r in res["results"]
        ]
    }


# ======================================================
# ROUTES
# ======================================================

@app.route("/type")
def type_check():
    id = request.args.get("id")
    col = request.args.get("columns")
    typ = request.args.get("type")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_values_to_be_of_type(col, typ))})


@app.route("/miss")
def miss():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_values_to_not_be_null(col))})


@app.route("/mean")
def mean():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna()

    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_mean_to_be_between(col))})


@app.route("/std")
def std():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    df[col] = pd.to_numeric(df[col], errors="coerce")

    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_stdev_to_be_between(col))})


@app.route("/min")
def minv():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_min_to_be_between(col))})


@app.route("/max")
def maxv():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_max_to_be_between(col))})


@app.route("/minl")
def minl():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_value_lengths_to_be_between(col))})


@app.route("/maxl")
def maxl():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_value_lengths_to_be_between(col))})


@app.route("/total")
def total():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_sum_to_be_between(col))})


@app.route("/duplicate")
def duplicate():
    id = request.args.get("id")
    col = request.args.get("columns")
    val = request.args.get("value")

    df, src = get_data(id)
    values = val.split(",") if val else []

    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_distinct_values_to_contain_set(col, values))})


@app.route("/uniqueness")
def uniqueness():
    id = request.args.get("id")
    col = request.args.get("columns")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_values_to_be_unique(col))})


@app.route("/mode")
def mode():
    id = request.args.get("id")
    col = request.args.get("columns")
    val = request.args.get("value")

    df, src = get_data(id)
    values = val.split(",") if val else []

    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_most_common_value_to_be_in_set(col, values))})


@app.route("/rows")
def rows():
    id = request.args.get("id")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_table_row_count_to_be_between())})


@app.route("/regex")
def regex():
    id = request.args.get("id")
    col = request.args.get("columns")
    regex = request.args.get("character")

    df, src = get_data(id)
    return jsonify({"source": src, "data": validate(df, lambda v: v.expect_column_values_to_match_regex(col, regex))})


# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    app.run(
        host=os.getenv("FLASK_HOST", "0.0.0.0"),
        port=int(os.getenv("FLASK_PORT", "PORT")),
        debug=os.getenv("FLASK_DEBUG", "False") == "True"
    )

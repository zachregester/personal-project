import os
import json
import pandas as pd
from datetime import datetime, timezone

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# ---- settings (from .env) ----
JSON_PATH = os.getenv("JSON_PATH", "python_ingestion/terminations_full_nested.json")

ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ROLE = os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE = os.getenv("SNOWFLAKE_TABLE")

if not all([ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA, TABLE]):
    raise ValueError("Missing one or more required Snowflake env vars in .env")


# ---- 1) read json ----
with open(JSON_PATH, "r", encoding="utf-8") as f:
    payload = json.load(f)

# ---- 2) parse into rows ----
rows = []
for r in payload.get("data", []):
    employee = r.get("employee", {})
    job = employee.get("job", {})
    term = r.get("termination", {})
    org = r.get("org", {})

    rows.append({
        "EMPLOYEE_ID": employee.get("employee_id"),
        "IS_CONTRACTOR": employee.get("is_contractor"),
        "JOB_NAME": job.get("name"),
        "JOB_NAME_CODE": job.get("code"),

        "PERIOD_DATE": term.get("period_date"),
        "TERMINATION_DATE": term.get("termination_date"),
        "TERMINATION_TYPE": term.get("termination_type"),
        "TERMINATION_REASON": term.get("termination_reason"),
        "METRIC": term.get("metric"),

        "EXEC_LEADER": org.get("exec_leader"),
        "L1_FROM_TOP_LEADER": org.get("l1_from_top_leader"),
        "L2_FROM_TOP_LEADER": org.get("l2_from_top_leader"),
        "L3_FROM_TOP_LEADER": org.get("l3_from_top_leader"),

        "INGESTED_AT": datetime.now(timezone.utc).isoformat(),
    })

df = pd.DataFrame(rows)

# convert dates
for c in ["PERIOD_DATE", "TERMINATION_DATE"]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

print("Parsed DF:", df.shape)
print(df.head(10))


# ---- 3) connect to snowflake ----
conn = connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
)

try:
    # ---- 4) get destination table columns ----
    sql = f"""
    select column_name
    from {DATABASE}.information_schema.columns
    where table_schema = '{SCHEMA.upper()}'
      and table_name = '{TABLE.upper()}'
    order by ordinal_position
    """
    cur = conn.cursor()
    cur.execute(sql)
    table_cols = [r[0] for r in cur.fetchall()]
    cur.close()

    print("Destination columns found:", len(table_cols))
    if len(table_cols) == 0:
        raise ValueError("No columns found. Check DATABASE/SCHEMA/TABLE or permissions.")

    # ---- 5) align df to match table ----
    df_load = df.copy()
    df_load.columns = [c.upper() for c in df_load.columns]

    # drop extras
    df_load = df_load[[c for c in df_load.columns if c in table_cols]]

    # add missing
    for c in table_cols:
        if c not in df_load.columns:
            df_load[c] = None

    # reorder
    df_load = df_load[table_cols]

    print("Load DF:", df_load.shape)

    # ---- 6) write to snowflake (overwrite table) ----
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_load,
        table_name=TABLE,
        database=DATABASE,
        schema=SCHEMA,
        quote_identifiers=False,
        overwrite=True,
        auto_create_table=False,
    )

    print("write_pandas success=", success, " rows_loaded=", nrows, " chunks=", nchunks)

finally:
    conn.close()

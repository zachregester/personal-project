import os
import json
import pandas as pd

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

JSON_PATH = os.getenv("JSON_PATH", "python_ingestion/terminations_full_nested.json")

ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ROLE = os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE = os.getenv("SNOWFLAKE_TABLE")

with open(JSON_PATH, "r", encoding="utf-8") as f:
    termination_data = json.load(f)

rows = []
for r in termination_data.get("data", []):
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
    })

df = pd.DataFrame(rows)

print("DF shape:", df.shape)
print(df.head(5))

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
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=TABLE,
        database=DATABASE,
        schema=SCHEMA,
        overwrite=True,  # replace table contents
        quote_identifiers=False,
        auto_create_table=False,
    )
    print("Loaded:", success, "rows:", nrows, "chunks:", nchunks)

finally:
    conn.close()


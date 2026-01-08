import os
import json
import pandas as pd
from datetime import datetime, timezone

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv


# ----------------------------
# Config / Env
# ----------------------------
load_dotenv()

JSON_PATH = os.getenv("JSON_PATH", "python_ingestion/terminations_full_nested.json")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")  # password or PAT
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE = os.getenv("SNOWFLAKE_TABLE")


# ----------------------------
# Helpers
# ----------------------------
def require_env(name: str, value: str | None) -> str:
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def main_request(path: str) -> dict:
    """Load the JSON payload (mock API response) from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_data(payload: dict) -> pd.DataFrame:
    """Flatten nested JSON records into a DataFrame."""
    rows: list[dict] = []

    for r in payload.get("data", []):
        employee = r.get("employee") or {}
        job = employee.get("job") or {}

        term = r.get("termination") or {}
        org = r.get("org") or {}

        rows.append(
            {
                # employee
                "EMPLOYEE_ID": employee.get("employee_id"),
                "IS_CONTRACTOR": employee.get("is_contractor"),
                "JOB_NAME": job.get("name"),
                "JOB_NAME_CODE": job.get("code"),
                # termination
                "PERIOD_DATE": term.get("period_date"),
                "TERMINATION_DATE": term.get("termination_date"),
                "TERMINATION_TYPE": term.get("termination_type"),
                "TERMINATION_REASON": term.get("termination_reason"),
                "METRIC": term.get("metric"),
                # org
                "EXEC_LEADER": org.get("exec_leader"),
                "L1_FROM_TOP_LEADER": org.get("l1_from_top_leader"),
                "L2_FROM_TOP_LEADER": org.get("l2_from_top_leader"),
                "L3_FROM_TOP_LEADER": org.get("l3_from_top_leader"),
                # metadata (only loads if your table has this column; otherwise it’ll be dropped)
                "INGESTED_AT": datetime.now(timezone.utc).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Convert to date objects (helps Snowflake types)
    for c in ["PERIOD_DATE", "TERMINATION_DATE"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    return df


def get_table_columns(conn, database: str, schema: str, table: str) -> list[str]:
    """Return destination table columns in ordinal order."""
    sql = f"""
    select column_name
    from {database}.information_schema.columns
    where table_schema = '{schema.upper()}'
      and table_name = '{table.upper()}'
    order by ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]


def align_df_to_table(df: pd.DataFrame, table_cols: list[str]) -> pd.DataFrame:
    """
    Make df match the destination table:
    - uppercase df column names
    - drop any df columns not in table
    - add missing table columns as NULL
    - reorder to exactly match the table
    """
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    # keep only columns that exist in the destination table
    df = df[[c for c in df.columns if c in table_cols]]

    # add any missing table columns
    for c in table_cols:
        if c not in df.columns:
            df[c] = None

    # reorder to match table order
    return df[table_cols]


def connect_snowflake():
    """Create and return a Snowflake connection."""
    account = require_env("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT)
    user = require_env("SNOWFLAKE_USER", SNOWFLAKE_USER)
    password = require_env("SNOWFLAKE_PASSWORD", SNOWFLAKE_PASSWORD)
    database = require_env("SNOWFLAKE_DATABASE", DATABASE)
    schema = require_env("SNOWFLAKE_SCHEMA", SCHEMA)
    warehouse = require_env("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE)

    return connect(
        account=account,
        user=user,
        password=password,
        role=SNOWFLAKE_ROLE,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )


# ----------------------------
# Main
# ----------------------------
def main():
    require_env("SNOWFLAKE_TABLE", TABLE)

    print("Reading JSON from:", JSON_PATH)
    print("Target table:", f"{DATABASE}.{SCHEMA}.{TABLE}")

    payload = main_request(JSON_PATH)
    df = parse_data(payload)

    print("Parsed DF shape:", df.shape)
    print(df.head(10))

    conn = connect_snowflake()
    try:
        table_cols = get_table_columns(conn, DATABASE, SCHEMA, TABLE)
        print("Destination columns found:", len(table_cols))
        if len(table_cols) == 0:
            raise ValueError("No columns found for target table. Check DB/SCHEMA/TABLE and table existence.")

        df_load = align_df_to_table(df, table_cols)
        print("Load DF shape (post-align):", df_load.shape)

        # Overwrite destination table contents
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df_load,
            table_name=TABLE,
            database=DATABASE,
            schema=SCHEMA,
            quote_identifiers=False,   # works best with unquoted standard uppercase tables
            overwrite=True,            # override/replace table contents
            auto_create_table=False,   # use existing table
        )

        print(f"write_pandas success={success}, rows_loaded={nrows}, chunks={nchunks}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()


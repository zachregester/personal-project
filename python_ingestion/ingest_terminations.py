import os
import json
import pandas as pd

from datetime import datetime, timezone
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

JSON_PATH = os.getenv("JSON_PATH", "python_ingestion/terminations_data.json")

def main_request(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_data(payload: dict) -> pd.DataFrame:
    rows = []
    for r in payload.get("data", []):
        employee = r.get("employee", {}) or {}
        job = employee.get("job", {}) or {}

        term = r.get("termination", {}) or {}
        org = r.get("org", {}) or {}

        rows.append({
            # employee
            "employee_id": employee.get("employee_id"),
            "is_contractor": employee.get("is_contractor"),
            "job_name": job.get("name"),
            "job_name_code": job.get("code"),

            # termination
            "period_date": term.get("period_date"),
            "termination_date": term.get("termination_date"),
            "termination_type": term.get("termination_type"),
            "termination_reason": term.get("termination_reason"),
            "metric": term.get("metric"),

            # org
            "exec_leader": org.get("exec_leader"),
            "l1_from_top_leader": org.get("l1_from_top_leader"),
            "l2_from_top_leader": org.get("l2_from_top_leader"),
            "l3_from_top_leader": org.get("l3_from_top_leader"),
        })

    df = pd.DataFrame(rows)

    # Optional: make dates actual dates (helps Snowflake typing)
    for c in ["period_date", "termination_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    return df

def main():
    payload = main_request(JSON_PATH)
    df = parse_data(payload)

    print(df.head(10))
    print("rows, cols:", df.shape)
    print(df.dtypes)

if __name__ == "__main__":
    main()


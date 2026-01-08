import os
import json
import pandas as pd

from datetime import datetime, timezone
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

JSON_PATH = os.getenv("JSON_PATH", "data/terminations_nested.json")

DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE = os.getenv("SNOWFLAKE_TABLE")


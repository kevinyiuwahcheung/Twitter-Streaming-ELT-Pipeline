import pandas as pd
import numpy as np
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer
import datetime

engine = create_engine(URL(
    account = "",
    user = "",
    password = "",
    database = "",
    schema = "",
    warehouse = ""
))

conn = engine.connect()
query = """
INSERT INTO FAKER_NORMAL
SELECT 
    RECORD_CONTENT:id_str,
    RECORD_CONTENT:text,
    RECORD_CONTENT:retweeted_status.extended_tweet.full_text,
    RECORD_CONTENT:user.id_str,
    RECORD_CONTENT:user.name,
    RECORD_METADATA
FROM FAKER
WHERE RECORD_METADATA:CreateTime > (SELECT max(METADATA:CreateTime) FROM FAKER_NORMAL);
"""
df = pd.read_sql(query, conn)
conn.close()

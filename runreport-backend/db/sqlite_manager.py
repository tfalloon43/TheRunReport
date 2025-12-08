import sqlite3
import pandas as pd

class SQLiteManager:
    def __init__(self, path="local.db"):
        self.conn = sqlite3.connect(path)

    def write_df(self, table_name: str, df: pd.DataFrame):
        df.to_sql(table_name, self.conn, if_exists="replace", index=False)
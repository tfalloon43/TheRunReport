import sqlite3
import pandas as pd
from pathlib import Path


class SQLiteManager:
    """
    SQLite wrapper that ALWAYS stores the DB in:
        runreport-backend/0_db/local.db
    unless an absolute path is explicitly provided.
    """

    def __init__(self, path="local.db"):
        path = Path(path)

        # If a relative path was given → force it into /0_db/
        if not path.is_absolute():
            backend_root = Path(__file__).resolve().parents[1]  # runreport-backend/
            db_dir = backend_root / "0_db"
            db_dir.mkdir(parents=True, exist_ok=True)
            path = db_dir / path  # local.db inside the correct folder

        self.path = path
        print(f"📌 SQLite DB path → {self.path}")

        # Connect to the database
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys = ON;")

    # ------------------------------------------------------------
    def write_df(self, table_name: str, df: pd.DataFrame):
        try:
            df.to_sql(table_name, self.conn, if_exists="replace", index=False)
            self.conn.commit()
            print(f"✔ Wrote {len(df):,} rows to '{table_name}'")
        except Exception as e:
            print(f"❌ Error writing to '{table_name}': {e}")
            raise

    # ------------------------------------------------------------
    def fetch_df(self, query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, self.conn)

    # ------------------------------------------------------------
    def close(self):
        if self.conn:
            self.conn.close()
            print("🔌 SQLite connection closed.")
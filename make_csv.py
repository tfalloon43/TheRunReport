# make_csv.py
import sqlite3
import pandas as pd
import os

DB_PATH = "pdf_data.sqlite"
EXPORT_DIR = "csv_exports"

def export_to_csv():
    # Ensure export directory exists
    os.makedirs(EXPORT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    # List of tables to export
    tables = [
        "escapement_daily",
        "escapement_weekly",
        "escapement_weekly_clean",
        "escapement_weekly_avg",
    ]

    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            csv_path = os.path.join(EXPORT_DIR, f"{table}.csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ Exported {table} → {csv_path}")
        except Exception as e:
            print(f"⚠️ Could not export {table}: {e}")

    conn.close()
    print("All exports complete.")

if __name__ == "__main__":
    export_to_csv()
    
# step21_manualdeletions.py
# ------------------------------------------------------------
# Step 21 (Flows): Manual cleanup — delete bad timestamp rows.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

print("🧹 Step 21 (Flows): Deleting rows with bad timestamps...")

project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

tables = ["NOAA_flows", "USGS_flows"]
bad_timestamp_like = "12-31-2025, %"

with sqlite3.connect(db_path) as conn:
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} WHERE timestamp LIKE ?;",
            (bad_timestamp_like,),
        )
        count = cursor.fetchone()[0]
        if count:
            cursor.execute(
                f"DELETE FROM {table} WHERE timestamp LIKE ?;",
                (bad_timestamp_like,),
            )
            print(
                f"✅ {table}: deleted {count} row(s) with bad timestamp pattern {bad_timestamp_like}."
            )
        else:
            print(f"✅ {table}: no rows to delete.")

print("✅ Step 21 complete.")

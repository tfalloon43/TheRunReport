# step20_removenegatives.py
# ------------------------------------------------------------
# Step 20 (Flows): Replace negative flow values with NaN and trim timestamps.
#
# If any negative values appear in flow_cfs for either USGS_flows
# or NOAA_flows, set them to NaN so plots stop instead of dipping
# below zero. Also trim timestamp strings to MM-DD-YYYY, HH-MM.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🧹 Step 20 (Flows): Converting negative flow values to NaN and trimming timestamps...")

# ------------------------------------------------------------
# DB PATH / TABLES
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
TABLES = ["USGS_flows", "NOAA_flows"]

print(f"🗄️ Using DB → {db_path}")


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    )
    return cur.fetchone() is not None


with sqlite3.connect(db_path) as conn:
    for table in TABLES:
        if not table_exists(conn, table):
            print(f"⚠️ Table missing: {table} — skipping.")
            continue

        df = pd.read_sql_query(f"SELECT * FROM {table};", conn)
        if df.empty:
            print(f"ℹ️ {table} is empty — skipping.")
            continue

        if "flow_cfs" not in df.columns:
            print(f"⚠️ {table} missing 'flow_cfs' — skipping.")
            continue

        if "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"], errors="coerce")
            df.loc[ts.notna(), "timestamp"] = ts.dt.strftime("%m-%d-%Y, %H-%M")

        df["flow_cfs"] = pd.to_numeric(df["flow_cfs"], errors="coerce")
        negatives = (df["flow_cfs"] < 0).sum()
        if negatives:
            df.loc[df["flow_cfs"] < 0, "flow_cfs"] = pd.NA
            print(f"🛑 {table}: replaced {int(negatives):,} negative flow_cfs values with NaN.")
        else:
            print(f"✅ {table}: no negative flow_cfs values found.")

        df.to_sql(table, conn, if_exists="replace", index=False)

print("✅ Step 20 complete — negative flow values cleared.")

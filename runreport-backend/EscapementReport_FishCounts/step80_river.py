# step80_river.py
# ------------------------------------------------------------
# Step 80: Extract river name from identifier
#
# Adds a 'river' column to EscapementReport_PlotData where
# river = text before the first " - " in identifier.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🌊 Step 80: Extracting river from identifier...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM EscapementReport_PlotData;", conn)
print(f"📂 Loaded {len(df):,} rows from EscapementReport_PlotData")

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
if "identifier" not in df.columns:
    raise ValueError("❌ Column 'identifier' is missing from EscapementReport_PlotData.")

# ------------------------------------------------------------
# EXTRACT RIVER
# ------------------------------------------------------------
def extract_river(identifier: str) -> str:
    if not isinstance(identifier, str):
        return ""
    return identifier.split(" - ")[0].strip()

df["river"] = df["identifier"].apply(extract_river)

# ------------------------------------------------------------
# OPTIONAL: reorder columns (keep existing order otherwise)
# ------------------------------------------------------------
cols = list(df.columns)
if "river" in cols and "identifier" in cols:
    cols.remove("river")
    insert_at = cols.index("identifier") + 1
    cols.insert(insert_at, "river")
    df = df[cols]

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 80 complete — river column added to EscapementReport_PlotData.")

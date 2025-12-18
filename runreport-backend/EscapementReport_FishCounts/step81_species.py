# step81_species.py
# ------------------------------------------------------------
# Step 81: Extract species for plotting
#
# Adds Species_Plot to EscapementReport_PlotData where
# Species_Plot = text after the last " - " in identifier.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🐟 Step 81: Extracting Species_Plot from identifier...")

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
# EXTRACT SPECIES_PLOT
# ------------------------------------------------------------
def extract_species(identifier: str) -> str:
    if not isinstance(identifier, str):
        return ""
    parts = identifier.split(" - ")
    return parts[-1].strip() if parts else ""

df["Species_Plot"] = df["identifier"].apply(extract_species)

# ------------------------------------------------------------
# OPTIONAL: reorder columns
# ------------------------------------------------------------
cols = list(df.columns)
if "Species_Plot" in cols and "identifier" in cols:
    cols.remove("Species_Plot")
    insert_at = cols.index("identifier") + 1
    cols.insert(insert_at, "Species_Plot")
    df = df[cols]

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 81 complete — Species_Plot column added to EscapementReport_PlotData.")

# step65_remove_Speelyai.py
# ------------------------------------------------------------
# Step 65: Remove Speelyai Hatchery Chinook/Coho rows
#
# Removes any rows in Escapement_PlotPipeline where facility is
# "Speelyai Hatchery" AND family is "Chinook" or "Coho"
# (case-insensitive).
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🧹 Step 65: Removing Speelyai Hatchery (Chinook/Coho) rows from Escapement_PlotPipeline...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# FILTER OUT SPEELYAI HATCHERY CHINOOK/COHO ROWS
# ------------------------------------------------------------
if "facility" not in df.columns:
    raise ValueError("❌ Missing required column 'facility' in Escapement_PlotPipeline.")

family_col = None
for candidate in ("family", "Family"):
    if candidate in df.columns:
        family_col = candidate
        break

if family_col is None:
    raise ValueError("❌ Missing required column 'family' in Escapement_PlotPipeline.")

facility_norm = df["facility"].astype("string").str.strip().str.casefold()
family_norm = df[family_col].astype("string").str.strip().str.casefold()

mask_speelyai = facility_norm.eq("speelyai hatchery")
mask_family = family_norm.isin({"chinook", "coho"})

mask_remove = mask_speelyai & mask_family
removed = int(mask_remove.sum())

df_filtered = df.loc[~mask_remove].reset_index(drop=True)

print(
    "🗑️ Rows removed (facility='Speelyai Hatchery' AND family in {'Chinook','Coho'}): "
    f"{removed:,}"
)
print(f"📊 Remaining rows: {len(df_filtered):,}")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_filtered.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 65 complete — Speelyai Hatchery Chinook/Coho rows removed.")

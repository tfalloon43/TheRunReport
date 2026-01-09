"""
step29_duplicates_delete.py
------------------------------------------------------------
Collapse duplicate biological count events in Escapement_PlotPipeline.

Rules:
    • Same facility, species, Stock_BO
    • Same full count payload
    • If date_iso within 365 days → keep earliest
    • If > 365 days apart → keep both

IMPORTANT:
    • date_iso is canonical (YYYY-MM-DD string)
    • date_dt is a temporary working column only
"""

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 29: Collapsing duplicate biological count events (DB)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_PATH = BACKEND_ROOT / "0_db" / "local.db"

print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# Load table
# ------------------------------------------------------------
with sqlite3.connect(DB_PATH) as conn:
    df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

initial_count = len(df)
print(f"📥 Loaded {initial_count:,} rows")

# ------------------------------------------------------------
# Canonical date handling (Option A)
# ------------------------------------------------------------

# Ensure date_iso is string-only YYYY-MM-DD
df["date_iso"] = df["date_iso"].astype(str).str.slice(0, 10)

# Temporary working datetime column
df["date_dt"] = pd.to_datetime(df["date_iso"], errors="coerce")

# ------------------------------------------------------------
# Normalize count columns
# ------------------------------------------------------------
COUNT_COLS = [
    "Adult_Total", "Jack_Total", "Total_Eggtake",
    "On_Hand_Adults", "On_Hand_Jacks",
    "Lethal_Spawned", "Live_Spawned", "Released",
    "Live_Shipped", "Mortality", "Surplus"
]

for c in COUNT_COLS:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

KEY_COLS = ["facility", "species", "Stock_BO"] + COUNT_COLS

# ------------------------------------------------------------
# Deterministic ordering
# ------------------------------------------------------------
df = (
    df.sort_values(KEY_COLS + ["date_dt"], kind="mergesort")
      .reset_index(drop=True)
)

# ------------------------------------------------------------
# Collapse duplicate events (365-day rule)
# ------------------------------------------------------------
keep_mask = [False] * len(df)

for _, g in df.groupby(KEY_COLS, dropna=False):
    last_kept_date = None

    for idx in g.index:
        curr_date = df.loc[idx, "date_dt"]

        if pd.isna(curr_date):
            # Defensive: keep rows with invalid dates
            keep_mask[idx] = True
            continue

        if last_kept_date is None:
            keep_mask[idx] = True
            last_kept_date = curr_date
        else:
            if (curr_date - last_kept_date).days > 365:
                keep_mask[idx] = True
                last_kept_date = curr_date

df_final = df[keep_mask].reset_index(drop=True)

removed = initial_count - len(df_final)

print(f"🧹 Removed {removed:,} duplicate event rows")
print(f"📊 Final row count: {len(df_final):,}")

# ------------------------------------------------------------
# Cleanup working columns before persistence
# ------------------------------------------------------------
df_final = df_final.drop(columns=["date_dt"])

# Enforce canonical string date again (belt & suspenders)
df_final["date_iso"] = df_final["date_iso"].astype(str).str.slice(0, 10)

# ------------------------------------------------------------
# Write back to DB
# ------------------------------------------------------------
with sqlite3.connect(DB_PATH) as conn:
    df_final.to_sql(
        "Escapement_PlotPipeline",
        conn,
        if_exists="replace",
        index=False
    )

print("💾 Updated Escapement_PlotPipeline in place")
print("✅ Step 28 complete!")

# step52_Iteration_plot.py
# ------------------------------------------------------------
# Step 52 (Plotting Iteration): Final plotting prep columns
#
# Adds the final derived plotting columns to Escapement_PlotPipeline:
#   day_diff_plot
#   adult_diff_plot
#   Biological_Year
#   Biological_Year_Length
#
# All work is performed directly inside the database.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path


# ------------------------------------------------------------
# Reorder helper
# ------------------------------------------------------------

def reorder_for_output(df):
    sort_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]
    missing = [c for c in sort_cols if c not in df.columns]
    if missing:
        return df
    df = df.copy()
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
    df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
    return df.sort_values(
        by=sort_cols,
        ascending=[True, True, True, True, True, False],
        na_position="last",
        kind="mergesort",
    )


print("🏗️ Step 52: Building final plotting columns (day_diff_plot → Biological_Year_Length)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD DATA FROM DB
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# Normalize column names to underscore schema if needed
rename_map = {
    "Adult Total": "Adult_Total",
    "Jack Total": "Jack_Total",
    "Total Eggtake": "Total_Eggtake",
    "On Hand Adults": "On_Hand_Adults",
    "On Hand Jacks": "On_Hand_Jacks",
    "Lethal Spawned": "Lethal_Spawned",
    "Live Spawned": "Live_Spawned",
    "Live Shipped": "Live_Shipped",
}
df = df.rename(columns=rename_map)

# ------------------------------------------------------------
# REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "Adult_Total",
    "by_adult_f", "by_adult_f_length",
    "day_diff_f", "adult_diff_f"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing columns required for plotting prep: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# SORT CONSISTENTLY
# ------------------------------------------------------------
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ============================================================
# STEP 1: day_diff_plot
# ============================================================
print("🔹 Creating day_diff_plot...")

df["day_diff_plot"] = df["day_diff_f"]

# Identify biological year transitions
df["prev_by"] = df.groupby(group_cols)["by_adult_f"].shift(1)
boundary_mask = df["by_adult_f"] == (df["prev_by"] + 1)

df.loc[boundary_mask, "day_diff_plot"] = 7
boundary_count = int(boundary_mask.sum())

df = df.drop(columns=["prev_by"])

# ============================================================
# STEP 2: adult_diff_plot
# ============================================================
print("🔹 Creating adult_diff_plot...")

df["adult_diff_plot"] = df["adult_diff_f"]
df.loc[df["adult_diff_f"] < 0, "adult_diff_plot"] = df["Adult_Total"]

# ============================================================
# STEP 3: Biological_Year
# ============================================================
df["Biological_Year"] = df["by_adult_f"]

# ============================================================
# STEP 4: Biological_Year_Length
# ============================================================
df["Biological_Year_Length"] = df["by_adult_f_length"]

# ============================================================
# SAVE BACK TO DB
# ============================================================
print("💾 Writing plotting prep columns back to Escapement_PlotPipeline...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration plot complete!")
print("📊 Added columns:")
print("   • day_diff_plot")
print("   • adult_diff_plot")
print("   • Biological_Year")
print("   • Biological_Year_Length")
print(f"🔢 Total rows: {len(df):,}")
print(f"🔁 Biological year transitions (day_diff_plot=7): {boundary_count:,}")

# 8_currentyear/8_weeklydata_current.py
# ------------------------------------------------------------
# Step 8 (Current Year): Convert daily *_current tables into weekly totals
# (with 12-31 partial-week adjustment ×3.5)
# ------------------------------------------------------------

import pandas as pd
import numpy as np
from pathlib import Path

print("📆 Step 8 (Current Year): Converting daily *_current tables to weekly totals (with 12-31 adjustment)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

file_pairs = [
    ("hatchspecies_h_current.csv", "hatchspecies_h_current_weekly.csv"),
    ("hatchspecies_w_current.csv", "hatchspecies_w_current_weekly.csv"),
    ("hatchfamily_h_current.csv",  "hatchfamily_h_current_weekly.csv"),
    ("hatchfamily_w_current.csv",  "hatchfamily_w_current_weekly.csv"),
    ("basinfamily_h_current.csv",  "basinfamily_h_current_weekly.csv"),
    ("basinfamily_w_current.csv",  "basinfamily_w_current_weekly.csv"),
    ("basinspecies_h_current.csv", "basinspecies_h_current_weekly.csv"),
    ("basinspecies_w_current.csv", "basinspecies_w_current_weekly.csv"),
]

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def make_weekly_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily data into 7-day blocks and apply 12-31 ×3.5 adjustment."""

    # Sort by date (fixed non-leap year for alignment)
    df["MM-DD"] = pd.to_datetime("2024-" + df["MM-DD"], errors="coerce")
    df = df.sort_values("MM-DD").reset_index(drop=True)

    numeric_cols = [c for c in df.columns if c != "MM-DD"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    weekly_rows = []
    for start in range(0, len(df), 7):
        chunk = df.iloc[start:start + 7]
        if len(chunk) == 0:
            continue
        summed = chunk[numeric_cols].sum(numeric_only=True).round(2)
        last_day = chunk["MM-DD"].iloc[-1].strftime("%m-%d")
        weekly_rows.append([last_day] + summed.tolist())

    weekly_df = pd.DataFrame(weekly_rows, columns=["MM-DD"] + numeric_cols)

    # Ensure numeric after creation
    for c in numeric_cols:
        weekly_df[c] = pd.to_numeric(weekly_df[c], errors="coerce").fillna(0.0)

    # Apply 12-31 adjustment ×3.5
    if (weekly_df["MM-DD"] == "12-31").any():
        idx = weekly_df.index[weekly_df["MM-DD"] == "12-31"][0]
        adj = (weekly_df.loc[idx:idx, numeric_cols].astype(float) * 3.5).round(2)
        weekly_df.loc[idx:idx, numeric_cols] = adj.values
        print("🔧 Adjusted final week (12-31) — multiplied values by 3.5")

    return weekly_df

# ------------------------------------------------------------
# Process all *_current tables
# ------------------------------------------------------------
for infile, outfile in file_pairs:
    input_path = data_dir / infile
    output_path = data_dir / outfile

    if not input_path.exists():
        print(f"⚠️ Skipping missing file: {infile}")
        continue

    df = pd.read_csv(input_path)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns) - 1:,} data columns from {infile}")

    weekly_df = make_weekly_sum(df)
    weekly_df.to_csv(output_path, index=False)
    print(f"💾 Saved → {outfile} ({len(weekly_df)} weekly rows)")

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print("🎯 Step 8 (Current Year) complete — all 8 *_current_weekly tables generated successfully with 12-31 adjustment.")

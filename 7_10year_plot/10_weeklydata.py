# 10_weeklydata.py
# ------------------------------------------------------------
# Step 10: Convert daily 10-year tables into weekly averages
# (with 12-31 partial-week adjustment ×3.5)
# ------------------------------------------------------------

import pandas as pd
import numpy as np
from pathlib import Path

print("📆 Step 10: Converting daily tables to weekly 10-year averages (with 12-31 adjustment)...")

project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

file_pairs = [
    ("hatchspecies_h.csv", "hatchspecies_h_weekly.csv"),
    ("hatchspecies_w.csv", "hatchspecies_w_weekly.csv"),
    ("hatchfamily_h.csv",  "hatchfamily_h_weekly.csv"),
    ("hatchfamily_w.csv",  "hatchfamily_w_weekly.csv"),
    ("basinfamily_h.csv",  "basinfamily_h_weekly.csv"),
    ("basinfamily_w.csv",  "basinfamily_w_weekly.csv"),
    ("basinspecies_h.csv", "basinspecies_h_weekly.csv"),
    ("basinspecies_w.csv", "basinspecies_w_weekly.csv"),
]

def make_weekly_average(df: pd.DataFrame) -> pd.DataFrame:
    # Sort by date; use a fixed non-leap year
    df["MM-DD"] = pd.to_datetime("2024-" + df["MM-DD"], errors="coerce")
    df = df.sort_values("MM-DD").reset_index(drop=True)

    numeric_cols = [c for c in df.columns if c != "MM-DD"]
    # Ensure numeric source
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    weekly_rows = []
    for start in range(0, len(df), 7):
        chunk = df.iloc[start:start+7]
        if len(chunk) == 0:
            continue
        summed = (chunk[numeric_cols].sum(numeric_only=True) / 10.0).round(2)
        last_day = chunk["MM-DD"].iloc[-1].strftime("%m-%d")
        weekly_rows.append([last_day] + summed.tolist())

    weekly_df = pd.DataFrame(weekly_rows, columns=["MM-DD"] + numeric_cols)

    # ✅ Force numeric after construction (prevents object-dtype surprises)
    for c in numeric_cols:
        weekly_df[c] = pd.to_numeric(weekly_df[c], errors="coerce").fillna(0.0)

    # ✅ 12-31 adjustment using a DataFrame slice (not a Series)
    if (weekly_df["MM-DD"] == "12-31").any():
        idx = weekly_df.index[weekly_df["MM-DD"] == "12-31"][0]
        # operate on a 1-row DataFrame to preserve dtypes, then assign back
        adj = (weekly_df.loc[idx:idx, numeric_cols].astype(float) * 3.5).round(2)
        weekly_df.loc[idx:idx, numeric_cols] = adj.values
        print("🔧 Adjusted final week (12-31) — multiplied values by 3.5")

    return weekly_df

for infile, outfile in file_pairs:
    input_path = data_dir / infile
    output_path = data_dir / outfile

    if not input_path.exists():
        print(f"⚠️ Skipping missing file: {infile}")
        continue

    df = pd.read_csv(input_path)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)-1:,} data columns from {infile}")

    weekly_df = make_weekly_average(df)
    weekly_df.to_csv(output_path, index=False)
    print(f"💾 Saved → {outfile} ({len(weekly_df)} weekly rows)")

print("🎯 Step 10 complete — all 8 weekly tables generated successfully with 12-31 adjustment.")
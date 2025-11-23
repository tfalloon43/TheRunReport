# 8_weeklydata_previous.py
# ------------------------------------------------------------
# Step 8 (Previous Year): Convert daily *_previous tables into
# weekly totals (with 12-31 partial-week adjustment ×3.5)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("📆 Step 8 (Previous Year): Converting daily *_previous tables to weekly totals (with 12-31 adjustment)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

stocks = ["h", "w", "u"]

file_pairs = [
    (f"hatchspecies_{s}_previous.csv", f"hatchspecies_{s}_previous_weekly.csv") for s in stocks
] + [
    (f"hatchfamily_{s}_previous.csv", f"hatchfamily_{s}_previous_weekly.csv") for s in stocks
] + [
    (f"basinfamily_{s}_previous.csv", f"basinfamily_{s}_previous_weekly.csv") for s in stocks
] + [
    (f"basinspecies_{s}_previous.csv", f"basinspecies_{s}_previous_weekly.csv") for s in stocks
]

# ------------------------------------------------------------
# Helper function — sums *one* year's worth of days
# ------------------------------------------------------------
def make_weekly_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily data into 7-day blocks and apply 12-31 ×3.5 adjustment."""

    # Convert MM-DD into a sortable datetime (using fixed year 2024)
    df["MM-DD"] = pd.to_datetime("2024-" + df["MM-DD"], errors="coerce")
    df = df.sort_values("MM-DD").reset_index(drop=True)

    # Identify numeric columns
    numeric_cols = [c for c in df.columns if c != "MM-DD"]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Build weekly rows
    weekly_rows = []
    for start in range(0, len(df), 7):
        chunk = df.iloc[start : start + 7]
        if len(chunk) == 0:
            continue

        summed = chunk[numeric_cols].sum(numeric_only=True, min_count=1).round(2)
        last_day = chunk["MM-DD"].iloc[-1].strftime("%m-%d")

        weekly_rows.append([last_day] + summed.tolist())

    weekly_df = pd.DataFrame(weekly_rows, columns=["MM-DD"] + numeric_cols)

    # Fix dtypes (numeric)
    for c in numeric_cols:
        weekly_df[c] = pd.to_numeric(weekly_df[c], errors="coerce")

    # 12-31 ×3.5 correction
    if (weekly_df["MM-DD"] == "12-31").any():
        idx = weekly_df.index[weekly_df["MM-DD"] == "12-31"][0]
        adj = (weekly_df.loc[idx:idx, numeric_cols].astype(float) * 3.5).round(2)
        weekly_df.loc[idx:idx, numeric_cols] = adj.values
        print("🔧 Adjusted final week (12-31) — multiplied values by 3.5")

    return weekly_df

# ------------------------------------------------------------
# Process all *_previous tables
# ------------------------------------------------------------
for infile, outfile in file_pairs:
    input_path = data_dir / infile
    output_path = data_dir / outfile

    if not input_path.exists():
        print(f"⚠️ Skipping missing file: {infile}")
        continue

    df = pd.read_csv(input_path)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)-1:,} data columns from {infile}")

    weekly_df = make_weekly_sum(df)
    weekly_df.to_csv(output_path, index=False)

    print(f"💾 Saved → {outfile} ({len(weekly_df)} weekly rows)")

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print("🎯 Step 8 (Previous Year) complete — all *_previous_weekly tables generated successfully with 12-31 adjustment.")
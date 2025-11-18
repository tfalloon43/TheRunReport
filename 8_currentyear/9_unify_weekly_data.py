# 9_unify_weekly_data.py
# ------------------------------------------------------------
# Step 9: Combine all weekly (10-year) and weekly_current (2025)
#         tables into a single unified long-format CSV.
#
# Input files:
#   hatchspecies_[h/w/u]_weekly.csv
#   hatchfamily_[h/w/u]_weekly.csv
#   basinfamily_[h/w/u]_weekly.csv
#   basinspecies_[h/w/u]_weekly.csv
#
#   hatchspecies_[h/w/u]_current_weekly.csv
#   hatchfamily_[h/w/u]_current_weekly.csv
#   basinfamily_[h/w/u]_current_weekly.csv
#   basinspecies_[h/w/u]_current_weekly.csv
#
# Output:
#   weekly_unified_long.csv
#
# Format:
#   MM-DD | identifier | category_type | stock | metric_type | value
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔗 Step 9: Unifying weekly 10-year + current-year CSVs into long format...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

# ------------------------------------------------------------
# 🧩 File lists (aligned with your REAL filenames)
# ------------------------------------------------------------

stocks = ["h", "w", "u"]

ten_year_files = [
    f"hatchspecies_{s}_weekly.csv" for s in stocks
] + [
    f"hatchfamily_{s}_weekly.csv" for s in stocks
] + [
    f"basinfamily_{s}_weekly.csv" for s in stocks
] + [
    f"basinspecies_{s}_weekly.csv" for s in stocks
]

current_year_files = [
    f"hatchspecies_{s}_current_weekly.csv" for s in stocks
] + [
    f"hatchfamily_{s}_current_weekly.csv" for s in stocks
] + [
    f"basinfamily_{s}_current_weekly.csv" for s in stocks
] + [
    f"basinspecies_{s}_current_weekly.csv" for s in stocks
]

all_files = ten_year_files + current_year_files

# ------------------------------------------------------------
# Helper: extract metadata from filename
# ------------------------------------------------------------
def parse_filename(name: str):
    base = name.replace(".csv", "")

    # Determine metric_type
    metric_type = "current_year" if "current" in base else "ten_year_avg"

    # Identify category
    if "hatchspecies" in base:
        category = "hatchspecies"
    elif "hatchfamily" in base:
        category = "hatchfamily"
    elif "basinfamily" in base:
        category = "basinfamily"
    elif "basinspecies" in base:
        category = "basinspecies"
    else:
        category = "unknown"

    # Stock (H/W/U)
    parts = base.split("_")
    stock_token = next((p for p in parts if p in {"h", "w", "u"}), None)
    if stock_token is None:
        print(f"⚠️ Unable to determine stock from filename '{name}' — defaulting to '?'")
        stock = "?"
    else:
        stock = stock_token.upper()

    return category, stock, metric_type

# ------------------------------------------------------------
# Main unify loop
# ------------------------------------------------------------
records = []

for filename in all_files:
    path = data_dir / filename

    if not path.exists():
        print(f"⚠️ Missing file: {filename} — skipping")
        continue

    print(f"📘 Loading {filename} ...")
    df = pd.read_csv(path)

    category, stock, metric_type = parse_filename(filename)

    # Convert from wide → long
    long_df = df.melt(
        id_vars=["MM-DD"],
        var_name="identifier",
        value_name="value"
    )

    long_df["category_type"] = category
    long_df["stock"] = stock
    long_df["metric_type"] = metric_type

    records.append(long_df)

# ------------------------------------------------------------
# Combine all into one unified table
# ------------------------------------------------------------
if len(records) == 0:
    raise RuntimeError("❌ No CSVs were successfully loaded — nothing to unify.")

unified = pd.concat(records, ignore_index=True)

# Cleanup
unified["value"] = pd.to_numeric(unified["value"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Save Output
# ------------------------------------------------------------
output_path = data_dir / "weekly_unified_long.csv"
unified.to_csv(output_path, index=False)

print("\n🎉 Unified weekly dataset created!")
print(f"📊 Total rows: {len(unified):,}")
print(f"📁 Saved → {output_path}")
print("🎯 Format: MM-DD | identifier | category_type | stock | metric_type | value")

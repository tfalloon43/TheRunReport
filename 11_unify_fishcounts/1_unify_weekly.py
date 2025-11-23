# 1_unify_weekly.py
# ------------------------------------------------------------
# Combine ALL weekly tables:
#   • 10-year averages   → *_weekly.csv
#   • current-year (2025)→ *_current_weekly.csv
#   • previous-year (2024)-> *_previous_weekly.csv
#
# Into a single long-format CSV:
#     csv_unify_fishcounts.csv
#
# Format:
#   MM-DD | identifier | category_type | stock | metric_type | value
# ------------------------------------------------------------

import numpy as np
import pandas as pd
from datetime import timedelta
from pathlib import Path

print("🔗 Step 1: Unifying ALL weekly datasets (10-year + current + previous)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

# ------------------------------------------------------------
# File groups
# ------------------------------------------------------------
stocks = ["h", "w", "u"]
categories = ["hatchspecies", "hatchfamily", "basinfamily", "basinspecies"]

# 10-year files
ten_year_files = [
    f"{cat}_{s}_weekly.csv" for cat in categories for s in stocks
]

# current-year files
current_year_files = [
    f"{cat}_{s}_current_weekly.csv" for cat in categories for s in stocks
]

# previous-year files
previous_year_files = [
    f"{cat}_{s}_previous_weekly.csv" for cat in categories for s in stocks
]

all_files = ten_year_files + current_year_files + previous_year_files

# ------------------------------------------------------------
# Helper: extract metadata from filename
# ------------------------------------------------------------
def parse_filename(name: str):
    base = name.replace(".csv", "")

    # Determine metric type
    if "_current_" in base:
        metric_type = "current_year"
    elif "_previous_" in base:
        metric_type = "previous_year"
    else:
        metric_type = "ten_year_avg"

    # Identify category
    category = None
    for c in categories:
        if c in base:
            category = c
            break
    if category is None:
        category = "unknown"

    # Extract stock token
    parts = base.split("_")
    stock_token = next((p for p in parts if p in {"h", "w", "u"}), None)
    stock = stock_token.upper() if stock_token else "?"

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

    # Wide → long
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
    raise RuntimeError("❌ No weekly CSVs were found — nothing to unify.")

unified = pd.concat(records, ignore_index=True)

# Numeric clean-up
unified["value"] = pd.to_numeric(unified["value"], errors="coerce")

# Build sortable date
unified["date_obj"] = pd.to_datetime(
    "2024-" + unified["MM-DD"],
    format="%Y-%m-%d",
    errors="coerce"
)

# ------------------------------------------------------------
# Save Output
# ------------------------------------------------------------
output_path = data_dir / "csv_unify_fishcounts.csv"
unified.to_csv(output_path, index=False)

print("\n🎉 Unified fishcounts dataset created!")
print(f"📊 Total rows: {len(unified):,}")
print(f"📁 Saved → {output_path}")
print("🎯 Format: MM-DD | identifier | category_type | stock | metric_type | value")

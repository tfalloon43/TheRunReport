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

import numpy as np
import pandas as pd
from datetime import timedelta
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

# Cleanup & baseline handling
unified["value"] = pd.to_numeric(unified["value"], errors="coerce")
unified["date_obj"] = pd.to_datetime(
    "2024-" + unified["MM-DD"], format="%Y-%m-%d", errors="coerce"
)

group_cols = ["category_type", "identifier", "stock", "metric_type"]


def backfill_pre_counts(group: pd.DataFrame) -> pd.DataFrame:
    """Set leading blanks to zero for current-year lines only."""
    if group.name[3] != "current_year":
        return group

    group = group.copy().sort_values("date_obj")
    valid_mask = group["value"].notna()
    if not valid_mask.any():
        return group

    first_date = group.loc[valid_mask, "date_obj"].iloc[0]
    leading_mask = group["date_obj"] < first_date
    group.loc[leading_mask, "value"] = 0.0
    return group


def trim_trailing_placeholders(group: pd.DataFrame) -> pd.DataFrame:
    """Drop placeholder zeros after the last real data point."""
    if group.name[3] != "current_year":
        return group

    group = group.copy().sort_values("date_obj")
    positive_mask = group["value"] > 0
    if not positive_mask.any():
        return group

    last_date = group.loc[positive_mask, "date_obj"].iloc[-1]
    trailing_mask = group["date_obj"] > last_date
    group.loc[trailing_mask, "value"] = np.nan
    return group


def align_hw_end_dates(group: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure Hatchery & Wild current-year lines end together.

    If one stock continues reporting later than the other, allow a 15-day lag
    before filling missing values with zero. This keeps the two lines within
    15 days of each other without forcing an immediate zero drop.
    """

    if group.name[2] != "current_year":
        return group

    group = group.copy()
    hw_mask = group["stock"].isin({"H", "W"})
    valid_hw = group[hw_mask & group["value"].notna()]
    if valid_hw.empty:
        return group

    last_by_stock = valid_hw.groupby("stock")["date_obj"].max()
    latest_hw_date = last_by_stock.max()
    lag_allowance = timedelta(days=15)
    latest_allowed_gap_date = latest_hw_date - lag_allowance

    for stock, last_date in last_by_stock.items():
        # If this stock already reports within the allowed 15-day window, skip.
        if last_date >= latest_allowed_gap_date:
            continue

        # Fill only up to the allowed gap date to avoid overshooting by more than 15 days.
        fill_mask = (
            (group["stock"] == stock)
            & group["date_obj"].gt(last_date)
            & group["date_obj"].le(latest_allowed_gap_date)
            & group["value"].isna()
        )
        group.loc[fill_mask, "value"] = 0.0
    return group


unified = (
    unified.groupby(group_cols, group_keys=False)
    .apply(backfill_pre_counts)
    .groupby(group_cols, group_keys=False)
    .apply(trim_trailing_placeholders)
    .groupby(["category_type", "identifier", "metric_type"], group_keys=False)
    .apply(align_hw_end_dates)
    .sort_values(group_cols + ["date_obj"])
)
unified = unified.drop(columns="date_obj")

# ------------------------------------------------------------
# Save Output
# ------------------------------------------------------------
output_path = data_dir / "weekly_unified_long.csv"
unified.to_csv(output_path, index=False)

print("\n🎉 Unified weekly dataset created!")
print(f"📊 Total rows: {len(unified):,}")
print(f"📁 Saved → {output_path}")
print("🎯 Format: MM-DD | identifier | category_type | stock | metric_type | value")

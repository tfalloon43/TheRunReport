# 9_tablefill.py
# ------------------------------------------------------------
# Step 9: Populate H/W daily tables with fish-per-day values
#
# Logic:
#   • Reads each row from csv_10av.csv
#   • Determines Stock (H or W)
#   • Gets fishperday value and day columns (Day1–DayN)
#   • Adds fishperday to the corresponding date cells in:
#         - hatchspecies_[H/W].csv
#         - hatchfamily_[H/W].csv
#         - basinfamily_[H/W].csv
#         - basinspecies_[H/W].csv
#
#   If a cell already has a value, new values are added (summed).
#
# Input  : 100_Data/csv_10av.csv + 8 empty tables from 8_tablegen.py
# Output : Updated 8 tables with filled-in values
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 9: Populating stock-based tables with daily fishperday values...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_10av.csv"

table_paths = {
    "hatchspecies": {"H": data_dir / "hatchspecies_h.csv", "W": data_dir / "hatchspecies_w.csv"},
    "hatchfamily":  {"H": data_dir / "hatchfamily_h.csv",  "W": data_dir / "hatchfamily_w.csv"},
    "basinfamily":  {"H": data_dir / "basinfamily_h.csv",  "W": data_dir / "basinfamily_w.csv"},
    "basinspecies": {"H": data_dir / "basinspecies_h.csv", "W": data_dir / "basinspecies_w.csv"},
}

# ------------------------------------------------------------
# Load input data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
df.columns = [c.strip() for c in df.columns]

print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# Ensure numeric and clean columns
df["fishperday"] = pd.to_numeric(df["fishperday"], errors="coerce").fillna(0)
df["Stock"] = df["Stock"].astype(str).str.strip().str.upper()

# Identify all "Day" columns dynamically
day_cols = [c for c in df.columns if c.lower().startswith("day")]
print(f"📅 Found {len(day_cols)} Day columns (e.g., {day_cols[:5]}...)")

# ------------------------------------------------------------
# Load all 8 output tables
# ------------------------------------------------------------
tables = {}
for key, stock_map in table_paths.items():
    tables[key] = {}
    for stock, path in stock_map.items():
        if not path.exists():
            raise FileNotFoundError(f"❌ Missing table file: {path}")
        tables[key][stock] = pd.read_csv(path)
        tables[key][stock]["MM-DD"] = tables[key][stock]["MM-DD"].astype(str)

print("✅ Loaded all 8 base tables from Step 8.")

# ------------------------------------------------------------
# Core logic — loop over rows
# ------------------------------------------------------------
rows_processed = 0
cells_updated = 0

for _, row in df.iterrows():
    stock = str(row["Stock"]).upper().strip()
    if stock not in ["H", "W"]:
        continue

    fish_value = row["fishperday"]
    if fish_value == 0:
        continue

    # Identify which tables to update
    try:
        h_species = str(row["hatchspecies"]).strip()
        h_family = str(row["hatchfamily"]).strip()
        b_family = str(row["basinfamily"]).strip()
        b_species = str(row["basinspecies"]).strip()
    except KeyError:
        continue

    # Filter valid Day cells (ignore blanks or NaNs)
    days = [str(row[c]).strip() for c in day_cols if isinstance(row[c], str) and row[c].strip() != ""]
    if not days:
        continue

    # Loop through each day and update relevant cells
    for d in days:
        for table_name, key_value in {
            "hatchspecies": h_species,
            "hatchfamily":  h_family,
            "basinfamily":  b_family,
            "basinspecies": b_species,
        }.items():
            table = tables[table_name][stock]

            # Find matching row for the date
            match = table["MM-DD"] == d
            if not match.any():
                continue

            if key_value not in table.columns:
                continue  # Skip if column missing

            # Add fish value
            table.loc[match, key_value] += fish_value
            cells_updated += 1

    rows_processed += 1

# ------------------------------------------------------------
# Save updated tables
# ------------------------------------------------------------
for table_name, stock_map in tables.items():
    for stock, tdf in stock_map.items():
        output_path = table_paths[table_name][stock]
        tdf.to_csv(output_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("✅ Step 9 complete — Tables updated successfully.")
print(f"📊 Rows processed: {rows_processed:,}")
print(f"➕ Cells updated: {cells_updated:,}")
print("💾 Updated files:")
for tname, stock_map in table_paths.items():
    for stock, path in stock_map.items():
        print(f"   • {path.name}")

print("🎯 All updates written to 100_Data/")
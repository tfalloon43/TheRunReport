# 8_currentyear/7_tablefill_current.py
# ------------------------------------------------------------
# Step 7 (Current Year): Populate H/W daily tables with fish-per-day values
#
# Logic:
#   • Reads each row from csv_currentyear.csv
#   • Determines Stock (H or W)
#   • Gets fishperday value and day columns (Day1–DayN)
#   • Adds fishperday to the corresponding date cells in:
#         - hatchspecies_[H/W]_current.csv
#         - hatchfamily_[H/W]_current.csv
#         - basinfamily_[H/W]_current.csv
#         - basinspecies_[H/W]_current.csv
#
#   If a cell already has a value, new values are added (summed).
#
# Input  : 100_Data/csv_currentyear.csv + 8 empty *_current tables from 6_tablegen_current.py
# Output : Updated *_current tables with filled-in values
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 7 (Current Year): Populating stock-based *_current tables with daily fishperday values...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_currentyear.csv"

table_paths = {
    "hatchspecies": {"H": data_dir / "hatchspecies_h_current.csv", "W": data_dir / "hatchspecies_w_current.csv"},
    "hatchfamily":  {"H": data_dir / "hatchfamily_h_current.csv",  "W": data_dir / "hatchfamily_w_current.csv"},
    "basinfamily":  {"H": data_dir / "basinfamily_h_current.csv",  "W": data_dir / "basinfamily_w_current.csv"},
    "basinspecies": {"H": data_dir / "basinspecies_h_current.csv", "W": data_dir / "basinspecies_w_current.csv"},
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

print("✅ Loaded all 8 *_current base tables from Step 6.")

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
        h_family  = str(row["hatchfamily"]).strip()
        b_family  = str(row["basinfamily"]).strip()
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
            try:
                table.loc[match, key_value] = (
                    pd.to_numeric(table.loc[match, key_value], errors="coerce").fillna(0) + fish_value
                )
                cells_updated += 1
            except Exception as e:
                print(f"⚠️ Error updating {table_name}_{stock} on {d}: {e}")

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
print("✅ Step 7 (Current Year) complete — *_current tables updated successfully.")
print(f"📊 Rows processed: {rows_processed:,}")
print(f"➕ Cells updated: {cells_updated:,}")
print("💾 Updated files:")
for tname, stock_map in table_paths.items():
    for stock, path in stock_map.items():
        print(f"   • {path.name}")

print("🎯 All *_current tables written to 100_Data/")

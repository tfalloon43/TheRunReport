# 27_cleanup7.py
# ------------------------------------------------------------
# Step 27 (v7): Manual cleanup for specific line deletions
#
# Automatically deletes rows when *exactly one* match is found.
# If multiple matches are found for a rule, you'll be prompted.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/27_cleanup7_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 27 (v7): Manual cleanup — delete specific rows by field values...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "27_cleanup7_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Normalize dates
# ------------------------------------------------------------
if "date_iso" in df.columns:
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

# ------------------------------------------------------------
# ✏️ MANUAL DELETION LIST
# ------------------------------------------------------------
# Add as many dictionary blocks as needed
manual_deletions = [
     {
         "pdf_name": "WA_EscapementReport_04-15-2021.pdf",
         "facility": "Kalama Falls Hatchery",
         "species": "Summer Steelhead",
         "Stock": "H",
         "date_iso": "2021-03-19",
         "Adult_Total": 1.0,
         #ROW 16417
     },
     {
         "pdf_name": "WA_EscapementReport_01-30-2025.pdf",
         "facility": "Cowlitz Salmon Hatchery",
         "species": "Type N Coho",
         "Stock": "H",
         "date_iso": "2025-01-21",
         "Adult_Total": 20005,
         #ROW 5587
     },
     {
         "pdf_name": "WA_EscapementReport_01-30-2025.pdf",
         "facility": "Cowlitz Salmon Hatchery",
         "species": "Type N Coho",
         "Stock": "W",
         "date_iso": "2025-01-21",
         "Adult_Total": 13204,
         #ROW 6182
     },
     {
         "pdf_name": "WA_EscapementReport_03-06-2025.pdf",
         "facility": "Cowlitz Salmon Hatchery",
         "species": "Type N Coho",
         "Stock": "W",
         "date_iso": "2025-02-27",
         "Adult_Total": 13206,
         #ROW 6182
     },
]

# ------------------------------------------------------------
# Build deletion mask
# ------------------------------------------------------------
to_delete = set()

for rule in manual_deletions:
    cond = pd.Series(True, index=df.index)
    for key, val in rule.items():
        if key not in df.columns:
            print(f"⚠️ Warning: column '{key}' not found in DataFrame; skipping this key.")
            continue
        if key == "date_iso":
            val = pd.to_datetime(val, errors="coerce")
        cond &= df[key] == val

    matched = df[cond]
    count = len(matched)

    if count == 0:
        print(f"⚠️ No match found for → {rule}")
    elif count == 1:
        idx = matched.index[0]
        print(f"🗑️ Auto-deleting 1 row → {rule}")
        print(matched[["pdf_name", "facility", "species", "Stock", "date_iso", "Adult_Total"]].to_string(index=True))
        to_delete.add(idx)
    else:
        print(f"⚠️ {count} rows matched → {rule}")
        print(matched[["pdf_name", "facility", "species", "Stock", "date_iso", "Adult_Total"]].to_string(index=True))
        confirm = input(f"\nType 'yes' to delete these {count} rows: ").strip().lower()
        if confirm == "yes":
            to_delete.update(matched.index.tolist())
        else:
            print("⏭️ Skipped deletion for this rule.")

# ------------------------------------------------------------
# Apply deletions
# ------------------------------------------------------------
if not to_delete:
    print("ℹ️ No matching rows to delete. Exiting.")
else:
    before = len(df)
    df = df.drop(index=to_delete).reset_index(drop=True)
    after = len(df)
    removed = before - after

    df.to_csv(output_path, index=False)
    df.to_csv(recent_path, index=False)

    print(f"\n✅ Manual cleanup complete → {output_path}")
    print(f"🧽 Removed {removed:,} specific rows. Remaining: {after:,}")
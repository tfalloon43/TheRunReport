# 22_delete_csv.py
# ------------------------------------------------------------
# Step 22: Delete all CSV files in 100_Data except csv_recent.csv.
# Safe utility for cleaning up intermediate outputs.
# ------------------------------------------------------------

import os
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[0]  # current folder
data_dir = Path(__file__).resolve().parents[1] / "100_Data"

if not data_dir.exists():
    raise FileNotFoundError(f"❌ 100_Data folder not found at: {data_dir}")

print(f"🧹 Cleaning up CSV files in: {data_dir}\n")

# ------------------------------------------------------------
# Collect CSV files
# ------------------------------------------------------------
csv_files = list(data_dir.glob("*.csv"))
if not csv_files:
    print("📂 No CSV files found — nothing to delete.")
    exit(0)

kept_file = "csv_recent.csv"
deleted_files = []

# ------------------------------------------------------------
# Delete unwanted CSVs
# ------------------------------------------------------------
for csv_path in csv_files:
    if csv_path.name.lower() == kept_file.lower():
        print(f"🛡️  Keeping: {csv_path.name}")
        continue

    try:
        os.remove(csv_path)
        deleted_files.append(csv_path.name)
    except Exception as e:
        print(f"⚠️  Could not delete {csv_path.name}: {e}")

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
if deleted_files:
    print("\n🗑️  Deleted files:")
    for f in deleted_files:
        print(f"   • {f}")
else:
    print("✅ No files deleted (only csv_recent.csv found).")

print("\n🎯 Cleanup complete.")

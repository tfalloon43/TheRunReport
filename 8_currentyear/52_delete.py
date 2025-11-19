# 52_delete.py
# ------------------------------------------------------------
# Deletes all CSV files in 100_Data EXCEPT:
#      • weekly_unified_long.csv
#
# Purpose:
#   Clean out all intermediate build CSVs after unifying
#   weekly data into one master file.
#
# This keeps the workspace clean while preserving the
# final exported dataset for use in the app/database.
# ------------------------------------------------------------

import os
from pathlib import Path

print("🧹 Step 52: Cleaning up CSV files in 100_Data...\n")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

if not data_dir.exists():
    raise FileNotFoundError(f"❌ 100_Data folder not found at: {data_dir}")

# ------------------------------------------------------------
# Gather all CSV files
# ------------------------------------------------------------
csv_files = list(data_dir.glob("*.csv"))

if not csv_files:
    print("📂 No CSV files found — nothing to delete.")
    exit(0)

# The ONE file we keep
kept_file = "weekly_unified_long.csv"

deleted_files = []

# ------------------------------------------------------------
# Delete all except the kept file
# ------------------------------------------------------------
for csv_path in csv_files:
    if csv_path.name.lower() == kept_file.lower():
        print(f"🛡️ Keeping: {csv_path.name}")
        continue

    try:
        os.remove(csv_path)
        deleted_files.append(csv_path.name)
    except Exception as e:
        print(f"⚠️ Could not delete {csv_path.name}: {e}")

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("\n🗑️ Deleted files:")
if deleted_files:
    for f in deleted_files:
        print(f"   • {f}")
else:
    print("   (None deleted — only kept file was present.)")

print("\n🎯 Cleanup complete — only weekly_unified_long.csv remains.")
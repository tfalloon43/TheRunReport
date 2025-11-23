# 2_delete_csvs.py
# ------------------------------------------------------------
# Delete ALL CSVs in 100_Data EXCEPT:
#   • csv_unify_fishcounts.csv
#   • columbiadaily_raw.csv
#
# Safe, explicit cleanup step.
# ------------------------------------------------------------

import os
from pathlib import Path

print("🧹 Step 2: Cleaning up old CSVs in 100_Data...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

if not data_dir.exists():
    raise FileNotFoundError(f"❌ 100_Data folder not found: {data_dir}")

print(f"📂 Cleaning folder: {data_dir}")

# ------------------------------------------------------------
# Files to KEEP
# ------------------------------------------------------------
KEEP = {
    "csv_unify_fishcounts.csv",
    "columbiadaily_raw.csv",
}

# ------------------------------------------------------------
# Scan and delete
# ------------------------------------------------------------
deleted = []
kept = []

for csv_path in data_dir.glob("*.csv"):
    filename = csv_path.name

    if filename in KEEP:
        kept.append(filename)
        print(f"🛡️ Keeping: {filename}")
        continue

    try:
        os.remove(csv_path)
        deleted.append(filename)
    except Exception as e:
        print(f"⚠️ Could not delete {filename}: {e}")

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("\n🧾 Summary:")
if deleted:
    print("🗑️ Deleted files:")
    for f in deleted:
        print(f"   • {f}")
else:
    print("   (No files deleted)")

print("\n📦 Files kept:")
for f in kept:
    print(f"   • {f}")

print("\n🎯 Cleanup complete.")
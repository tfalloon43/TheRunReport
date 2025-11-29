# 21_delete.py
# ------------------------------------------------------------
# Deletes all CSV files in 100_Data EXCEPT:
#   • csv_unify_fishcounts.csv
#   • columbiadaily_raw.csv
#
# Output:
#   Deletes files in place
#   Prints summary to console
# ------------------------------------------------------------

import os
from pathlib import Path

print("🗑️ Step 21: Cleaning up CSV files in 100_Data (keeping only unified + Columbia raw)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

if not data_dir.exists():
    raise FileNotFoundError(f"❌ 100_Data directory not found at: {data_dir}")

print(f"📂 Scanning folder: {data_dir}")

# ------------------------------------------------------------
# CSVs to KEEP
# ------------------------------------------------------------
KEEP = {
    "csv_unify_fishcounts.csv",
    "columbiadaily_raw.csv"
}

# ------------------------------------------------------------
# Scan folder
# ------------------------------------------------------------
csv_files = list(data_dir.glob("*.csv"))

if not csv_files:
    print("📭 No CSV files found — nothing to delete.")
    exit(0)

deleted = []
kept = []

# ------------------------------------------------------------
# Delete all except the KEEP list
# ------------------------------------------------------------
for csv_path in csv_files:
    if csv_path.name in KEEP:
        print(f"🛡️ Keeping: {csv_path.name}")
        kept.append(csv_path.name)
        continue

    try:
        os.remove(csv_path)
        print(f"🗑️ Deleted: {csv_path.name}")
        deleted.append(csv_path.name)
    except Exception as e:
        print(f"⚠️ Could not delete {csv_path.name}: {e}")

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("\n📋 SUMMARY")
print(f"   • Kept {len(kept)} files:")
for f in kept:
    print(f"     - {f}")

print(f"\n   • Deleted {len(deleted)} files:")
for f in deleted:
    print(f"     - {f}")

print("\n🎯 Cleanup complete.")
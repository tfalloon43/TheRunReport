# step73_remove_basinfamily.py
# ------------------------------------------------------------
# Step 73: Remove specific basin/family combos from
#           Escapement_PlotPipeline.
#
# Populate `pairs_to_remove` with tuples:
#   (basin_name, family_name)
# Each matching row is removed from the table.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 73: Removing specified basin/family combinations...")

# ------------------------------------------------------------
# CONFIGURE REMOVALS HERE
# Example:
# pairs_to_remove = [
#     ("Bogus River", "Chinook"),
#     ("Another River", "Coho"),
# ]
pairs_to_remove = [
     ("Baker River", "Chinook"),
     ("Cedar River", "Chinook"),
     ("Chambers Creek", "Cutthroat"),
     ("Chambers Creek", "Chum"),
     ("Chehalis River", "Pink"),
     ("Chehalis River", "Sockeye"),
     ("Chelan River", "Cutthroat"),
     ("Chiwawa River", "Cutthroat"),
     ("Chiwawa River", "Steelhead"),
     ("Cowlitz River", "Chum"),
     ("Cowlitz River", "Pikeminnow"),
     ("Cowlitz River", "Pink"),
     ("Cowlitz River", "Sockeye"),
     ("Cowlitz River", "Sucker"),
     ("Cowlitz River", "Whitefish"),
     ("Dungeness River", "Pink"),
     ("Dungeness River", "Steelhead"),
     ("Elochoman River", "Pink"),
     ("Elwha River", "Chum"),
     ("Elwha River", "Coho"),
     ("Elwha River", "Pink"),
     ("Elwha River", "Sockeye"),
     ("Elwha River", "Steelhead"),
     ("Grays River", "Pink"),
     ("Grays River", "Steelhead"),
     ("Green River", "Cutthroat"),
     ("Green River", "Chum"),
     ("Issaquah Creek", "Sockeye"),
     ("Issaquah Creek", "Cutthroat"),
     ("Kalama River", "Chum"),
     ("Kalama River", "Pink"),
     ("Kalama River", "Sockeye"),
     ("Lewis River", "Chum"),
     ("Lewis River", "Pink"),
     ("Methow River", "Chinook"),
     ("Methow River", "Steelhead"),
     ("Minter Creek", "Cutthroat"),
     ("Minter Creek", "Sockeye"),
     ("Minter Creek", "Steelhead"),
     ("Naselle River", "Pink"),
     ("Naselle River", "Sockeye"),
     ("North Fork Nooksack River", "Cutthroat"),
     ("North Fork Nooksack River", "Sockeye"),
     ("North Fork Nooksack River", "Whitefish"),
     ("North Nemah River", "Steelhead"),
     ("North Nemah River", "Cutthroat"),
     ("Satsop River", "Sockeye"),
     ("Skagit River", "Pink"),
     ("Skagit River", "Steelhead"),
     ("Skagit River", "Cutthroat"),
     ("Skokomish River", "Sockeye"),
     ("Skykomish River", "Bull Trout"),
     ("Skykomish River", "Chum"),
     ("Skykomish River", "Whitefish"),
     ("Snohomish River", "Bull Trout"),
     ("Snohomish River", "Chum"),
     ("Snohomish River", "Whitefish"),
     ("Sol Duc River", "Steelhead"),
     ("Spokane River", "Cutthroat"),
     ("Stillaguamish River", "Sockeye"),
     ("Stillaguamish River", "Chum"),
     ("Stillaguamish River", "Pink"),
     ("Sammamish River", "Cutthroat"),
     ("Sammamish River", "Kokanee"),
     ("Sammamish River", "Sockeye"),
     ("Tucannon River", "Whitefish"),
     ("Wallace River", "Sockeye"),
     ("Washougal River", "Cutthroat"),
     ("Whatcom Creek", "Coho"),
     ("Whatcom Creek", "Whitefish"),
     ("Whatcom Creek", "Pink"),
     ("Whatcom Creek", "Steelhead"),
     ("Whatcom Creek", "Cutthroat"),
     ("Willapa River", "Cutthroat"),
     ("Wishkah River", "Chinook"),
     ("Nooksack River", "Sockeye"),
     ("Wynoochee River", "Chinook"),

#     ("Another River", "Cutthroat"),
 ]

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
required = ["basinfamily"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

df["basinfamily"] = df["basinfamily"].astype(str).str.strip()

if not pairs_to_remove:
    print("ℹ️ No pairs specified in pairs_to_remove; nothing to remove.")
else:
    removed_total = 0
    for basin_name, family_name in pairs_to_remove:
        basinfamily = f"{str(basin_name).strip()} - {str(family_name).strip()}"
        mask = df["basinfamily"] == basinfamily
        count = int(mask.sum())
        if count:
            df = df.loc[~mask].reset_index(drop=True)
            removed_total += count
            print(f"🗑️ Removed {count} rows for ({basinfamily})")
        else:
            print(f"ℹ️ No rows matched ({basinfamily})")

    print(f"🧾 Total rows removed: {removed_total:,}")
    print(f"📊 Remaining rows: {len(df):,}")

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 73 complete — specified basin/family combinations removed.")

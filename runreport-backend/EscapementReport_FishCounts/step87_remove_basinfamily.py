# step87_remove_basinfamily.py
# ------------------------------------------------------------
# Step 87: Remove specific river/species combos from
#           EscapementReport_PlotData.
#
# Populate `pairs_to_remove` with tuples:
#   (river_name, species_plot)
# Each matching row is removed from the table.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 87: Removing specified river/species combinations...")

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
     ("Tucannon River", "Whitefish"),
     ("Wallace River", "Sockeye"),
     ("Washougal River", "Cutthroat"),
     ("Whatcom Creek", "Whitefish"),
     ("Whatcom Creek", "Pink"),
     ("Whatcom Creek", "Steelhead"),
     ("Whatcom Creek", "Cutthroat"),
     ("Willapa River", "Cutthroat"),
     ("Wishkah River", "Chinook"),
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
df = pd.read_sql_query("SELECT * FROM EscapementReport_PlotData;", conn)

print(f"✅ Loaded {len(df):,} rows from EscapementReport_PlotData")

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
required = ["river", "Species_Plot"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

df["river"] = df["river"].astype(str).str.strip()
df["Species_Plot"] = df["Species_Plot"].astype(str).str.strip()

if not pairs_to_remove:
    print("ℹ️ No pairs specified in pairs_to_remove; nothing to remove.")
else:
    removed_total = 0
    for river_name, species_name in pairs_to_remove:
        mask = (df["river"] == str(river_name).strip()) & (df["Species_Plot"] == str(species_name).strip())
        count = int(mask.sum())
        if count:
            df = df.loc[~mask].reset_index(drop=True)
            removed_total += count
            print(f"🗑️ Removed {count} rows for ({river_name}, {species_name})")
        else:
            print(f"ℹ️ No rows matched ({river_name}, {species_name})")

    print(f"🧾 Total rows removed: {removed_total:,}")
    print(f"📊 Remaining rows: {len(df):,}")

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 87 complete — specified river/species combinations removed.")

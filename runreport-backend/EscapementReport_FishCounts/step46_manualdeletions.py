# step46_manualdeletions.py
# ------------------------------------------------------------
# Step 46 (v7): Manual cleanup — delete specific rows in DB
#
# Deletes rows from Escapement_PlotPipeline when EXACTLY ONE row
# matches all given field values. If 0 or >1 rows match a rule,
# that rule is skipped to avoid accidental mass deletion.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 46 (v7): Manual cleanup — deleting specific rows from DB...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# Normalize column names to underscore schema if needed
rename_map = {
    "Adult Total": "Adult_Total",
    "Jack Total": "Jack_Total",
    "Total Eggtake": "Total_Eggtake",
    "On Hand Adults": "On_Hand_Adults",
    "On Hand Jacks": "On_Hand_Jacks",
    "Lethal Spawned": "Lethal_Spawned",
    "Live Spawned": "Live_Spawned",
    "Live Shipped": "Live_Shipped",
}
df = df.rename(columns=rename_map)

# ------------------------------------------------------------
# Normalize dates
# ------------------------------------------------------------
if "date_iso" in df.columns:
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

# ------------------------------------------------------------
# MANUAL DELETION RULES
# ------------------------------------------------------------
manual_deletions = [
    {
        "pdf_name": "WA_EscapementReport_04-15-2021.pdf",
        "facility": "Kalama Falls Hatchery",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2021-03-19",
        "Adult_Total": 1.0,
    },
    {
        "pdf_name": "WA_EscapementReport_01-30-2025.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Type N Coho",
        "Stock": "H",
        "date_iso": "2025-01-21",
        "Adult_Total": 20005,
    },
    {
        "pdf_name": "WA_EscapementReport_01-30-2025.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Type N Coho",
        "Stock": "W",
        "date_iso": "2025-01-21",
        "Adult_Total": 13204,
    },
    {
        "pdf_name": "WA_EscapementReport_03-06-2025.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Type N Coho",
        "Stock": "W",
        "date_iso": "2025-02-27",
        "Adult_Total": 13206,
    },
    {
        "pdf_name": "WA_EscapementReport_11-06-2025.pdf",
        "facility": "Soos Creek Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2025-10-20",
        "Adult_Total": 11820,
    },
    {
        "pdf_name": "WA_EscapementReport_03-20-2025.pdf",
        "facility": "Eastbank Hatchery",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2025-03-10",
        "Adult_Total": 161,
    },
    {
        "pdf_name": "WA_EscapementReport_10-02-2025.pdf",
        "facility": "Lewis River Hatchery",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2025-09-30",
        "Adult_Total": 392,
    },
    {
        "pdf_name": "WA_EscapementReport_10-09-2025.pdf",
        "facility": "Lewis River Hatchery",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2025-10-07",
        "Adult_Total": 394,
    },
    {
        "pdf_name": "WA_EscapementReport_10-16-2025.pdf",
        "facility": "Lewis River Hatchery",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2025-10-14",
        "Adult_Total": 395,
    },
    {
        "pdf_name": "WA_EscapementReport_09-18-2025.pdf",
        "facility": "Merwin Dam FCF",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2025-09-17",
        "Adult_Total": 3226,
    },
    {
        "pdf_name": "WA_EscapementReport_09-25-2025.pdf",
        "facility": "Merwin Dam FCF",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2025-09-23",
        "Adult_Total": 3349,
    },
    {
        "pdf_name": "WA_EscapementReport_10-02-2014.pdf",
        "facility": "Merwin Dam FCF",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2014-10-01",
        "Adult_Total": 7058,
    },
    {
        "pdf_name": "WA_EscapementReport_03-19-2020.pdf",
        "facility": "Wynoochee River Dam Trap",
        "species": "Winter-Late Steelhead",
        "Stock": "H",
        "date_iso": "2014-10-01",
        "Adult_Total": 660,
    },
    {
        "pdf_name": "WA_EscapementReport_05-11-2023.pdf",
        "facility": "Tacoma Power – Wynoochee River",
        "species": "Winter-Late Steelhead",
        "Stock": "H",
        "date_iso": "2023-04-27",
        "Adult_Total": 1028,
    },
]

# ------------------------------------------------------------
# MATCH & DELETE
# ------------------------------------------------------------
delete_indices = set()

for rule in manual_deletions:
    cond = pd.Series(True, index=df.index)

    for col, val in rule.items():
        if col not in df.columns:
            print(f"⚠️ Column '{col}' missing — skipping rule: {rule}")
            cond &= False
            continue

        if col == "date_iso":
            val = pd.to_datetime(val, errors="coerce")

        cond &= (df[col] == val)

    matches = df[cond]

    if len(matches) == 0:
        print(f"⚠️ No match found for → {rule}")
    elif len(matches) == 1:
        idx = matches.index[0]
        delete_indices.add(idx)
        print(f"🗑️ Deleting row → {rule}")
    else:
        print(f"⛔ WARNING — {len(matches)} matches found, skipping rule to avoid mass deletion.")
        print(matches)

# ------------------------------------------------------------
# APPLY DELETIONS
# ------------------------------------------------------------
before = len(df)
df = df.drop(index=delete_indices).reset_index(drop=True)
after = len(df)
removed = before - after

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("------------------------------------------------------------")
print(f"✅ Manual cleanup complete!")
print(f"🧽 Rows removed: {removed:,}")
print(f"📊 Remaining rows: {after:,}")
print("------------------------------------------------------------")

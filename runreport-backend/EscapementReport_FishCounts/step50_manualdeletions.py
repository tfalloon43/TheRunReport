# step50_manualdeletions.py
# ------------------------------------------------------------
# Step 50 (v7): Manual cleanup — delete specific rows in DB
#
# Deletes rows from Escapement_PlotPipeline when EXACTLY ONE row
# matches all given field values. If 0 or >1 rows match a rule,
# that rule is skipped to avoid accidental mass deletion.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 50 (v7): Manual cleanup — deleting specific rows from DB...")

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
        "pdf_name": "WA_EscapementReport_01-16-2025.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Type N Coho",
        "Stock": "H",
        "date_iso": "2025-01-14",
        "Adult_Total": 24514,
    },
    {
        "pdf_name": "WA_EscapementReport_02-20-2025.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Type N Coho",
        "Stock": "H",
        "date_iso": "2025-02-18",
        "Adult_Total": 20006,
    },
    {
        "pdf_name": "WA_EscapementReport_12-12-2019.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2019-12-10",
        "Adult_Total": 829,
    },
    {
        "pdf_name": "WA_EscapementReport_01-09-2020.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2019-12-31",
        "Adult_Total": 825,
    },
    {
        "pdf_name": "WA_EscapementReport_01-07-2021.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2020-11-30",
        "Adult_Total": 1023,
    },
    {
        "pdf_name": "WA_EscapementReport_01-08-2026.pdf",
        "facility": "Forks Creek Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2025-12-22",
        "Adult_Total": 2873,
    },
    {
        "pdf_name": "WA_EscapementReport_12-11-2025.pdf",
        "facility": "Baker Lake Hatchery",
        "species": "Sockeye",
        "Stock": "U",
        "date_iso": "2025-12-08",
        "Adult_Total": 10563,
    },
    {
        "pdf_name": "WA_EscapementReport_01-11-2024.pdf",
        "facility": "Baker Lake Hatchery",
        "species": "Sockeye",
        "Stock": "U",
        "date_iso": "2023-12-22",
        "Adult_Total": 10648,
    },
    {
        "pdf_name": "WA_EscapementReport_02-08-2024.pdf",
        "facility": "Bogachiel Hatchery",
        "species": "Winter Steelhead",
        "Stock": "H",
        "date_iso": "2024-02-06",
        "Adult_Total": 1344,
    },
    {
        "pdf_name": "WA_EscapementReport_03-05-2015.pdf",
        "facility": "Bogachiel Hatchery",
        "species": "Winter Steelhead",
        "Stock": "H",
        "date_iso": "2015-03-04",
        "Adult_Total": 3612,
    },
    {
        "pdf_name": "WA_EscapementReport_12-19-2024.pdf",
        "facility": "Cedar River Hatchery",
        "species": "Sockeye",
        "Stock": "U",
        "date_iso": "2024-12-19",
        "Adult_Total": 2617,
    },
    {
        "pdf_name": "WA_EscapementReport_01-02-2025.pdf",
        "facility": "Cedar River Hatchery",
        "species": "Sockeye",
        "Stock": "U",
        "date_iso": "2024-12-30",
        "Adult_Total": 2623,
    },
    {
        "pdf_name": "WA_EscapementReport_03-06-2025.pdf",
        "facility": "Cowlitz Salmon Hatchery",
        "species": "Type N Coho",
        "Stock": "H",
        "date_iso": "2025-02-28",
        "Adult_Total": 20008,
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
        "pdf_name": "WA_EscapementReport_10-27-2016.pdf",
        "facility": "Naselle Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2016-10-25",
        "Adult_Total": 2005,
    },
    {
        "pdf_name": "WA_EscapementReport_01-07-2022.pdf",
        "facility": "Naselle Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2021-11-23",
        "Adult_Total": 8335,
    },
    {
        "pdf_name": "WA_EscapementReport_01-08-2026.pdf",
        "facility": "Kendall Creek Hatchery",
        "species": "Spring Chinook",
        "Stock": "H",
        "date_iso": "2025-09-23",
        "Adult_Total": 3471,
    },
    {
        "pdf_name": "WA_EscapementReport_01-02-2025.pdf",
        "facility": "Kendall Creek Hatchery",
        "species": "Chum",
        "Stock": "U",
        "date_iso": "2024-12-26",
        "Adult_Total": 6798,
    },
    {
        "pdf_name": "WA_EscapementReport_01-16-2025.pdf",
        "facility": "Kendall Creek Hatchery",
        "species": "Chum",
        "Stock": "U",
        "date_iso": "2025-01-06",
        "Adult_Total": 6832,
    },
    {
        "pdf_name": "WA_EscapementReport_01-08-2026.pdf",
        "facility": "Voights Creek Hatchery",
        "species": "Odd Year Pink",
        "Stock": "W",
        "date_iso": "2025-11-03",
        "Adult_Total": 367,
    },
    {
        "pdf_name": "WA_EscapementReport_11-06-2025.pdf",
        "facility": "Bingham Creek Hatchery",
        "species": "Coho",
        "Stock": "H",
        "date_iso": "2025-11-03",
        "Adult_Total": 26000,
    },
    {
        "pdf_name": "WA_EscapementReport_09-19-2024.pdf",
        "facility": "Marblemount Hatchery",
        "species": "Spring Chinook",
        "Stock": "H",
        "date_iso": "2024-09-03",
        "Adult_Total": 3790,
    },
    {
        "pdf_name": "WA_EscapementReport_12-06-2018.pdf",
        "facility": "Marblemount Hatchery",
        "species": "Coho",
        "Stock": "H",
        "date_iso": "2018-12-04",
        "Adult_Total": 8960,
    },
    {
        "pdf_name": "WA_EscapementReport_01-02-2020.pdf",
        "facility": "Marblemount Hatchery",
        "species": "Coho",
        "Stock": "H",
        "date_iso": "2019-12-10",
        "Adult_Total": 8080,
    },
    {
        "pdf_name": "WA_EscapementReport_12-14-2023.pdf",
        "facility": "Marblemount Hatchery",
        "species": "Coho",
        "Stock": "H",
        "date_iso": "2023-12-08",
        "Adult_Total": 25769,
    },
    {
        "pdf_name": "WA_EscapementReport_12-21-2023.pdf",
        "facility": "Marblemount Hatchery",
        "species": "Coho",
        "Stock": "H",
        "date_iso": "2023-12-19",
        "Adult_Total": 25985,
    },
    {
        "pdf_name": "WA_EscapementReport_01-07-2021.pdf",
        "facility": "Marblemount Hatchery",
        "species": "Coho",
        "Stock": "W",
        "date_iso": "2020-12-09",
        "Adult_Total": 422,
    },
    {
        "pdf_name": "WA_EscapementReport_01-08-2026.pdf",
        "facility": "Sol Duc Hatchery",
        "species": "Summer Chinook",
        "Stock": "H",
        "date_iso": "2025-09-23",
        "Adult_Total": 1551,
    },
    {
        "pdf_name": "WA_EscapementReport_01-08-2026.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2025-11-12",
        "Adult_Total": 881,
    },
    {
        "pdf_name": "WA_EscapementReport_11-17-2016.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2016-10-31",
        "Adult_Total": 1084,
    },
    {
        "pdf_name": "WA_EscapementReport_11-17-2016.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "W",
        "date_iso": "2016-10-25",
        "Adult_Total": 348,
    },
    {
        "pdf_name": "WA_EscapementReport_11-21-2019.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2019-11-12",
        "Adult_Total": 2394,
    },
    {
        "pdf_name": "WA_EscapementReport_01-02-2020.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2019-11-18",
        "Adult_Total": 2395,
    },
    {
        "pdf_name": "WA_EscapementReport_01-07-2022.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2021-11-15",
        "Adult_Total": 2377,
    },
    {
        "pdf_name": "WA_EscapementReport_11-16-2023.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2023-11-01",
        "Adult_Total": 451,
    },
    {
        "pdf_name": "WA_EscapementReport_11-21-2019.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "W",
        "date_iso": "2019-11-12",
        "Adult_Total": 171,
    },
    {
        "pdf_name": "WA_EscapementReport_01-02-2020.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "W",
        "date_iso": "2019-11-18",
        "Adult_Total": 172,
    },
    {
        "pdf_name": "WA_EscapementReport_01-07-2021.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "W",
        "date_iso": "2020-10-27",
        "Adult_Total": 419,
    },
    {
        "pdf_name": "WA_EscapementReport_11-09-2023.pdf",
        "facility": "Toutle River Hatchery",
        "species": "Fall Chinook",
        "Stock": "W",
        "date_iso": "2023-10-26",
        "Adult_Total": 133,
    },
    {
        "pdf_name": "WA_EscapementReport_08-15-2024.pdf",
        "facility": "Skamania Hatchery",
        "species": "Summer Steelhead",
        "Stock": "H",
        "date_iso": "2024-08-13",
        "Adult_Total": 2045,
    },
    {
        "pdf_name": "WA_EscapementReport_12-04-2025.pdf",
        "facility": "Forks Creek Hatchery",
        "species": "Fall Chinook",
        "Stock": "H",
        "date_iso": "2025-12-02",
        "Adult_Total": 2870,
    },
    {
        "pdf_name": "WA_EscapementReport_01-17-2019.pdf",
        "facility": "Wynoochee River Dam Trap",
        "species": "Coho",
        "Stock": "W",
        "date_iso": "2019-01-15",
        "Adult_Total": 835,
    },
    {
        "pdf_name": "WA_EscapementReport_01-20-2022.pdf",
        "facility": "Wynoochee River Dam Trap",
        "species": "Coho",
        "Stock": "W",
        "date_iso": "2022-01-18",
        "Adult_Total": 1565,
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

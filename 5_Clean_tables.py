# clean_tables.py
import sqlite3
import pandas as pd

db_path = "pdf_data.sqlite"

# -------------------------------
# Phase 1: SQL-based hard deletes
# -------------------------------
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# NEW: drop rows where date is NULL or blank
cursor.execute(
    "DELETE FROM escapement_data WHERE date IS NULL OR TRIM(date) = ''"
)

# Exact-match patterns to remove (entire line noise appearing in hatchery column)
unwanted_patterns_exact = [
    # "Thursday",  # (left commented, as in your original)
    "CAUTION - All Numbers represent preliminary estimates only",
    "Adult Jack Total On Hand On Hand Lethal Live Live",
    "Stock-BO Total Total Eggtake Adults",
    "WDFW In-Season Hatchery Escapement Report",
    "- M -",
    "Hatchery.",
    "Stock-",
    "Final in-season estimate.",
    "Hatchery Stock- M",
    "season estimate.",
    "(",
    "Mixed-",
    "Aberdeen Hatchery.",
    "Springs Hatchery.",
    "River-",
    "Hatchery. Final in-",
    "Eastbank-",
    "Pond)-",
    "River- U",
    "M",
    "WEIR Hatchery.",
    "Late Chinook",
    "Resident Coastal Cutthroat",
    "WEIR",
    "WEIR Stock-",
    "Hatchery. Final in-season",
    "estimate.",
    "Springs Hatchery. Final",
    "in-season estimate.",
    "Friday",
    "WEIR Hatchery. Final in-",
    "WYNOOCHEE R DAM Hatchery.",
    "NFH. Final in-season",
    "Rainbow",
    "Outfall-",
    "held at Cowlitz Salmon",
    "Hatchery",
    "Hatchery. Mortalities due",
    "to intentional study. Final",
    "(Wenatchee)- W",
    "Mixed- W",
    "Stock- H",
    "Stock- W",
    "Hatchery Stock- H",
    "Community College.",
    "brood.",
    "W",
    "Stock- H Hatchery.",
    "Stock- H Hatchery. Final in-",
    "H",
    "H Hatchery. Final in-",
    "W Hatchery. Final in-",
    "River- W Hatchery. Final in-",
    "River- W",
    "Eastbank- H",
    "Pond)- H",
    "River- W Hatchery.",
    "Stock- H Salmon Hatchery.",
    "H Hatchery.",
    "W Hatchery.",
    "River- W Hatchery. Final in-season",
    "Stock- H Hatchery. Final in-season",
    "River- H",
    "Stock- H lakes.",
    "WEIR Stock- H",
    "W Hatchery. Final in-season",
    "Hatchery Stock- H Aberdeen Hatchery. Final",
    "WYNOOCHEE R DAM",
    "River- H Hatchery.",
    "WYNOOCHEE R DAM Hatchery Stock- H Hatchery.",
    "Endemic- W",
    "Endemic-",
    "Mixed- H",
    "H Hatchery. Final in-season",
    "Aberdeen Hatchery",
    "Outfall- H",
    "HATCHERY",
    "Hatchery Stock- H Aberdeen Hatchery.",
    "WYNOOCHEE R DAM Hatchery. Released to",
    "landlocked lakes.",
    "Hatchery.Final in-season",
    "HATCHERY Hatchery Stock- H",
    "WEIR Hatchery. Final in-season",
    "Hatchery Stock- H Aberdeen Hatchery.",
    "HATCHERY Stock- H",
    "Hatchery. M ortalities due",
    "WEIR Shipped to Washougal",
    "Stock- H Shipped to Speelyai",
    "NFH.",
    "for BY",
    "season estimate for",
    "Acclim-",
    "Springs Hatchery",
    "Odd Year Pink",
    "Monday",
    "Hatchery Final in-season",
    "(Wenatchee)-",
    "WEIR for BY",
    "M for BY",
    "Creek Hatchery.",
    "Creek.",
    "Endemic- W for BY",
    "Endemic- W season estimate.",
    "estimates.",
    "for",
    "lakes.",
    "River Hatchery.",
    "River- W for BY",
    "Salmon Hatchery.",
    "season estimate",
    "",  # keep if you want exact-empty matches in hatchery removed here too
]

for pattern in unwanted_patterns_exact:
    cursor.execute(
        "DELETE FROM escapement_data WHERE hatchery = ?",
        (pattern,)
    )

# Remove NULL/blank hatchery right away
cursor.execute(
    "DELETE FROM escapement_data WHERE hatchery IS NULL OR TRIM(hatchery) = ''"
)

# LIKE-based removals (contains)
unwanted_patterns_like = [
    "Thursday",
    "CAUTION - All Numbers represent preliminary estimates only",
    "Adult Jack Total On Hand On Hand Lethal Live Live",
    "Stock-BO Total Total Eggtake Adults",
    "WDFW In-Season Hatchery Escapement Report",
    "Wednesday",
    "M - -",
    "H - -",
    "W - -",
    " - -",
    "U -",
    "- C",
    "- M",
    "season estimate",
]

for pattern in unwanted_patterns_like:
    cursor.execute(
        "DELETE FROM escapement_data WHERE hatchery LIKE ?",
        (f"%{pattern}%",)
    )

conn.commit()
conn.close()
print("Phase 1 ✅ SQL deletes done (unwanted hatchery rows + blank hatchery/date).")


# ------------------------------------------------
# Phase 2: Merge stock-only rows, then Pandas cleanup
# ------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM escapement_data ORDER BY id", conn)

# Merge pattern: if a row has blank/None hatchery or is exactly 'Stock-', push its stock to previous row
rows_to_drop = []
for i in range(1, len(df)):
    hatchery_value = df.at[i, "hatchery"]
    if (hatchery_value is None or str(hatchery_value).strip() == "" or str(hatchery_value).strip() == "Stock-"):
        # Move stock to previous row if present
        if pd.notna(df.at[i, "stock"]) and str(df.at[i, "stock"]).strip() != "":
            df.at[i-1, "stock"] = df.at[i, "stock"]
        rows_to_drop.append(i)

df.drop(rows_to_drop, inplace=True)
df.reset_index(drop=True, inplace=True)

# Save intermediate cleaned table
df.to_sql("escapement_data_cleaned", conn, if_exists="replace", index=False)
conn.close()
print("Phase 2 ✅ Merged stock-only rows → escapement_data_cleaned.")


# ------------------------------------------------
# Phase 3: Pandas filtering on escapement_data_cleaned
# ------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM escapement_data_cleaned ORDER BY id", conn)

# 3a) Drop rows where stock == "IDK" AND adult_total is missing / ',' / 'None'
mask_missing = df['adult_total'].isna()
mask_comma_or_none = df['adult_total'].astype(str).str.strip().isin([',', 'None'])
drop_idx = df[(df['stock'] == "IDK") & (mask_missing | mask_comma_or_none)].index
df.drop(drop_idx, inplace=True)

# 3b) Drop rows where date is blank or null (again, in case any slipped through merges)
drop_idx = df[df['date'].isna() | (df['date'].astype(str).str.strip() == "")].index
df.drop(drop_idx, inplace=True)

# 3c) Remove duplicate rows where the *second instance* has stock/adult_total/date all None
dups = df[df.duplicated(subset=["hatchery"], keep="first")]
drop_idx = dups[
    dups['stock'].isna() &
    dups['adult_total'].isna() &
    dups['date'].isna()
].index
df.drop(drop_idx, inplace=True)

# Finalize
df.reset_index(drop=True, inplace=True)
df.to_sql("escapement_data_cleaned", conn, if_exists="replace", index=False)

conn.close()
print("Phase 3 ✅ escapement_data_cleaned updated: removed IDK+None, blank dates, and duplicate empty rows.")
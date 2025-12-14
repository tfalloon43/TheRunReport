# step44_cleanup6.py
# ------------------------------------------------------------
# Step 44: Cleanup (v6) — Remove biologically invalid negative rows.
#
# Deletes rows inside the DATABASE table Escapement_PlotPipeline
# using updated iteration6 metrics:
#
#   Rule 1️⃣: Last row in a biological identity group has adult_diff6 < 0
#   Rule 2️⃣: adult_diff6 < 0 AND by_adult6_length = 1 AND NEXT row adult_diff6 < 0
#
# After removal, rewritten back into Escapement_PlotPipeline.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path


# ------------------------------------------------------------
# Reorder helper
# ------------------------------------------------------------

def reorder_for_output(df):
    sort_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]
    missing = [c for c in sort_cols if c not in df.columns]
    if missing:
        return df
    df = df.copy()
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
    df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
    return df.sort_values(
        by=sort_cols,
        ascending=[True, True, True, True, True, False],
        na_position="last",
        kind="mergesort",
    )


print("🧹 Step 44 (v6 Cleanup): Removing invalid negative-diff rows...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE FROM DB
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = [
    "facility",
    "species",
    "Stock",
    "Stock_BO",
    "date_iso",
    "adult_diff6",
    "by_adult6_length"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["adult_diff6"] = pd.to_numeric(df["adult_diff6"], errors="coerce").fillna(0)
df["by_adult6_length"] = pd.to_numeric(df["by_adult6_length"], errors="coerce").fillna(0).astype(int)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# Keep original DB index for deletion tracking
df["orig_index"] = df.index

# ------------------------------------------------------------
# IDENTIFY ROWS TO DELETE
# ------------------------------------------------------------
to_delete = set()

def mark_rows_for_deletion(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    # Rule 1️⃣: Last row negative
    if len(g) > 0 and g.loc[len(g) - 1, "adult_diff6"] < 0:
        to_delete.add(g.loc[len(g) - 1, "orig_index"])

    # Rule 2️⃣: negative diff, by_adult6_length=1, next row negative
    for i in range(len(g) - 1):
        curr = g.loc[i]
        nxt = g.loc[i + 1]

        if (
            curr["adult_diff6"] < 0
            and curr["by_adult6_length"] == 1
            and nxt["adult_diff6"] < 0
        ):
            to_delete.add(curr["orig_index"])

df.groupby(group_cols, group_keys=False).apply(mark_rows_for_deletion)

# ------------------------------------------------------------
# DELETE MARKED ROWS
# ------------------------------------------------------------
before = len(df)
df = df[~df["orig_index"].isin(to_delete)].reset_index(drop=True)
after = len(df)
removed = before - after

df = df.drop(columns=["orig_index"])

print(f"🗑️ Rows marked for deletion: {len(to_delete):,}")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
print("💾 Writing cleaned dataset back to Escapement_PlotPipeline...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print(f"✅ Cleanup v6 complete!")
print(f"🧽 Removed {removed:,} biologically invalid rows:")
print("   • Last-row negative diffs")
print("   • Isolated short-run negatives (len=1 + next row negative)")
print(f"📊 Final dataset size: {after:,} rows")

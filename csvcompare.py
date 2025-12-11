#!/usr/bin/env python3
# csvcompare.py
#
# Purpose:
#   From file A, list rows that do not appear anywhere in file B
#   (match is on all shared columns, regardless of order).
#
# Output:
#   /Users/thomasfalloon/Desktop/TheRunReport/diff.csv

import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CSV_A = "/Users/thomasfalloon/Desktop/TheRunReport/100_Data/csv_plotdata.csv"
CSV_B = "/Users/thomasfalloon/Desktop/TheRunReport/runreport-backend/Escapement_PlotPipeline.csv"
OUT_DIFF = "/Users/thomasfalloon/Desktop/TheRunReport/diff.csv"

print(f"🔍 Comparing\n  A: {CSV_A}\n  B: {CSV_B}")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
A = pd.read_csv(CSV_A)
B = pd.read_csv(CSV_B)

print(f"📄 Loaded A: {A.shape[0]:,} rows, {A.shape[1]} cols")
print(f"📄 Loaded B: {B.shape[0]:,} rows, {B.shape[1]} cols")

# ------------------------------------------------------------
# Column-name aliases for B (spaces -> underscores)
# ------------------------------------------------------------
alias = {
    "Adult Total": "Adult_Total",
    "Jack Total": "Jack_Total",
    "Total Eggtake": "Total_Eggtake",
    "On Hand Adults": "On_Hand_Adults",
    "On Hand Jacks": "On_Hand_Jacks",
    "Lethal Spawned": "Lethal_Spawned",
    "Live Spawned": "Live_Spawned",
    "Live Shipped": "Live_Shipped",
}

rename_map = {old: new for old, new in alias.items() if old in B.columns}
if rename_map:
    print(f"🔧 Applying alias renames on B: {rename_map}")
    B = B.rename(columns=rename_map)

# ------------------------------------------------------------
# Normalize date columns (string 'YYYY-MM-DD')
# ------------------------------------------------------------
for df_name, df in (("A", A), ("B", B)):
    for col in ("date_iso", "pdf_date"):
        if col in df.columns:
            # Strip any time component so dates like 2016-11-30 00:00:00
            # compare equal to 2016-11-30.
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d")
                .fillna("")
            )

# ------------------------------------------------------------
# Shared columns to use for matching
# ------------------------------------------------------------
shared_cols = sorted(set(A.columns) & set(B.columns))
if not shared_cols:
    raise SystemExit("❌ No shared columns between A and B; cannot compare.")

print(f"🔗 Shared columns: {len(shared_cols)}")

# ------------------------------------------------------------
# Hash rows for membership test (all shared columns)
# ------------------------------------------------------------
def row_hash(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    cols_sorted = cols  # already sorted
    # Normalize cell values so that ints/floats compare the same and stray
    # whitespace is ignored.
    def normalize_value(v):
        if pd.isna(v):
            return ""
        if isinstance(v, float):
            # Remove trailing .0 for whole numbers, keep precision otherwise
            return str(int(v)) if v.is_integer() else f"{v:.15g}"
        return str(v).strip()

    normalized = df[cols_sorted].map(normalize_value)

    return pd.util.hash_pandas_object(
        normalized.agg("¶".join, axis=1),
        index=False,
    )

hash_B = set(row_hash(B, shared_cols))
hash_A = row_hash(A, shared_cols)

# Mask rows in A that are missing in B
missing_mask = ~hash_A.isin(hash_B)
missing_rows = A.loc[missing_mask].copy()

missing_rows.to_csv(OUT_DIFF, index=False)

print(f"✅ Rows in A not found in B: {len(missing_rows):,}")
print(f"📝 Diff CSV written → {OUT_DIFF}")

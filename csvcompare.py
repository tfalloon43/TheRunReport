#!/usr/bin/env python3
# csvcompare.py
#
# Compare:
#   A = csv_plotdata.csv
#   B = Escapement_PlotPipeline.csv
#
# Normalizes:
#   - column name aliases (Adult Total -> Adult_Total, etc. in B)
#   - date formats for date_iso + pdf_date
# Aligns rows by `index` and reports differences.
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
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d")
                .fillna("")
            )

# ------------------------------------------------------------
# Determine shared columns & ensure 'index' exists
# ------------------------------------------------------------
shared_cols = sorted(set(A.columns) & set(B.columns))
if "index" not in shared_cols:
    raise SystemExit("❌ 'index' column not found in BOTH CSVs; cannot safely align rows.")

print(f"🔗 Shared columns: {len(shared_cols)}")

# Restrict to shared columns, set index
A_s = A[shared_cols].set_index("index")
B_s = B[shared_cols].set_index("index")

# ------------------------------------------------------------
# Outer join on index to see membership
# ------------------------------------------------------------
merged = A_s.merge(
    B_s,
    how="outer",
    left_index=True,
    right_index=True,
    suffixes=("_A", "_B"),
    indicator=True,
)

print("📊 Row membership by index:")
print(merged["_merge"].value_counts())

both_mask = merged["_merge"] == "both"
onlyA_mask = merged["_merge"] == "left_only"
onlyB_mask = merged["_merge"] == "right_only"

# ------------------------------------------------------------
# Cell-wise differences for rows present in BOTH
# ------------------------------------------------------------
both = merged[both_mask].copy()
data_cols = [c for c in shared_cols if c != "index"]

diffs_any = pd.Series(False, index=both.index)
diff_counts = {}

for c in data_cols:
    ca = both[f"{c}_A"]
    cb = both[f"{c}_B"]
    # equal if same or both NaN
    same = (ca == cb) | (ca.isna() & cb.isna())
    diff_mask = ~same
    if diff_mask.any():
        diffs_any |= diff_mask
        diff_counts[c] = int(diff_mask.sum())
    else:
        diff_counts[c] = 0

print("📊 Column-wise differing row counts (rows where that column differs):")
for c in data_cols:
    print(f"  - {c}: {diff_counts[c]:,}")

# Rows present in both files where at least one shared column differs
diff_rows = both[diffs_any].copy()

# ------------------------------------------------------------
# For each differing row, list which columns differ
# ------------------------------------------------------------
def list_diff_cols(row):
    cols = []
    for c in data_cols:
        va = row[f"{c}_A"]
        vb = row[f"{c}_B"]
        if not ((pd.isna(va) and pd.isna(vb)) or (va == vb)):
            cols.append(c)
    return ", ".join(cols)

if not diff_rows.empty:
    diff_rows["diff_cols"] = diff_rows.apply(list_diff_cols, axis=1)
else:
    diff_rows["diff_cols"] = ""

diff_rows["_merge"] = "both"

# ------------------------------------------------------------
# Rows only in A or only in B
# ------------------------------------------------------------
onlyA_df = merged[onlyA_mask].copy()
if not onlyA_df.empty:
    onlyA_df["diff_cols"] = "ONLY_IN_A"

onlyB_df = merged[onlyB_mask].copy()
if not onlyB_df.empty:
    onlyB_df["diff_cols"] = "ONLY_IN_B"

# ------------------------------------------------------------
# Combine & save
# ------------------------------------------------------------
combined = pd.concat([diff_rows, onlyA_df, onlyB_df], axis=0)
combined = combined.sort_index()

combined.to_csv(OUT_DIFF)
print(f"✅ Diff CSV written → {OUT_DIFF}")
print(f"🔢 Total differing / unmatched indices: {combined.shape[0]:,}")
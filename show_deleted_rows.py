#!/usr/bin/env python3
"""
show_deleted_rows.py
------------------------------------------------------------
Compare File A vs File B using a key of:
    date_iso, Adult_Total, basin, Stock

Outputs:
    diff_key_only_in_A.csv  # rows in A whose key is missing in B
    diff_key_only_in_B.csv  # rows in B whose key is missing in A
"""

from pathlib import Path
import pandas as pd
from pandas.util import hash_pandas_object as hpo

ROOT = Path(__file__).resolve().parent
CSV_A = ROOT / "100_Data" / "csv_plotdata.csv"
CSV_B = ROOT / "runreport-backend" / "Escapement_PlotPipeline.csv"
OUT_A = ROOT / "diff_key_only_in_A.csv"
OUT_B = ROOT / "diff_key_only_in_B.csv"

key_cols = ["date_iso", "Adult_Total", "basin", "Stock"]

print(f"🔍 Comparing on key {key_cols}")
print(f"  A: {CSV_A}")
print(f"  B: {CSV_B}")

# Load
A = pd.read_csv(CSV_A)
B = pd.read_csv(CSV_B)

print(f"📄 Loaded A: {len(A):,} rows")
print(f"📄 Loaded B: {len(B):,} rows")

# Normalize date and numbers
for df_name, df in (("A", A), ("B", B)):
    if "date_iso" in df.columns:
        df["date_iso"] = (
            pd.to_datetime(df["date_iso"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
            .fillna("")
        )
    if "Adult_Total" in df.columns:
        df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce")

# Build hashed key
def hashed(df: pd.DataFrame) -> pd.Series:
    return hpo(df[key_cols].astype(str).agg("¶".join, axis=1), index=False)

A_key = hashed(A)
B_key = hashed(B)

setA = set(A_key)
setB = set(B_key)

only_in_A = A.loc[~A_key.isin(setB)].copy()
only_in_B = B.loc[~B_key.isin(setA)].copy()

only_in_A.to_csv(OUT_A, index=False)
only_in_B.to_csv(OUT_B, index=False)

print(f"✅ Rows in A but not B (key match): {len(only_in_A):,} → {OUT_A}")
print(f"✅ Rows in B but not A (key match): {len(only_in_B):,} → {OUT_B}")

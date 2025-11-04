# 12_by_2.py
# ------------------------------------------------------------
# Step 12: Build 'by2' (biological year v2) from bio_year,
# adjusting only near boundaries using local (±5 row) evidence
# gated by bio_year_fix.
#
# For each (facility, species, Stock, Stock_BO) group:
#   - Start with by2 = bio_year.
#   - Identify "boundary" rows: within ±5 rows there exists at least
#     one neighbor whose bio_year != current bio_year.
#   - For boundary rows, among neighbors with the SAME bio_year_fix
#     as the current row, compute the majority bio_year in the ±5 window.
#     If majority exists and differs from current bio_year, set by2 to it.
#
# Input : 100_Data/csv_plotdata.csv (requires columns:
#         facility, species, Stock, Stock_BO, date_iso,
#         bio_year, bio_year_fix)
# Output: 100_Data/12_by_2_output.csv (+ updates csv_plotdata.csv)
# ------------------------------------------------------------

import pandas as pd
from collections import Counter
from pathlib import Path

print("🏗️ Step 12: Building by2 with ±5 neighbor vote gated by bio_year_fix...")

# ----------------------------
# Paths
# ----------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_plotdata.csv"
output_path  = data_dir / "12_by_2_output.csv"
recent_path  = data_dir / "csv_plotdata.csv"

# ----------------------------
# Load & checks
# ----------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "bio_year", "bio_year_fix"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# Normalize dtypes
df["date_iso"]   = pd.to_datetime(df["date_iso"], errors="coerce")
df["bio_year"]   = pd.to_numeric(df["bio_year"], errors="coerce")
df["bio_year_fix"] = pd.to_numeric(df["bio_year_fix"], errors="coerce")

# Sort for stable neighborhood windows
group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# Initialize by2 = bio_year
df["by2"] = df["bio_year"]

WINDOW = 5  # lookahead/lookbehind size

def adjust_group(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("date_iso").copy()
    by2 = g["bio_year"].to_numpy()
    bio  = g["bio_year"].to_numpy()
    fix  = g["bio_year_fix"].to_numpy()
    n = len(g)

    # Precompute neighbor ranges for speed
    for i in range(n):
        # window bounds (inclusive)
        lo = max(0, i - WINDOW)
        hi = min(n - 1, i + WINDOW)

        # If there is no neighbor with a different bio_year → not a boundary
        neighbor_bio = bio[lo:hi+1]
        if (neighbor_bio == bio[i]).all():
            continue  # keep by2[i] as is

        # Filter to neighbors with SAME bio_year_fix as current row
        same_fix_mask = (fix[lo:hi+1] == fix[i])

        # If no neighbors with same fix, keep as-is
        if not same_fix_mask.any():
            continue

        neighbor_bio_same_fix = neighbor_bio[same_fix_mask]

        # Majority vote among neighbor bio_years (same fix)
        counts = Counter(neighbor_bio_same_fix)
        if not counts:
            continue

        # Get most common (bio_year, count)
        (major_bio, major_count) = counts.most_common(1)[0]

        # Only adjust if majority differs from current
        if pd.notna(major_bio) and major_bio != bio[i]:
            by2[i] = major_bio

    g["by2"] = by2
    return g

# Apply per group
out = (
    df.groupby(group_cols, group_keys=False, sort=False)
      .apply(adjust_group)
      .reset_index(drop=True)
)

# Save
out.to_csv(output_path, index=False)
out.to_csv(recent_path, index=False)

# Report
changed = (out["by2"] != out["bio_year"]).sum()
print(f"✅ by2 created → {output_path}")
print(f"🔄 csv_plotdata.csv updated with by2")
print(f"🧮 Rows adjusted near boundaries: {changed:,}")
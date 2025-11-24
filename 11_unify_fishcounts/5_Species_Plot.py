# 5_Species_Plot.py
# ------------------------------------------------------------
# Step 5: Create "Species_Plot" column from identifier.
#
# Logic:
#   • identifier looks like: "Columbia River - Rocky Reach Dam - Sockeye"
#   • Species_Plot = everything AFTER the last " - "
#
# Input  : 100_Data/csv_unify_fishcounts.csv
# Output : 100_Data/5_Species_Plot.csv
#          + updates csv_unify_fishcounts.csv in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🐟 Step 5: Extracting Species_Plot from identifier...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_unify_fishcounts.csv"
output_path  = data_dir / "5_Species_Plot.csv"
recent_path  = data_dir / "csv_unify_fishcounts.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_unify_fishcounts.csv")

# ------------------------------------------------------------
# Validate required column
# ------------------------------------------------------------
if "identifier" not in df.columns:
    raise ValueError("❌ Column 'identifier' is missing.")

# ------------------------------------------------------------
# Extract Species_Plot
# ------------------------------------------------------------
def extract_species(identifier: str) -> str:
    """Return everything after the LAST ' - '."""
    if not isinstance(identifier, str):
        return ""
    parts = identifier.split(" - ")
    return parts[-1].strip() if parts else ""

df["Species_Plot"] = df["identifier"].apply(extract_species)

print("🌟 Added new column: Species_Plot")

# ------------------------------------------------------------
# Save output + update inplace
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved → {output_path}")
print("🔄 Updated csv_unify_fishcounts.csv in place")
print("✅ Step 5 complete.")
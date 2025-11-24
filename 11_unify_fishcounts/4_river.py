# 4_river.py
# ------------------------------------------------------------
# Step 4: Create "River" column from identifier.
#
# Logic:
#   • identifier looks like: "Baker Lake/Baker River - Chinook"
#   • River = everything BEFORE the first " - "
#
# Input  : 100_Data/csv_unify_fishcounts.csv
# Output : 100_Data/4_river_output.csv
#          + updates csv_unify_fishcounts.csv in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🌊 Step 4: Extracting River from identifier...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_unify_fishcounts.csv"
output_path  = data_dir / "4_river_output.csv"
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
    raise ValueError("❌ Column 'identifier' is missing from CSV.")

# ------------------------------------------------------------
# Extract River name
# ------------------------------------------------------------
def extract_river(identifier: str) -> str:
    """Return everything before the first ' - '."""
    if not isinstance(identifier, str):
        return ""
    return identifier.split(" - ")[0].strip()

df["river"] = df["identifier"].apply(extract_river)

print("🌟 Added new column: River")

# ------------------------------------------------------------
# Save output + update file
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved → {output_path}")
print("🔄 Updated csv_unify_fishcounts.csv in place")
print("✅ Step 4 complete.")
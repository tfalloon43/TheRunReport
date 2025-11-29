# 5_flowpresence.py
# ------------------------------------------------------------
# Step 5: Add flow_presence column to flows.csv
#
# Logic:
#   - If "Site 1" contains ANY non-empty value → flow_presence = "USGS"
#   - Else → blank
#
# Column order:
#   river | flow_presence | river_name | ...
#
# Output:
#   • 5_flowpresence_output.csv  (snapshot)
#   • flows.csv updated in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("💧 Step 5: Adding flow_presence column to flows.csv...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

flows_path = data_dir / "flows.csv"
output_path = data_dir / "5_flowpresence_output.csv"

# ------------------------------------------------------------
# Load flows.csv
# ------------------------------------------------------------
if not flows_path.exists():
    raise FileNotFoundError(f"❌ flows.csv not found at: {flows_path}")

df = pd.read_csv(flows_path)
print(f"📘 Loaded {len(df):,} rows from flows.csv")

# ------------------------------------------------------------
# Ensure "Site 1" column exists
# ------------------------------------------------------------
if "Site 1" not in df.columns:
    raise ValueError("❌ flows.csv does not contain a 'Site 1' column.")

# ------------------------------------------------------------
# Compute flow_presence
# ------------------------------------------------------------
def has_usgs(val):
    if pd.isna(val):
        return ""
    if str(val).strip() == "":
        return ""
    return "USGS"

df["flow_presence"] = df["Site 1"].apply(has_usgs)

print("💧 flow_presence column created based on Site 1")

# ------------------------------------------------------------
# Reorder columns: river | flow_presence | river_name | ...
# ------------------------------------------------------------
cols = df.columns.tolist()

if "river" not in cols:
    raise ValueError("❌ 'river' column missing from flows.csv")

if "river_name" not in cols:
    raise ValueError("❌ 'river_name' column missing from flows.csv")

# Remove then reinsert in correct order
cols.remove("flow_presence")
cols.remove("river_name")

# Insert positions
new_cols = []
for col in df.columns:
    if col == "river":
        new_cols.append("river")
        new_cols.append("flow_presence")
        new_cols.append("river_name")
        # skip original river_name so add it later
        break

# Now append the remaining columns except these three:
skip = {"river", "flow_presence", "river_name"}
for col in df.columns:
    if col not in skip:
        new_cols.append(col)

# Apply ordering
df = df[new_cols]

print("📑 Columns reordered: river → flow_presence → river_name → ...")

# ------------------------------------------------------------
# Save output + update flows.csv
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(flows_path, index=False)

print(f"💾 Snapshot saved → {output_path}")
print(f"🔄 flows.csv updated in place")
print("✅ Step 5 complete.")

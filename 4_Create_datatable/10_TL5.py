# 10_TL5.py
# ------------------------------------------------------------
# Build TL5 column:
#   • If stock_presence_lower has a value → TL5 = text_line
#   • Otherwise → TL5 = ""
#
# Input  : 100_data/csv_recent.csv
# Output : 100_data/10_TL5_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "10_TL5_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 10: Creating TL5 (copy text_line if stock_presence_lower exists)...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")
df = pd.read_csv(input_path)

# Ensure required columns exist
required = ["text_line", "stock_presence_lower"]
for col in required:
    if col not in df.columns:
        raise ValueError(f"❌ Missing required column: {col}")

# ------------------------------------------------------------
# Core logic
# ------------------------------------------------------------
df["TL5"] = df.apply(
    lambda r: str(r["text_line"]).strip()
    if isinstance(r.get("stock_presence_lower", ""), str)
    and r["stock_presence_lower"].strip()
    else "",
    axis=1,
)

# ------------------------------------------------------------
# Save outputs (in 100_data)
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total_rows = len(df)
populated = df["TL5"].astype(str).str.strip().ne("").sum()
print(f"✅ TL5 creation complete → {output_path}")
print(f"🔄 csv_recent.csv updated with TL5 column")
print(f"📊 {populated} of {total_rows} rows populated with TL5 (stock_presence_lower check)")
# 2_date_extract.py
# ------------------------------------------------------------
# Step 2: Extracts dates from text_line and saves both
# - 2_date_extract_output.csv
# - csv_recent.csv (latest snapshot)
# ------------------------------------------------------------

import pandas as pd
import re
from pathlib import Path

# --- Paths ---
# Get project root (one level up from this script)
project_root = Path(__file__).resolve().parents[1]

# Path to the data folder
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

# Input CSV produced by Step 0/1
input_path = data_dir / "csv_recent.csv"

if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 0 first.")

df = pd.read_csv(input_path)

# --- Extract date ---
def extract_date(text):
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    return m.group(1) if m else None

df["date"] = df["text_line"].apply(extract_date)

# --- Save outputs (in 100_data folder) ---
step_output = data_dir / "2_date_extract_output.csv"
recent_output = data_dir / "csv_recent.csv"

df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 2 complete → Saved:")
print(f"   📄 {step_output}")
print(f"   🔄 {recent_output} (latest snapshot)")

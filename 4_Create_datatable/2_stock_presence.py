# 2_stock_presence.py
# ------------------------------------------------------------
# Step 2: Adds "stock_presence" column.
# If a row has a date, look for standalone stock indicators (H/W/M/U/C)
# in the text_line and record it in stock_presence.
# Saves both step output and csv_recent snapshot (in 100_data/).
# ------------------------------------------------------------

import pandas as pd
import re
from pathlib import Path

print("▶ Running Step 2: stock_presence")

# --- Paths ---
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
step_output = data_dir / "2_stock_presence_output.csv"
recent_output = data_dir / "csv_recent.csv"

# --- Load previous output ---
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 1 first.")

df = pd.read_csv(input_path)

# --- Define helper ---
def find_stock_indicator(text):
    if not isinstance(text, str):
        return None
    # Look for standalone H/W/M/U/C surrounded by spaces or punctuation
    m = re.search(r'\b([HWUMC])\b', text)
    return m.group(1) if m else None

# --- Apply logic ---
def get_stock_presence(row):
    if pd.notna(row.get("date")) and str(row["date"]).strip() != "":
        return find_stock_indicator(row.get("text_line", ""))
    return None

df["stock_presence"] = df.apply(get_stock_presence, axis=1)

# --- Save outputs (in 100_data) ---
df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 2 complete → Saved:")
print(f"   📄 {step_output}")
print(f"   🔄 {recent_output} (latest snapshot)")

# 4_stock_presence_lower.py
# ------------------------------------------------------------
# Step 4: Adds "stock_presence_lower" column.
# If a row has a date but NO stock_presence,
# look at the *next* row's text_line for standalone stock indicators (H/W/M/U/C).
# Copies that indicator into the *lower* row (the one containing it).
# Saves both step output and csv_recent snapshot (in 100_data/).
# ------------------------------------------------------------

import pandas as pd
import re
from pathlib import Path

print("▶ Running Step 4: stock_presence_lower")

# --- Paths ---
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
step_output = data_dir / "4_stock_presence_lower_output.csv"
recent_output = data_dir / "csv_recent.csv"

# --- Load previous output ---
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 3 first.")

df = pd.read_csv(input_path)

# --- Helper to find standalone stock letter ---
def find_stock_indicator(text):
    if not isinstance(text, str):
        return None
    m = re.search(r'\b([HWUMC])\b', text)
    return m.group(1) if m else None

# --- Initialize new column ---
df["stock_presence_lower"] = None

# --- Iterate rows ---
for i in range(len(df) - 1):
    current_row = df.iloc[i]
    next_row = df.iloc[i + 1]

    # Rule: current row has a date but no stock_presence
    if pd.notna(current_row.get("date")) and (not pd.notna(current_row.get("stock_presence"))):
        stock_val = find_stock_indicator(str(next_row.get("text_line", "")))
        if stock_val:
            df.at[i + 1, "stock_presence_lower"] = stock_val

# --- Save outputs (in 100_data) ---
df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 4 complete → Saved:")
print(f"   📄 {step_output}")
print(f"   🔄 {recent_output} (latest snapshot)")

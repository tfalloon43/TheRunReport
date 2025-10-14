# 3_stock_presence_lower.py
# ------------------------------------------------------------
# Step 3: Adds "stock_presence_lower" column.
# If a row has a date but NO stock_presence,
# look at the *next* row's text_line for standalone stock indicators (H/W/M/U/C).
# Copies that indicator into the *lower* row (the one containing it).
# Saves both step output and csv_recent snapshot.
# ------------------------------------------------------------

import pandas as pd
import re
import os

print("▶ Running Step 3: stock_presence_lower")

# --- Load previous output ---
base_dir = os.path.dirname(__file__)
input_path = os.path.join(base_dir, "csv_recent.csv")
if not os.path.exists(input_path):
    raise FileNotFoundError("csv_recent.csv not found. Run Step 2 first.")

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
        # Look for stock indicator in the NEXT row's text_line
        stock_val = find_stock_indicator(str(next_row.get("text_line", "")))
        if stock_val:
            df.at[i + 1, "stock_presence_lower"] = stock_val

# --- Save outputs ---
step_output = os.path.join(base_dir, "3_stock_presence_lower_output.csv")
recent_output = os.path.join(base_dir, "csv_recent.csv")

df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 3 complete → Saved:")
print(f"   {step_output}")
print(f"   {recent_output} (latest snapshot)")

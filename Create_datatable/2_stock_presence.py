# 2_stock_presence.py
# ------------------------------------------------------------
# Step 2: Adds "stock_presence" column.
# If a row has a date, look for standalone stock indicators (H/W/M/U/C)
# in the text_line and record it in stock_presence.
# Saves both step output and csv_recent snapshot.
# ------------------------------------------------------------

import pandas as pd
import re
import os

print("▶ Running Step 2: stock_presence")

# --- Load previous output ---
base_dir = os.path.dirname(__file__)
input_path = os.path.join(base_dir, "csv_recent.csv")
if not os.path.exists(input_path):
    raise FileNotFoundError("csv_recent.csv not found. Run Step 1 first.")

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
        val = find_stock_indicator(row.get("text_line", ""))
        return val
    return None

df["stock_presence"] = df.apply(get_stock_presence, axis=1)

# --- Save outputs ---
step_output = os.path.join(base_dir, "2_stock_presence_output.csv")
recent_output = os.path.join(base_dir, "csv_recent.csv")

df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 2 complete → Saved:")
print(f"   {step_output}")
print(f"   {recent_output} (latest snapshot)")
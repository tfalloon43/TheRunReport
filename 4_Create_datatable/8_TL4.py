# 8_TL4.py
# ------------------------------------------------------------
# Build TL4 column from TL2 and TL3
# TL4 = TL2 with TL3 (numeric tail) removed
# Removes exactly the final 11 numeric/dash tokens from TL2.
# Keeps prefix text intact (e.g., 'Stock-').
# Input  : 100_data/csv_recent.csv
# Output : 100_data/8_TL4_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import re
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "8_TL4_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 8: Creating TL4...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def make_TL4(tl2, tl3):
    """
    Remove only the last 11 numeric/dash tokens from TL2.
    Keeps everything else (e.g., 'Stock-') intact.
    """
    if not isinstance(tl2, str) or not tl2.strip():
        return ""
    if not isinstance(tl3, str) or not tl3.strip():
        return ""

    # Standardize for token counting
    t2 = tl2.strip().replace(",", "")
    tokens = t2.split()

    # Defensive: if fewer than 11 tokens, don't modify
    if len(tokens) <= 11:
        return tl2.strip()

    # Remove the last 11 tokens (numeric/dash block)
    tl4_tokens = tokens[:-11]

    # Rebuild, preserving punctuation such as trailing hyphen
    result = " ".join(tl4_tokens)

    # Only trim trailing whitespace (DO NOT strip hyphens/commas)
    result = re.sub(r"\s+$", "", result)

    return result

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["TL4"] = df.apply(
    lambda r: make_TL4(r.get("TL2", ""), r.get("TL3", "")),
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
populated = df["TL4"].astype(str).str.strip().ne("").sum()
print(f"✅ TL4 extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with TL4 column")
print(f"📊 {populated} of {total_rows} rows populated with TL4")
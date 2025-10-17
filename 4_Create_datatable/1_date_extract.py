# 1_date_extract.py
# ------------------------------------------------------------
# Step 1: Extracts dates from text_line and saves both
# - 1_date_extract_output.csv
# - csv_recent.csv (latest snapshot)
# ------------------------------------------------------------

import sqlite3
import pandas as pd
import re
from pathlib import Path

# --- Paths ---
# Get project root (one level up from this script)
project_root = Path(__file__).resolve().parents[1]

# Path to the data folder
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

# SQLite database inside 100_data
db_path = data_dir / "pdf_data.sqlite"

# --- Load data from SQLite ---
if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found: {db_path}")

conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM z1_pdf_lines ORDER BY id", conn)
conn.close()

# --- Extract date ---
def extract_date(text):
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    return m.group(1) if m else None

df["date"] = df["text_line"].apply(extract_date)

# --- Save outputs (in 100_data folder) ---
step_output = data_dir / "1_date_extract_output.csv"
recent_output = data_dir / "csv_recent.csv"

df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 1 complete → Saved:")
print(f"   📄 {step_output}")
print(f"   🔄 {recent_output} (latest snapshot)")

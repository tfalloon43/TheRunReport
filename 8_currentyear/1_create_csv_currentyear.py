# 8_currentyear/1_create_csv_currentyear.py
# ------------------------------------------------------------
# Step 1 (Current Year): Export the "z4_plot_data" table from
# pdf_data.sqlite into a CSV file called csv_currentyear.csv
# (saved in 100_Data).
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 1 (Current Year): Creating csv_currentyear.csv from z4_plot_data...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

db_path = data_dir / "pdf_data.sqlite"
output_path = data_dir / "csv_currentyear.csv"

# ------------------------------------------------------------
# Validate paths
# ------------------------------------------------------------
if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found at: {db_path}")

print(f"📂 Database found at: {db_path}")

# ------------------------------------------------------------
# Read data from SQLite
# ------------------------------------------------------------
try:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM 'z4_plot_data';", conn)
except Exception as e:
    raise RuntimeError(f"❌ Error reading table 'z4_plot_data' from database: {e}")

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from z4_plot_data")

# ------------------------------------------------------------
# Save as CSV in 100_Data
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Exported → {output_path}")

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print("✅ Step 1 (Current Year) complete — csv_currentyear.csv created successfully in 100_Data.")
# tester.py
# ------------------------------------------------------------
# Quick test script: export Escapement_PlotPipeline to CSV
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("📤 Exporting Escapement_PlotPipeline → Escapement_PlotPipeline.csv")

# ------------------------------------------------------------
# DB PATH (fixed)
# ------------------------------------------------------------
script_path = Path(__file__).resolve()
project_root = script_path.parents[1]  # TheRunReport/runreport-backend
db_path = project_root / "0_db" / "local.db"
output_path = project_root / "Escapement_PlotPipeline.csv"

print(f"🗄️ Reading DB → {db_path}")

# ------------------------------------------------------------
# LOAD FROM DB
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
conn.close()

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns.")

# ------------------------------------------------------------
# WRITE CSV
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Export complete → {output_path}")
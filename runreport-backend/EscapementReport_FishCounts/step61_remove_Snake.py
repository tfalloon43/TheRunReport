# step61_remove_Snake.py
# ------------------------------------------------------------
# Step 61: Remove Snake River rows
#
# Removes any rows in Escapement_PlotPipeline where the basin
# column contains "Snake River" (case-insensitive).
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

print("🧹 Step 61: Removing Snake River rows from Escapement_PlotPipeline...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# DELETE ROWS VIA SQL
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(Escapement_PlotPipeline);")
columns = {row[1] for row in cursor.fetchall()}
if "basin" not in columns:
    raise ValueError("❌ Missing required column 'basin' in Escapement_PlotPipeline.")

cursor.execute(
    "CREATE INDEX IF NOT EXISTS idx_plotpipeline_basin "
    "ON Escapement_PlotPipeline(basin);"
)

cursor.execute(
    "SELECT COUNT(*) FROM Escapement_PlotPipeline "
    "WHERE lower(basin) LIKE '%snake river%';"
)
removed = cursor.fetchone()[0]

cursor.execute(
    "DELETE FROM Escapement_PlotPipeline "
    "WHERE lower(basin) LIKE '%snake river%';"
)

cursor.execute("SELECT COUNT(*) FROM Escapement_PlotPipeline;")
remaining = cursor.fetchone()[0]

conn.commit()
conn.close()

print(f"🗑️ Rows removed (basin contains 'Snake River'): {removed:,}")
print(f"📊 Remaining rows: {remaining:,}")

print("✅ Step 61 complete — Snake River rows removed.")

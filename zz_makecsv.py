#!/usr/bin/env python3
import csv
import sqlite3
from pathlib import Path

DB_PATH = Path("runreport-backend/0_db/local.db")
OUTPUT_CSV = Path("Escapement_PlotPipeline.csv")

# Add column names to keep, e.g. ["river", "site_name", "timestamp", "flow_cfs"].
# Leave empty to export all columns.
COLUMNS_TO_KEEP = ["pdf_name", "facility", "basin", "species", "Family", "date_iso", "Adult_Total"]

def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        cursor = conn.execute("PRAGMA table_info(Escapement_PlotPipeline)")
        all_columns = [row["name"] for row in cursor.fetchall()]

        columns = COLUMNS_TO_KEEP or all_columns
        missing = [col for col in columns if col not in all_columns]
        if missing:
            raise SystemExit(f"Unknown columns in COLUMNS_TO_KEEP: {missing}")

        select_clause = ", ".join([f'"{col}"' for col in columns])
        rows = conn.execute(f"SELECT {select_clause} FROM Escapement_PlotPipeline").fetchall()

        with OUTPUT_CSV.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", *columns])
            for idx, row in enumerate(rows, start=1):
                writer.writerow([idx, *[row[col] for col in columns]])

    print(f"Wrote {OUTPUT_CSV}")


if __name__ == "__main__":
  main()

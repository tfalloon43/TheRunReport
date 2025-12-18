"""
step0_runner.py
-----------------------------------------
Runs the Columbia_FishCounts ETL pipeline with a gating check:

    • Step 1 always runs (downloads raw data).
    • If downloaded data is unchanged vs the DB, stop.
    • Otherwise continue:
        Step 2: add_species_plot()
        Step 3: add_river_column()
        Step 4: reorganize_daily_data()
        Step 5: add_id_and_convert_numeric()

Writes the cleaned DataFrame to runreport-backend/0_db/local.db.
"""

import hashlib
import sys
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------
# Resolve correct folder paths
# ------------------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parents[1]          # runreport-backend/
COLUMBIA_DIR = BACKEND_ROOT / "Columbia_FishCounts"         # module folder
DB_DIR = BACKEND_ROOT / "0_db"                              # folder containing local.db
DB_PATH = DB_DIR / "local.db"                               # unified DB file

# Add folders to Python path
sys.path.insert(0, str(COLUMBIA_DIR))
sys.path.insert(0, str(DB_DIR))

# ------------------------------------------------------------
# Import pipeline steps
# ------------------------------------------------------------
from step1_datapull import fetch_columbia_daily
from step2_species_plot import add_species_plot
from step3_river import add_river_column
from step4_reorg import reorganize_daily_data
from step5_id import add_id_and_convert_numeric

# SQLite manager (already built earlier)
from sqlite_manager import SQLiteManager


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def df_hash(df: pd.DataFrame) -> str:
    """
    Produce a stable hash for a DataFrame by sorting columns/rows first.
    """
    cols = sorted(df.columns)
    ordered = df[cols].sort_values(by=cols).reset_index(drop=True)
    data = ordered.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


# ------------------------------------------------------------
# MAIN PIPELINE FUNCTION
# ------------------------------------------------------------
def run_columbia_pipeline():
    print("\n🚀 Running Columbia_FishCounts ETL Pipeline...\n")

    # Step 1 — download + raw CSVs
    print("👉 Step 1: Fetching raw FPC data...")
    df_raw = fetch_columbia_daily()
    print(f"   ✔ Retrieved {len(df_raw):,} raw rows")

    # Step 2 — Species_Plot
    print("👉 Step 2: Adding Species_Plot...")
    df = add_species_plot(df_raw)

    # Step 3 — river column
    print("👉 Step 3: Mapping dam_code → river...")
    df = add_river_column(df)

    # Step 4 — reorganize
    print("👉 Step 4: Reorganizing columns...")
    df = reorganize_daily_data(df)

    # Step 5 — add ID, enforce numeric types
    print("👉 Step 5: Adding ID + converting numeric columns...")
    df = add_id_and_convert_numeric(df)

    # Gating: compare final transformed data to existing table
    db = SQLiteManager("local.db")
    try:
        existing = db.fetch_df("SELECT * FROM Columbia_FishCounts")
        old_hash = df_hash(existing)
        print(f"🔑 Existing table hash: {old_hash[:12]}...")
    except Exception:
        old_hash = None
        print("ℹ️ No existing Columbia_FishCounts table found (or unreadable).")

    new_hash = df_hash(df)
    print(f"🔑 New data hash:      {new_hash[:12]}...")

    if old_hash and new_hash == old_hash:
        print("✔ No change detected — skipping write.\n")
        return None

    print("🆕 Change detected — writing updated table.")

    print("\n🎉 Pipeline complete!")
    print(f"   Final row count: {len(df):,}")
    print(f"   Final columns: {list(df.columns)}\n")

    return df


# ------------------------------------------------------------
# WRITE FINAL DF TO LOCAL DB
# ------------------------------------------------------------
def write_to_local_db(df: pd.DataFrame, table_name="Columbia_FishCounts"):
    print(f"🗄️ Writing results → local.db")

    # Pass full DB path to SQLiteManager
    db = SQLiteManager("local.db")
    db.write_df(table_name, df)

    print("✔ Write complete\n")


# ------------------------------------------------------------
# MAIN ENTRY POINT
# ------------------------------------------------------------
if __name__ == "__main__":
    final_df = run_columbia_pipeline()
    if final_df is not None:
        write_to_local_db(final_df)
        print("🏁 ETL job finished successfully.")

"""
step90_export_supabase.py
-----------------------------------------
Export EscapementReport_PlotData from local.db to Supabase.

This replaces the remote table contents (truncate + insert).
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
import sys
import os

import pandas as pd


CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_PATH = BACKEND_ROOT / "0_db" / "local.db"
sys.path.append(str(BACKEND_ROOT))

PLOTDATA_TABLE = "EscapementReport_PlotData"
PIPELINE_TABLE = "Escapement_PlotPipeline"
PIPELINE_COLUMNS = [
    "index",
    "pdf_name",
    "facility",
    "basin",
    "species",
    "Family",
    "Stock",
    "date_iso",
    "Adult_Total",
    "adult_diff_plot",
]


try:
    from publish.supabase_client import SupabaseConfigError, get_supabase_client
    from publish.audit import upsert_publish_audit
except Exception as exc:
    raise ImportError(
        "❌ Could not import Supabase client helper. "
        f"Checked path: {BACKEND_ROOT / 'publish'}\n"
        f"Error: {exc}"
    )


def get_supabase():
    try:
        return get_supabase_client()
    except SupabaseConfigError as exc:
        print(f"❌ Supabase not configured: {exc}")
        return None


def load_plotdata() -> pd.DataFrame:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"❌ local.db not found at {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(f"SELECT * FROM {PLOTDATA_TABLE};", conn)


def load_pipeline_subset() -> pd.DataFrame:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"❌ local.db not found at {DB_PATH}")
    columns_sql = ", ".join(f'"{col}"' for col in PIPELINE_COLUMNS)
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            f"SELECT {columns_sql} FROM {PIPELINE_TABLE};",
            conn,
        )

def get_local_max_pdf_date() -> str | None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"❌ local.db not found at {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """
            SELECT MAX(pdf_date)
            FROM Escapement_PlotPipeline
            WHERE pdf_date IS NOT NULL AND TRIM(pdf_date) <> '';
            """
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def truncate_table(client, df: pd.DataFrame, table_name: str) -> None:
    rpc_name = os.getenv("SUPABASE_TRUNCATE_RPC", "").strip()
    if rpc_name:
        response = client.rpc(rpc_name, {"table_name": table_name}).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase truncate failed for {table_name}: {response.error}")
        return

    if "id" not in df.columns:
        raise RuntimeError(
            f"SUPABASE_TRUNCATE_RPC is required because {table_name} lacks an 'id' column."
        )

    response = client.table(table_name).delete().gte("id", 0).execute()
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase delete failed for {table_name}: {response.error}")


def insert_rows(
    client, table_name: str, rows: list[dict], chunk_size: int = 1000
) -> None:
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        response = client.table(table_name).insert(chunk).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase insert failed for {table_name}: {response.error}")


def main() -> None:
    print("🚚 Step 90: Exporting EscapementReport_PlotData to Supabase...")

    client = get_supabase()
    if client is None:
        return

    df = load_plotdata()
    if df.empty:
        print("⚠️  No rows found in EscapementReport_PlotData. Nothing to export.")
        return

    truncate_table(client, df, PLOTDATA_TABLE)
    df = df.astype(object).where(pd.notnull(df), None)
    rows = df.to_dict(orient="records")
    insert_rows(client, PLOTDATA_TABLE, rows)

    print(f"✅ Export complete — {len(rows):,} rows written to {PLOTDATA_TABLE}.")
    source_max_date = get_local_max_pdf_date()
    run_id = os.getenv("PUBLISH_RUN_ID", "").strip() or None
    upsert_publish_audit(
        client,
        "escapement",
        source_max_date=source_max_date,
        row_count=len(rows),
        run_id=run_id,
    )
    print("🧾 Escapement publish audit updated.")

    pipeline_df = load_pipeline_subset()
    if pipeline_df.empty:
        print("⚠️  No rows found in Escapement_PlotPipeline. Nothing to export.")
        return

    truncate_table(client, pipeline_df, PIPELINE_TABLE)
    pipeline_df = pipeline_df.astype(object).where(pd.notnull(pipeline_df), None)
    pipeline_rows = pipeline_df.to_dict(orient="records")
    insert_rows(client, PIPELINE_TABLE, pipeline_rows)
    print(
        f"✅ Export complete — {len(pipeline_rows):,} rows written to {PIPELINE_TABLE}."
    )


if __name__ == "__main__":
    main()

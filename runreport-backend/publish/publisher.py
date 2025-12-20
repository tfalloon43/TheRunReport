"""Publish pipeline outputs to Supabase.

This is the only place that should write to Supabase.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

from .schemas import DATASET_TABLES, METADATA_TABLES, REGISTRY_TABLES, TABLE_SCHEMAS
from .supabase_client import SupabaseConfigError, get_supabase_client


DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "0_db" / "local.db"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    )
    return cur.fetchone() is not None


def _get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]


def _get_row_count(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.execute(f"SELECT COUNT(1) FROM {table};")
    return int(cur.fetchone()[0])


def _validate_sqlite_table(conn: sqlite3.Connection, table: str, required_columns: Iterable[str]) -> None:
    if not _table_exists(conn, table):
        raise ValueError(f"SQLite table missing: {table}")

    row_count = _get_row_count(conn, table)
    if row_count <= 0:
        raise ValueError(f"SQLite table has no rows: {table}")

    columns = _get_table_columns(conn, table)
    missing = [col for col in required_columns if col not in columns]
    if missing:
        raise ValueError(f"SQLite table {table} missing columns: {missing}")


def _truncate_table(client, table: str, delete_filter: tuple[str, object] | None) -> None:
    rpc_name = os.getenv("SUPABASE_TRUNCATE_RPC", "").strip()
    if rpc_name:
        response = client.rpc(rpc_name, {"table_name": table}).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase truncate failed for {table}: {response.error}")
        return

    if delete_filter is None:
        raise SupabaseConfigError(
            f"No delete_filter configured for {table} and SUPABASE_TRUNCATE_RPC is unset."
        )

    column, value = delete_filter
    response = client.table(table).delete().neq(column, value).execute()
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase delete failed for {table}: {response.error}")


def _insert_rows(client, table: str, rows: list[dict], chunk_size: int = 1000) -> None:
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        response = client.table(table).insert(chunk).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase insert failed for {table}: {response.error}")


def _publish_table(conn: sqlite3.Connection, client, table: str, dry_run: bool) -> int:
    schema = TABLE_SCHEMAS.get(table, {})
    required_columns = schema.get("required_columns", [])
    delete_filter = schema.get("delete_filter")

    _validate_sqlite_table(conn, table, required_columns)

    df = pd.read_sql_query(f"SELECT * FROM {table};", conn)
    if df.empty:
        raise ValueError(f"SQLite table has no rows after load: {table}")

    if dry_run:
        print(f"🧪 Dry-run: would publish {len(df):,} rows to {table}")
        return len(df)

    _truncate_table(client, table, delete_filter)
    if table == "EscapementReports" and "report_year" in df.columns:
        df["report_year"] = pd.to_numeric(df["report_year"], errors="coerce").astype("Int64")
    df = df.astype(object).where(pd.notnull(df), None)
    rows = df.to_dict(orient="records")
    _insert_rows(client, table, rows)
    return len(rows)


def _update_metadata(client, dataset: str, row_counts: dict[str, int], dry_run: bool) -> None:
    if not METADATA_TABLES:
        return

    if dry_run:
        print(f"🧪 Dry-run: would update metadata for {dataset}")
        return

    # Placeholder: implement dataset metadata writes.
    print(f"🧾 Metadata update placeholder for {dataset}: {row_counts}")
    _ = client


def _update_registry(client, dataset: str, dry_run: bool) -> None:
    tables = REGISTRY_TABLES.get(dataset, [])
    if not tables:
        return

    if dry_run:
        print(f"🧪 Dry-run: would update registry tables for {dataset}: {tables}")
        return

    # Placeholder: implement registry writes.
    print(f"📚 Registry update placeholder for {dataset}: {tables}")
    _ = client


def _publish_dataset(conn: sqlite3.Connection, client, dataset: str, dry_run: bool) -> None:
    tables = DATASET_TABLES.get(dataset, [])
    if not tables:
        print(f"⏭️  No tables configured for dataset: {dataset}")
        return

    row_counts: dict[str, int] = {}
    for table in tables:
        row_counts[table] = _publish_table(conn, client, table, dry_run=dry_run)
        print(f"✅ Published {row_counts[table]:,} rows to {table}")

    if dataset == "escapement":
        _update_registry(client, dataset, dry_run=dry_run)

    _update_metadata(client, dataset, row_counts, dry_run=dry_run)


def publish_all(
    flags: dict[str, bool],
    db_path: str | Path | None = None,
    dry_run: bool = False,
) -> None:
    """Publish selected datasets from the local SQLite DB."""
    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH

    if dry_run:
        print("🧪 Publisher dry-run: no data will be written.")

    try:
        client = get_supabase_client()
    except SupabaseConfigError as exc:
        print(f"⏭️  Publisher skipped: {exc}")
        return

    if not db_path.exists():
        print(f"❌ Publisher failed: SQLite DB not found: {db_path}")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            if flags.get("columbia"):
                _publish_dataset(conn, client, "columbia", dry_run=dry_run)
            if flags.get("flows"):
                _publish_dataset(conn, client, "flows", dry_run=dry_run)
            if flags.get("escapement"):
                _publish_dataset(conn, client, "escapement", dry_run=dry_run)
    except Exception as exc:
        print(f"❌ Publisher failed: {exc}")
        return

    print("🏁 Publisher finished.")


if __name__ == "__main__":
    publish_all({"columbia": True, "flows": True, "escapement": True})

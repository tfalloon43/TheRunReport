"""Publish audit helpers for Supabase."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _normalize_timestamp(value: str | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return value


def get_publish_audit(client: Any, dataset_name: str) -> dict | None:
    response = (
        client.table("publish_audit")
        .select("dataset_name,last_published_at,source_max_date,row_count,run_id")
        .eq("dataset_name", dataset_name)
        .execute()
    )
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase audit read failed: {response.error}")
    data = response.data or []
    return data[0] if data else None


def upsert_publish_audit(
    client: Any,
    dataset_name: str,
    source_max_date: str | datetime | None,
    row_count: int,
    run_id: str | None = None,
) -> None:
    payload = {
        "dataset_name": dataset_name,
        "last_published_at": datetime.now(timezone.utc).isoformat(),
        "source_max_date": _normalize_timestamp(source_max_date),
        "row_count": row_count,
        "run_id": run_id,
    }
    response = client.table("publish_audit").upsert(payload, on_conflict="dataset_name").execute()
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase audit upsert failed: {response.error}")

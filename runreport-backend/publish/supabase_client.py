"""Supabase client helper.

Uses env vars:
  - SUPABASE_URL
  - SUPABASE_SERVICE_ROLE_KEY (preferred) or SUPABASE_ANON_KEY
"""

from __future__ import annotations

import os
from typing import Any


class SupabaseConfigError(RuntimeError):
    pass


def _get_credentials() -> tuple[str, str]:
    url = os.getenv("SUPABASE_URL", "").strip()
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    anon_key = os.getenv("SUPABASE_ANON_KEY", "").strip()

    if not url:
        raise SupabaseConfigError("SUPABASE_URL is not set.")

    key = service_key or anon_key
    if not key:
        raise SupabaseConfigError("SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY must be set.")

    return url, key


def get_supabase_client() -> Any:
    """Return a supabase-py client instance.

    This intentionally imports supabase lazily so the rest of the pipeline
    can run even if the dependency is not installed yet.
    """
    url, key = _get_credentials()

    try:
        from supabase import create_client  # type: ignore
    except Exception as exc:  # pragma: no cover - placeholder for now
        raise SupabaseConfigError(
            "supabase-py is not installed. Add it to your environment to publish."
        ) from exc

    return create_client(url, key)

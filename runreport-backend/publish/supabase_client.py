"""Supabase client helper.

Uses env vars:
  - SUPABASE_URL
  - SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import os
from typing import Any


class SupabaseConfigError(RuntimeError):
    pass


def _load_dotenv_if_needed() -> None:
    if os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        return

    try:
        from dotenv import find_dotenv, load_dotenv  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise SupabaseConfigError(
            "python-dotenv is required to load .env locally. Install it or set env vars."
        ) from exc

    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)


def _get_credentials() -> tuple[str, str]:
    url = os.getenv("SUPABASE_URL", "").strip()
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if not url or not service_key:
        _load_dotenv_if_needed()
        url = os.getenv("SUPABASE_URL", "").strip()
        service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if not url:
        raise SupabaseConfigError(
            "SUPABASE_URL is not set. Define it in the environment or .env."
        )
    if not service_key:
        raise SupabaseConfigError(
            "SUPABASE_SERVICE_ROLE_KEY is not set. Define it in the environment or .env."
        )

    return url, service_key


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

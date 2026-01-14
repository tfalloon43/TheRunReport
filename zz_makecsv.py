#!/usr/bin/env python3
import csv
import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path

OUTPUT_CSV = Path("Escapement_PlotPipeline.csv")
TABLE_NAME = "Escapement_PlotPipeline"
PAGE_SIZE = 1000


def build_ssl_context() -> ssl.SSLContext:
    if os.environ.get("SUPABASE_SSL_NO_VERIFY") == "1":
        return ssl._create_unverified_context()
    cafile = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    if cafile:
        return ssl.create_default_context(cafile=cafile)
    try:
        import certifi
    except ImportError:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=certifi.where())


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def fetch_supabase_rows(base_url: str, api_key: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    ssl_context = build_ssl_context()
    order_param = "order=%22index%22.asc"

    while True:
        url = (
            f"{base_url}/rest/v1/{TABLE_NAME}"
            f'?select=*,Stock_BO&limit={PAGE_SIZE}&offset={offset}&{order_param}'
        )
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, context=ssl_context) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(
                f"Supabase request failed: {exc.code} {exc.reason}\n{detail}"
            ) from exc

        data = json.loads(body) if body.strip() else []
        if not data:
            break
        rows.extend(data)
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return rows


def main() -> None:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    load_env_file(os.path.join(repo_root, ".env"))

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get(
        "SUPABASE_ANON_KEY"
    )
    if not supabase_url:
        raise SystemExit("Missing SUPABASE_URL env var.")
    if not supabase_key:
        raise SystemExit("Missing SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY env var.")

    rows = fetch_supabase_rows(supabase_url, supabase_key)
    if not rows:
        raise SystemExit("No rows returned from Supabase.")

    def sort_key(row: dict):
        value = row.get("index")
        try:
            return int(value)
        except (TypeError, ValueError):
            return float("inf")

    rows.sort(key=sort_key)

    headers = list(rows[0].keys())
    if "matching" not in headers:
        headers.append("matching")

    with OUTPUT_CSV.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in rows:
            adult_total = row.get("Adult_Total")
            adult_diff = row.get("adult_diff_plot")
            match = ""
            try:
                if adult_total is not None and adult_diff is not None:
                    if float(adult_total) == float(adult_diff):
                        match = "X"
            except (TypeError, ValueError):
                match = ""
            out_row = [row.get(col, "") for col in headers if col != "matching"]
            out_row.append(match)
            writer.writerow(out_row)

    print(f"Wrote {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

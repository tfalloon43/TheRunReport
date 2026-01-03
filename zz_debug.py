#!/usr/bin/env python3
import csv
import io
import os
import sqlite3
import urllib.error
import urllib.request

EXCLUDE_COLUMNS = {"report_id", "line_order"}


def ensure_output_dir() -> str:
    desktop = os.path.expanduser("~/Desktop")
    output_dir = os.path.join(desktop, "zz_tester")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def quote_ident(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) + chr(34))}"'


def export_table(db_path: str, table_name: str, output_dir: str) -> None:
    output_path = os.path.join(output_dir, f"local_{table_name}.csv")
    with sqlite3.connect(db_path) as conn:
        columns = [
            col for col in get_table_columns(conn, table_name) if col not in EXCLUDE_COLUMNS
        ]
        if not columns:
            raise RuntimeError(f"No columns to export for table: {table_name}")
        select_cols = ", ".join(quote_ident(col) for col in columns)
        cursor = conn.execute(f"SELECT {select_cols} FROM {table_name}")
        headers = [desc[0] for desc in cursor.description]
        with open(output_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            for row in cursor:
                writer.writerow(row)


def export_supabase_table(
    base_url: str, api_key: str, table_name: str, output_dir: str
) -> None:
    output_path = os.path.join(output_dir, f"supabase_{table_name}.csv")
    page_size = 1000
    offset = 0
    wrote_header = False
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/csv",
    }

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        keep_indices = None
        while True:
            url = (
                f"{base_url}/rest/v1/{table_name}"
                f"?select=*&limit={page_size}&offset={offset}"
            )
            request = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(request) as response:
                    body_text = response.read().decode("utf-8")
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Supabase request failed for {table_name}: {exc.code} {exc.reason}\n{detail}"
                ) from exc

            if not body_text.strip():
                break

            csv_reader = csv.reader(io.StringIO(body_text))
            try:
                header_row = next(csv_reader)
            except StopIteration:
                break

            if keep_indices is None:
                keep_indices = [
                    idx
                    for idx, col in enumerate(header_row)
                    if col not in EXCLUDE_COLUMNS
                ]
                if not keep_indices:
                    raise RuntimeError(f"No columns to export for table: {table_name}")

            if not wrote_header:
                writer.writerow([header_row[i] for i in keep_indices])
                wrote_header = True

            row_count = 0
            for row in csv_reader:
                writer.writerow([row[i] for i in keep_indices])
                row_count += 1

            if row_count < page_size:
                break

            offset += page_size


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


def main() -> None:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    load_env_file(os.path.join(repo_root, ".env"))
    db_path = os.path.join(repo_root, "runreport-backend", "0_db", "local.db")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    output_dir = ensure_output_dir()
    export_table(db_path, "EscapementReport_PlotData", output_dir)
    export_table(db_path, "EscapementRawLines", output_dir)

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get(
        "SUPABASE_ANON_KEY"
    )
    if not supabase_url:
        raise RuntimeError("Missing SUPABASE_URL env var for Supabase access.")
    if not supabase_key:
        raise RuntimeError(
            "Missing SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY env var."
        )
    export_supabase_table(
        supabase_url, supabase_key, "EscapementReport_PlotData", output_dir
    )
    export_supabase_table(
        supabase_url, supabase_key, "EscapementRawLines", output_dir
    )


if __name__ == "__main__":
    main()

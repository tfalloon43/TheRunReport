#!/usr/bin/env python3
import csv
import io
import json
import os
import sqlite3
import sys
import ssl
import urllib.error
import urllib.parse
import urllib.request

EXCLUDE_COLUMNS = {"id"}
ORDER_BY = {
    "EscapementReport_PlotData": ["river", "Species_Plot", "MM-DD"],
}


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


def get_order_clause(columns: list[str], table_name: str) -> str:
    desired = ORDER_BY.get(table_name, [])
    order_cols = [col for col in desired if col in columns]
    if not order_cols:
        return ""
    return " ORDER BY " + ", ".join(quote_ident(col) for col in order_cols)


def build_supabase_order_param(table_name: str) -> str:
    desired = ORDER_BY.get(table_name, [])
    if not desired:
        return ""
    parts = []
    for col in desired:
        if col.isidentifier():
            token = f"{col}.asc"
        else:
            token = f'"{col}".asc'
        parts.append(urllib.parse.quote(token, safe="._-"))
    return "&order=" + ",".join(parts)


def export_table(db_path: str, table_name: str, output_dir: str) -> None:
    output_path = os.path.join(output_dir, f"local_{table_name}.csv")
    with sqlite3.connect(db_path) as conn:
        columns = [
            col for col in get_table_columns(conn, table_name) if col not in EXCLUDE_COLUMNS
        ]
        if not columns:
            raise RuntimeError(f"No columns to export for table: {table_name}")
        select_cols = ", ".join(quote_ident(col) for col in columns)
        order_clause = get_order_clause(columns, table_name)
        cursor = conn.execute(f"SELECT {select_cols} FROM {table_name}{order_clause}")
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
    order_param = build_supabase_order_param(table_name)
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/csv",
    }
    ssl_context = build_ssl_context()

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        keep_indices = None
        while True:
            url = (
                f"{base_url}/rest/v1/{table_name}"
                f"?select=*&limit={page_size}&offset={offset}{order_param}"
            )
            request = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(request, context=ssl_context) as response:
                    body_text = response.read().decode("utf-8")
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Supabase request failed for {table_name}: {exc.code} {exc.reason}\n{detail}"
                ) from exc
            except urllib.error.URLError as exc:
                raise RuntimeError(
                    "Supabase request failed during SSL handshake. "
                    "Set SSL_CERT_FILE to a CA bundle path or install certifi."
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


def fetch_local_rows(db_path: str, table_name: str) -> tuple[list[str], list[sqlite3.Row]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        columns = [
            col for col in get_table_columns(conn, table_name) if col not in EXCLUDE_COLUMNS
        ]
        if not columns:
            return [], []
        select_cols = ", ".join(quote_ident(col) for col in columns)
        order_clause = get_order_clause(columns, table_name)
        rows = conn.execute(
            f"SELECT {select_cols} FROM {table_name}{order_clause}"
        ).fetchall()
    return columns, rows


def fetch_supabase_rows(
    base_url: str, api_key: str, table_name: str
) -> tuple[list[str], list[dict]]:
    page_size = 1000
    offset = 0
    rows: list[dict] = []
    order_param = build_supabase_order_param(table_name)
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    ssl_context = build_ssl_context()

    while True:
        url = (
            f"{base_url}/rest/v1/{table_name}"
            f"?select=*&limit={page_size}&offset={offset}{order_param}"
        )
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, context=ssl_context) as response:
                body_text = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Supabase request failed for {table_name}: {exc.code} {exc.reason}\n{detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                "Supabase request failed during SSL handshake. "
                "Set SSL_CERT_FILE to a CA bundle path or install certifi."
            ) from exc

        data = json.loads(body_text) if body_text.strip() else []
        if not data:
            break

        rows.extend(data)
        if len(data) < page_size:
            break

        offset += page_size

    columns = [col for col in (rows[0].keys() if rows else []) if col not in EXCLUDE_COLUMNS]
    return columns, rows


def print_rows(title: str, columns: list[str], rows: list[object]) -> None:
    print(f"\n=== {title} ===")
    if not columns:
        print("(no rows)")
        return
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    for row in rows:
        if isinstance(row, dict):
            writer.writerow([row.get(col, "") for col in columns])
        else:
            writer.writerow([row[col] for col in columns])


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


def build_ssl_context() -> ssl.SSLContext:
    if os.environ.get("SUPABASE_SSL_NO_VERIFY") == "1":
        # Explicit override for local debugging only.
        return ssl._create_unverified_context()

    cafile = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    if cafile:
        return ssl.create_default_context(cafile=cafile)

    try:
        import certifi
    except ImportError:
        return ssl.create_default_context()

    return ssl.create_default_context(cafile=certifi.where())


def main() -> None:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(repo_root, "runreport-backend", "0_db", "local.db")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    output_dir = ensure_output_dir()
    export_table(db_path, "Columbia_FishCounts", output_dir)
    export_table(db_path, "EscapementReport_PlotData", output_dir)
    export_table(db_path, "NOAA_flows", output_dir)
    export_table(db_path, "USGS_flows", output_dir)


if __name__ == "__main__":
    main()

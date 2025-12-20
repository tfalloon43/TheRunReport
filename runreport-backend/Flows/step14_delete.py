# step14_delete.py
# ------------------------------------------------------------
# Step 14 (Flows): Manual deletions of inactive stations
#
# This step lets you manually remove specific Site/Gage pairs from
# the DB-backed Flows table (e.g., stations that are no longer active).
#
# It will:
#   - Remove matching (Site N, Gage #N) pairs from Flows
#   - Re-pack remaining pairs to eliminate gaps (Site 1..N)
#   - Recompute flow_presence (USGS / NOAA / BOTH / blank)
#
# Safety:
#   - Each rule must match exactly one existing pair, otherwise it is skipped.
# ------------------------------------------------------------

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd

print("🧹 Step 14 (Flows): Applying manual deletions for inactive stations…")

TABLE_FLOWS = "Flows"

# ============================================================
# 🔧 MANUAL DELETION RULES (EDIT THIS LIST)
# ============================================================
#
# Provide either:
#   - {"gage": "12345678"}                     # remove wherever uniquely found
#   - {"river": "Cedar River", "gage": "..."}  # scoped to a river
# Optional:
#   - {"river": "...", "site": "...", "gage": "..."} for extra precision
#
MANUAL_DELETIONS: list[dict[str, str]] = [
    # --------------------------------------------------------
    # Added from Step 15 output: "No data returned" for ALL of
    # 7d / 30d / 1y windows (candidate inactive stations)
    # --------------------------------------------------------
    {"river": "Cedar River", "gage": "12118610"},
    {"river": "Columbia River", "gage": "12399510"},
    {"river": "Columbia River", "gage": "12436500"},
    {"river": "Columbia River", "gage": "12438000"},
    {"river": "Columbia River", "gage": "12450700"},
    {"river": "Columbia River", "gage": "12453700"},
    {"river": "Columbia River", "gage": "12462600"},
    {"river": "Columbia River", "gage": "1247351910"},
    {"river": "Columbia River", "gage": "1247351985"},
    {"river": "Columbia River", "gage": "1251420010"},
    {"river": "Columbia River", "gage": "12514450"},
    {"river": "Cowlitz River", "gage": "14238800"},
    {"river": "Cowlitz River", "gage": "14243550"},
    {"river": "Cowlitz River", "gage": "14244100"},
    {"river": "Cowlitz River", "gage": "14244180"},
    {"river": "Deschutes River", "gage": "12078920"},
    {"river": "Deschutes River", "gage": "12078930"},
    {"river": "Deschutes River", "gage": "12079980"},
    {"river": "Minter Creek", "gage": "12073425"},
    {"river": "North Fork Nooksack River", "gage": "12208600"},
    {"river": "Okanogan River", "gage": "12447302"},
    {"river": "Stillaguamish River", "gage": "12167400"},
    {"river": "Whatcom Creek", "gage": "12203542"},

    ## Below was manually added ##

    {"river": "Cedar River", "gage": "12114500"},
    {"river": "Cedar River", "gage": "12115000"},
    {"river": "Cedar River", "gage": "12116500"},
    {"river": "Chambers Creek", "gage": "12091500"},
    {"river": "Chehalis River", "gage": "12020000"},
    {"river": "Chehalis River", "gage": "12021800"},
    {"river": "Chehalis River", "gage": "12028060"},
    {"river": "Columbia River", "gage": "12399500"},
    {"river": "Cowlitz River", "gage": "14226500"},
    {"river": "Cowlitz River", "gage": "14231000"},
    {"river": "Cowlitz River", "gage": "14233500"},
    {"river": "Green River", "gage": "12106700"},
    {"river": "Green River", "gage": "12113000"},
    {"river": "Green River", "gage": "12113310"},
    {"river": "Green River", "gage": "12113344"},
    {"river": "Lewis River", "gage": "14216000"},
    {"river": "Okanogan River", "gage": "12439500"},
    {"river": "Okanogan River", "gage": "12445000"},
    {"river": "Puyallup River", "gage": "12092000"},
    {"river": "Puyallup River", "gage": "12096500"},
    {"river": "Puyallup River", "gage": "12096505"},
    {"river": "Puyallup River", "gage": "12101470"},
    {"river": "Skagit River", "gage": "12178600"},
    {"river": "Skagit River", "gage": "12178900"},
    {"river": "Skagit River", "gage": "12179000"},
    {"river": "Skagit River", "gage": "12180300"},
    {"river": "Skagit River", "gage": "12184800"},
    {"river": "Skagit River", "gage": "12189700"},
    {"river": "Skagit River", "gage": "12199000"},
    {"river": "Skookumchuck River", "gage": "12025700"},
#    {"river": "", "gage": ""},
#    {"river": "", "gage": ""},
#    {"river": "", "gage": ""},
    

    # Example skeletons:
    # {"river": "Some River", "gage": "12123456"},
    # {"gage": "ABCD1"},
]


def normalize_text(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none", "<na>") else s


def is_usgs_gage(gage: str) -> bool:
    """
    USGS gages are numeric (sometimes stored as '12123456.0').
    """
    g = normalize_text(gage)
    if not g:
        return False
    if g.endswith(".0"):
        g = g[:-2]
    return g.isdigit()


def is_noaa_gage(gage: str) -> bool:
    g = normalize_text(gage)
    if not g:
        return False
    return not is_usgs_gage(g)


def recompute_flow_presence(row: pd.Series, gage_cols: list[str]) -> str:
    any_usgs = any(is_usgs_gage(row.get(c)) for c in gage_cols)
    any_noaa = any(is_noaa_gage(row.get(c)) for c in gage_cols)
    if any_usgs and any_noaa:
        return "BOTH"
    if any_usgs:
        return "USGS"
    if any_noaa:
        return "NOAA"
    return ""


def collect_pairs(row: pd.Series, max_sites: int) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for i in range(1, max_sites + 1):
        site_col = f"Site {i}"
        gage_col = f"Gage #{i}"
        if site_col not in row.index or gage_col not in row.index:
            break
        site = normalize_text(row.get(site_col))
        gage = normalize_text(row.get(gage_col))
        if not site and not gage:
            continue
        pairs.append((site, gage))
    return pairs


def write_pairs(row: pd.Series, pairs: list[tuple[str, str]], max_sites: int) -> pd.Series:
    for i in range(1, max_sites + 1):
        site_col = f"Site {i}"
        gage_col = f"Gage #{i}"
        if site_col not in row.index or gage_col not in row.index:
            break
        if i <= len(pairs):
            row[site_col] = pairs[i - 1][0]
            row[gage_col] = pairs[i - 1][1]
        else:
            row[site_col] = ""
            row[gage_col] = ""
    return row


# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    df = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn)

if df.empty:
    print("ℹ️ Flows table is empty; nothing to do.")
    raise SystemExit(0)

if "river" not in df.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: river")

# Determine max Site/Gage index in the table
site_cols = [c for c in df.columns if re.fullmatch(r"Site \d+", str(c))]
gage_cols = [c for c in df.columns if re.fullmatch(r"Gage #\d+", str(c))]
max_sites = 0
for c in site_cols + gage_cols:
    m = re.search(r"(\d+)$", str(c))
    if m:
        max_sites = max(max_sites, int(m.group(1)))

if max_sites == 0:
    print("ℹ️ No Site/Gage columns found in Flows; nothing to delete.")
    raise SystemExit(0)

removed_pairs = 0
skipped_rules = 0

if not MANUAL_DELETIONS:
    print("ℹ️ No manual deletions configured (MANUAL_DELETIONS is empty).")
else:
    for rule in MANUAL_DELETIONS:
        rule_river = normalize_text(rule.get("river"))
        rule_site = normalize_text(rule.get("site"))
        rule_gage = normalize_text(rule.get("gage"))

        if not rule_gage:
            print(f"⚠️ Skipping invalid rule (missing gage): {rule}")
            skipped_rules += 1
            continue

        matches: list[tuple[int, int]] = []  # (row_index, pair_index)

        for row_idx, row in df.iterrows():
            if rule_river and normalize_text(row.get("river")).lower() != rule_river.lower():
                continue

            pairs = collect_pairs(row, max_sites=max_sites)
            for pair_idx, (site, gage) in enumerate(pairs):
                if normalize_text(gage).upper() != rule_gage.upper():
                    continue
                if rule_site and normalize_text(site).lower() != rule_site.lower():
                    continue
                matches.append((row_idx, pair_idx))

        if len(matches) != 1:
            print(f"⚠️ Rule did not match exactly one station (matches={len(matches)}), skipping: {rule}")
            skipped_rules += 1
            continue

        row_idx, pair_idx = matches[0]
        row = df.loc[row_idx]
        pairs = collect_pairs(row, max_sites=max_sites)
        removed = pairs.pop(pair_idx)
        removed_pairs += 1

        updated_row = write_pairs(row.copy(), pairs, max_sites=max_sites)
        df.loc[row_idx] = updated_row
        print(f"🗑️ Removed station from river='{row.get('river')}': site='{removed[0]}' gage='{removed[1]}'")

# Recompute flow_presence
gage_cols_ordered = [f"Gage #{i}" for i in range(1, max_sites + 1) if f"Gage #{i}" in df.columns]
if "flow_presence" in df.columns:
    df["flow_presence"] = df.apply(lambda r: recompute_flow_presence(r, gage_cols_ordered), axis=1)

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)

print("✅ Step 14 complete — manual station deletions applied.")
print(f"🗑️ Stations removed: {removed_pairs:,}")
print(f"⚠️ Rules skipped: {skipped_rules:,}")

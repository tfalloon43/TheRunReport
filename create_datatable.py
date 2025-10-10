# create_datatable_v9.py
# ------------------------------------------------------------
# Cleans and merges raw pdf_lines data into structured format
# Adds: species + family + hatchery + hatch_name + basin + stock_BO + stock + date + date_iso + count_data
# Reference data (species_headers, family_map, hatch_name_map, basin_map, stock_corrections)
# imported from lookup_maps.py
# ------------------------------------------------------------

import sqlite3
import pandas as pd
import re
from datetime import datetime
from lookup_maps import species_headers, family_map, hatch_name_map, basin_map, stock_corrections

# ------------------------------------------------------------
# 1) Load data
# ------------------------------------------------------------
db_path = "pdf_data.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT pdf_name, page_num, text_line FROM pdf_lines ORDER BY id")
rows = cursor.fetchall()
conn.close()

species_set_lc = {s.lower() for s in species_headers}

# ------------------------------------------------------------
# 2) Helper functions
# ------------------------------------------------------------
def is_species_header(text):
    return text.strip().lower() in species_set_lc

def contains_stock_tag(text):
    return bool(re.search(r"-\s*[WHUMwhum]\b", text))

def find_insert_pos(upper_text):
    m = re.search(r"(\d| -)", upper_text)
    return m.start() if m else len(upper_text)

def extract_hatchery(text):
    if not text:
        return None
    text = text.strip()
    m = re.match(r"^([A-Z0-9 ./-]+)(?=\s[A-Z][a-z])", text)
    if m:
        return m.group(1).strip()
    parts = text.split()
    uppers = [p for p in parts if p.isupper()]
    if len(uppers) >= 2:
        return " ".join(uppers[:2])
    elif uppers:
        return uppers[0]
    return None

def lookup_hatch_name(hatch):
    if not hatch:
        return None
    return hatch_name_map.get(hatch.upper(), hatch.title())

def lookup_basin(hatch):
    if not hatch:
        return None
    return basin_map.get(hatch.upper())

# ------------------------------------------------------------
# 3) Text repair helpers
# ------------------------------------------------------------
def move_trailing_stock_letter(s):
    """
    Moves a stray H/W/U/M to immediately after 'Stock- ' if:
    - 'Stock-' or 'River-' is followed by numbers/dashes
    - the line later has ' H Hatchery' or ends with standalone H/W/U/M
    """
    if not any(tag in s for tag in ("Stock-", "River-")):
        return s

    # Identify which tag applies
    tag = "Stock-" if "Stock-" in s else "River-"
    if not re.search(fr"{tag}\s*[\d-]", s):
        return s
    if re.search(fr"{tag}\s*[HWUM]\b", s):
        return s

    # Case A: "H Hatchery" or "H. Hatchery"
    m_hatch = re.search(r'\b([HWUM])\s+\.?[Hh]atchery', s)
    if m_hatch:
        letter = m_hatch.group(1)
        s = s[:m_hatch.start(1)] + s[m_hatch.end(1):]
        s = re.sub(fr'({tag}\s*)', rf'\1{letter} ', s, count=1)
        return re.sub(r'\s{2,}', ' ', s).strip()

    # Case B: trailing standalone H/W/U/M
    tail = re.search(r'\s([HWUM])\s*\.?\s*$', s)
    if tail:
        letter = tail.group(1)
        s = s[:tail.start(1)].rstrip()
        s = re.sub(fr'({tag}\s*)', rf'\1{letter} ', s, count=1)
        return re.sub(r'\s{2,}', ' ', s).strip()

    return s

def has_double_record(line):
    """Detect two hatchery records on one line (by repeated 'HATCHERY' or multiple dates)."""
    return line.upper().count("HATCHERY") > 1 or len(re.findall(r"\d{1,2}/\d{1,2}/\d{2,4}", line)) > 1

def split_double_record(line):
    """Split a line into multiple records if two hatcheries are found."""
    parts = re.split(r'(?=\b[A-Z]{2,}\s+HATCHERY\b)', line)
    return [p.strip() for p in parts if p.strip()]

# ------------------------------------------------------------
# 4) Merge continuation lines
# ------------------------------------------------------------
fixed = [list(r) for r in rows]

for i in range(1, len(fixed)):
    prev = (fixed[i-1][2] or "").strip()
    cur = (fixed[i][2] or "").strip()
    if not cur or is_species_header(cur):
        continue

    # Case 1–2: short continuation lines (<=30 chars)
    if len(cur) <= 30:
        if contains_stock_tag(cur):
            insert_at = find_insert_pos(prev)
            if insert_at > 0:
                fixed[i-1][2] = (
                    prev[:insert_at].rstrip() + " " + cur + " " + prev[insert_at:]
                ).strip()
                fixed[i][2] = ""
            continue
        else:
            fixed[i-1][2] = (prev + " " + cur).strip()
            fixed[i][2] = ""
            continue

    # Case 3: fix Stock- or River- tags missing letter
    if ("Stock-" in cur or "River-" in cur) and re.search(r"(Stock-|River-)\s*[\d-]", cur):
        if not re.search(r"(Stock-|River-)\s*[HWUM]\b", cur):
            fixed[i][2] = move_trailing_stock_letter(cur)
        continue

    # Case 4: Next line begins with 'Stock- H...' or 'River- H...'
    if re.match(r'^\s*(Stock-|River-)\s*[HWUM]\b', cur):
        combined = (prev + " " + cur).strip()
        combined = move_trailing_stock_letter(combined)
        fixed[i-1][2] = combined
        fixed[i][2] = ""
        continue

# Global fix + standalone letter check
for j in range(len(fixed)):
    txt = (fixed[j][2] or "").strip()
    if not txt:
        continue
    # Standalone letter at end, e.g. “... 11/19/19 M”
    if re.search(r'\s([HWUM])\s*\.?\s*$', txt):
        txt = move_trailing_stock_letter(txt)
    if has_double_record(txt):
        parts = split_double_record(txt)
        fixed[j][2] = parts[0]
        for extra in parts[1:]:
            fixed.insert(j + 1, [fixed[j][0], fixed[j][1], extra])
    fixed[j][2] = txt

merged_df = pd.DataFrame(fixed, columns=["pdf_name", "page_num", "text_line"])
merged_df.to_csv("pdf_lines_fixed.csv", index=False)
print("✅ Saved merged lines → pdf_lines_fixed.csv")

# ------------------------------------------------------------
# 5) Add metadata columns
# ------------------------------------------------------------
labeled_rows = []
current_species = None

for pdf_name, page_num, text_line in merged_df.itertuples(index=False):
    line = (text_line or "").strip()
    if not line:
        labeled_rows.append([pdf_name, page_num, text_line, None, None])
        continue
    if is_species_header(line):
        current_species = line
        labeled_rows.append([pdf_name, page_num, line, current_species, family_map.get(line.lower())])
        continue
    species_val = current_species
    family_val = family_map.get(species_val.lower()) if species_val else None
    labeled_rows.append([pdf_name, page_num, text_line, species_val, family_val])

labeled_df = pd.DataFrame(
    labeled_rows, columns=["pdf_name", "page_num", "text_line", "species", "family"]
)

# ------------------------------------------------------------
# 6) Hatchery + basin + stock info
# ------------------------------------------------------------
labeled_df["Hatchery"] = labeled_df["text_line"].apply(extract_hatchery)
labeled_df["hatch_name"] = labeled_df["Hatchery"].apply(lookup_hatch_name)
labeled_df["basin"] = labeled_df["Hatchery"].apply(lookup_basin)

def extract_stock_bo(text, hatchery):
    if not text or not hatchery:
        return None
    remainder = text[len(hatchery):].strip()
    m = re.search(r"([A-Za-z0-9().\s]+?-\s*[HWMU])\b", remainder)
    return m.group(1).strip() if m else None

labeled_df["stock_BO"] = labeled_df.apply(
    lambda r: extract_stock_bo(r["text_line"], r["Hatchery"]), axis=1
)
labeled_df["stock_BO"] = labeled_df["stock_BO"].apply(
    lambda v: stock_corrections.get(v.strip(), v.strip()) if v else v
)

def extract_stock(stock_bo):
    if not stock_bo:
        return None
    m = re.search(r"([A-Z])\s*$", stock_bo.strip())
    return m.group(1) if m else None

labeled_df["stock"] = labeled_df["stock_BO"].apply(extract_stock)

# ------------------------------------------------------------
# 7) Date + date_iso + count_data
# ------------------------------------------------------------
def extract_date(text):
    if not text:
        return None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    return m.group(1) if m else None

labeled_df["date"] = labeled_df["text_line"].apply(extract_date)

def convert_to_iso(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%m/%d/%y")
    except ValueError:
        try:
            dt = datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            return None
    return dt.strftime("%Y-%m-%d")

labeled_df["date_iso"] = labeled_df["date"].apply(convert_to_iso)

def extract_count_data(text):
    if not text:
        return None
    # Between "- H/W/U/M" and date
    m = re.search(r"-\s*[HWUM]\s+(.*?)\s+\d{1,2}/\d{1,2}/\d{2,4}", text)
    return m.group(1).strip() if m else None

labeled_df["count_data"] = labeled_df["text_line"].apply(extract_count_data)

# ------------------------------------------------------------
# 8) Save output
# ------------------------------------------------------------
labeled_df.to_csv("pdf_lines_labeled_full_v9.csv", index=False)
print("✅ Finished create_datatable_v9 → pdf_lines_labeled_full_v9.csv")